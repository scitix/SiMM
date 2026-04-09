#include <unistd.h>
#include <chrono>
#include <concepts>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_set>

#include <gflags/gflags.h>

#include "rpc/rpc.h"
#include "transport/ibv_manager.h"

#include "proto/cm_clnt_rpcs.pb.h"
#include "proto/ds_clnt_rpcs.pb.h"

#include "clnt_messenger.h"
#include "cluster_manager/cm_rpc_handler.h"
#include "common/base/assert.h"
#include "common/context/context.h"
#include "common/errcode/errcode_def.h"
#include "common/hashkit/hashkit.h"
#include "common/logging/logging.h"
#include "common/trace/trace.h"
#include "common/utils/k8s_util.h"
#include "data_server/kv_rpc_handler.h"

DECLARE_string(cm_namespace);
DECLARE_string(cm_svc_name);
DECLARE_string(cm_port_name);
DECLARE_string(cm_primary_node_ip);
DECLARE_int32(cm_rpc_inter_port);
DECLARE_int32(clnt_sync_req_timeout_ms);
DECLARE_int32(clnt_async_req_timeout_ms);
DECLARE_uint32(shard_total_num);
DECLARE_uint32(clnt_thread_pool_size);
DECLARE_uint32(clnt_cm_addr_check_interval_inSecs);
DECLARE_uint32(clnt_syncreq_retry_count);
DECLARE_bool(clnt_syncreq_enable_retry);
DECLARE_bool(clnt_use_k8s);
DECLARE_bool(simm_enable_trace);
DECLARE_uint32(clnt_deferred_reshard_wait_inSecs);

DECLARE_LOG_MODULE("simm_client");

template <typename RequestPB>
concept HasPBKey = requires(RequestPB pb) {
  {pb.key()};
};

namespace simm {
namespace clnt {

ClientMessenger::ClientMessenger()
    : shard_num_(FLAGS_shard_total_num),
      thread_pool_size_(FLAGS_clnt_thread_pool_size),
      executor_{FLAGS_clnt_thread_pool_size, std::make_shared<folly::NamedThreadFactory>("MultiSendThreadpool")} {
  shard_table_.reserve(shard_num_);
  hashkit_ = &simm::hashkit::HashkitBase::Instance();
  // create rpc client and rdma mempool
  sicl::rpc::SiRPC::newInstance(rpc_client_, false /*is_server*/);
  // set client request timeout
  sync_req_timeout_ms_ = convert_timeout_setting_to_timer_tick(FLAGS_clnt_sync_req_timeout_ms);
  async_req_timeout_ms_ = convert_timeout_setting_to_timer_tick(FLAGS_clnt_async_req_timeout_ms);
  MLOG_INFO("ClientMessenger init, sync req timeout tick type : {}, async req timeout tick type : {}",
            std::to_string(sync_req_timeout_ms_),
            std::to_string(async_req_timeout_ms_));
}

ClientMessenger &ClientMessenger::Instance() {
  static ClientMessenger messenger;
  return messenger;
}

ClientMessenger::~ClientMessenger() {
  failover_flag_.store(false);
  {
    std::lock_guard guard(failover_mutex_);
    failover_condv_.notify_one();
  }
  if (failover_thread_ != nullptr) {
    failover_thread_->join();
    failover_thread_.reset();
  }

  if (rpc_client_)
    delete rpc_client_;
}

error_code_t ClientMessenger::ReInit() {
  initialized_ = false;

  cm_addr_ = get_cm_address();
  if (cm_addr_ == "") {
    MLOG_ERROR("Get cluster manager address failed, may not started yet");
    return CommonErr::InvalidState;
  }

  auto [ret, routing] = update_all_route_table(cm_addr_);
  if (ret != CommonErr::OK) {
    MLOG_ERROR("Update routing table from cluster manager failed : {}", ret);
    cm_addr_ = "";
    return CommonErr::InvalidState;
  }
  ret = ApplyRouteTableDiff(*routing);
  if (ret != CommonErr::OK) {
    MLOG_ERROR("Apply routing table diff failed : {}", ret);
    cm_addr_ = "";
    return CommonErr::InvalidState;
  }

#ifdef SIMM_ENABLE_TRACE
  trace_server_ = std::make_unique<simm::trace::TraceServer>("/run/simm/simm_trace");

  simm::trace::TraceManager::Instance().SetEnabled(FLAGS_simm_enable_trace);
#endif

  initialized_ = true;
  return CommonErr::OK;
}

error_code_t ClientMessenger::Init() {
  if (initialized_) {
    return CommonErr::OK;
  }

  if (failover_thread_ == nullptr) {
    failover_thread_ = std::make_unique<std::thread>([this]() {
      while (failover_flag_.load()) {
        {
          std::unique_lock lock(failover_mutex_);
          // NOTE: We might miss messages if the conditon variable is notified while we rebuild connections.
          // So check on regular intervals to workaround that.
          failover_condv_.wait_for(lock, std::chrono::seconds(FLAGS_clnt_cm_addr_check_interval_inSecs));
        }
        if (!failover_flag_.load()) {
          // avoid one meaningless cm address query below, for curl_easy_perform() in get_cm_address()
          // will coredump(SIGSEGV) with nullptr pthead rwlock in OpenSSL lib
          MLOG_INFO("Failover thread will exit");
          break;
        }

        bool should_reinit = false;
        if (get_cm_address() != cm_addr_) {
          // cluster manager address changed, maybe it was restarted, so client should
          // sync with it and get latest data servers address info
          should_reinit = true;
        } else {
          for (auto [addr, ds_ctx] : ds_conn_ctxs_) {
            if (!ds_ctx->active.load()) {
              // track when DS was first seen as dead
              {
                std::lock_guard lg(ds_dead_since_mtx_);
                if (!ds_dead_since_.count(addr)) {
                  ds_dead_since_[addr] = std::chrono::steady_clock::now();
                }
              }

              // Try to reconnect — new DS may have come up with same port
              if (CommonErr::OK == build_connection(addr)) {
                std::lock_guard lg(ds_dead_since_mtx_);
                ds_dead_since_.erase(addr);
                continue;
              }

              // Reconnect failed — check if deferred reshard wait window exceeded.
              // NOTE: CM updates its routing table immediately upon DS handshake (IP update or
              // replacement), but the client has no way to learn about it promptly because
              // CM-to-client routing push (RPC_ROUTING_TABLE_UPDATE) is not yet implemented
              // (see cm_service.cc TODO). Until push is available, the client can only discover
              // the new IP by polling CM via ReInit() after this wait window expires.
              // If the DS restarts with a different IP, IO to the affected shards will fail for
              // up to clnt_deferred_reshard_wait_inSecs seconds. This is a known limitation;
              // implementing CM→client push will eliminate the gap.
              std::chrono::duration<double> dur;
              {
                std::lock_guard lg(ds_dead_since_mtx_);
                dur = std::chrono::steady_clock::now() - ds_dead_since_[addr];
              }

              if (dur > std::chrono::seconds(FLAGS_clnt_deferred_reshard_wait_inSecs)) {
                // Window expired: CM may have done reshard or IP update, pull new routes
                {
                  std::lock_guard lg(ds_dead_since_mtx_);
                  ds_dead_since_.erase(addr);
                }
                should_reinit = true;
                MLOG_ERROR("Failover thread: DS {} unreachable for {}s (> {}s window), triggering reinit",
                           addr, static_cast<int>(dur.count()), FLAGS_clnt_deferred_reshard_wait_inSecs);
              }
              // else: still within wait window, keep retrying next cycle
            }
          }
        }
        if (should_reinit) {
          ReInit();
        }
      }
    });
  }

  return ReInit();
}

std::string ClientMessenger::get_cm_address() {
#if defined(SIMM_UNIT_TEST)
  if (test_get_cm_address_hook_) {
    return test_get_cm_address_hook_();
  }
#endif
  const std::string default_cm_addr = FLAGS_cm_primary_node_ip + ":" + std::to_string(FLAGS_cm_rpc_inter_port);
  if (!FLAGS_clnt_use_k8s) {
    return default_cm_addr;
  }
  // get cluster manager pod info from K8S api, get vector of [pod_name, pod_ip]
  // TODO: change to get cluster manager info from etcd
  auto [ret, cm_ips] =
      simm::utils::GetPodIpsOfHeadlessService(FLAGS_cm_namespace, FLAGS_cm_svc_name, FLAGS_cm_port_name);
  if (ret != 0) {
    MLOG_ERROR("Get cluster manager service info from k8s failed");
    return default_cm_addr;
  }
  // online cluster manager pod can only be 1
  if (cm_ips.empty() || cm_ips.size() > 1) {
    MLOG_ERROR("Invalid cluster manager svc pods: {}", cm_ips.size());
    return default_cm_addr;
  }

  return cm_ips[0].second;
}

error_code_t ClientMessenger::build_connection(const std::string &addr, BuildConnWaitMode wait_mode) {
  auto ds_ctx = GetOrCreateConnectionContext(addr);
  if (ds_ctx->active.load() && ds_ctx->LoadConnection() != nullptr) {
    return CommonErr::OK;
  }

  bool expected = false;
  if (!ds_ctx->connecting_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
    if (wait_mode == BuildConnWaitMode::kNoWait) {
      return ClntErr::BuildConnectionFailed;
    }
    std::unique_lock lock(ds_ctx->connect_wait_mutex_);
    ds_ctx->connect_cv_.wait(lock, [&]() { return !ds_ctx->connecting_.load(std::memory_order_acquire); });
    if (ds_ctx->active.load() && ds_ctx->LoadConnection() != nullptr) {
      return CommonErr::OK;
    }
    return ClntErr::BuildConnectionFailed;
  }

  error_code_t ret = CommonErr::OK;
  std::shared_ptr<sicl::rpc::Connection> connection;
  auto node_addr = simm::common::NodeAddress::ParseFromString(addr);
  if (!node_addr) {
    MLOG_ERROR("Invalid server address format: {}", addr);
    ret = CommonErr::InvalidArgument;
  } else {
#if defined(SIMM_UNIT_TEST)
    if (test_build_connection_hook_) {
      ret = test_build_connection_hook_(addr);
    } else
#endif
    {
      connection = rpc_client_->connect(node_addr->node_ip_, node_addr->node_port_);
      if (connection == nullptr) {
        MLOG_ERROR("Build connection with {} failed", addr);
        ret = ClntErr::BuildConnectionFailed;
      }
    }
  }

  if (ret == CommonErr::OK) {
#if !defined(SIMM_UNIT_TEST)
    ds_ctx->StoreConnection(connection);
    ds_ctx->gen_num.fetch_add(1);
    ds_ctx->active.store(true);
#else
    if (test_build_connection_hook_ == nullptr) {
      ds_ctx->StoreConnection(connection);
      ds_ctx->gen_num.fetch_add(1);
      ds_ctx->active.store(true);
    } else if (!(ds_ctx->active.load() && ds_ctx->LoadConnection() != nullptr)) {
      ret = ClntErr::BuildConnectionFailed;
    }
#endif
  }
  ds_ctx->connecting_.store(false, std::memory_order_release);
  ds_ctx->connect_cv_.notify_all();
  return ret;
}

void ClientMessenger::ReleaseConnectionContext(const std::shared_ptr<ConnectionContext> &ds_ctx) {
  if (ds_ctx == nullptr) {
    return;
  }
  ds_ctx->active.store(false);
  ds_ctx->gen_num.fetch_add(1);
  ds_ctx->StoreConnection(nullptr);
}

std::shared_ptr<ClientMessenger::ConnectionContext> ClientMessenger::GetOrCreateConnectionContext(
    const std::string &addr) {
  auto ds_ctx = std::make_shared<ConnectionContext>(addr);
  if (auto [existing, inserted] = ds_conn_ctxs_.emplace(addr, ds_ctx); !inserted) {
    ds_ctx = existing->second;
  }
  return ds_ctx;
}

void ClientMessenger::PruneStaleConnectionContexts(const std::unordered_set<std::string> &live_servers) {
  std::vector<std::string> stale_servers;
  for (const auto &[addr, ds_ctx] : ds_conn_ctxs_) {
    if (!live_servers.contains(addr)) {
      stale_servers.push_back(addr);
    }
  }

  if (stale_servers.empty()) {
    return;
  }

  std::vector<uint16_t> stale_shards;
  for (const auto &[shard_id, ds_ctx] : shard_table_) {
    if (ds_ctx != nullptr && !live_servers.contains(ds_ctx->ip_port)) {
      stale_shards.push_back(shard_id);
    }
  }
  for (auto shard_id : stale_shards) {
    shard_table_.erase(shard_id);
  }

  for (const auto &addr : stale_servers) {
    auto it = ds_conn_ctxs_.find(addr);
    if (it != ds_conn_ctxs_.end()) {
      ReleaseConnectionContext(it->second);
      ds_conn_ctxs_.erase(addr);
    }
  }
}

template <typename RequestType, typename ResponseType>
error_code_t ClientMessenger::call_sync(uint16_t shard_id,
                                        const sicl::rpc::ReqType req_type,
                                        const RequestType &req,
                                        std::shared_ptr<ResponseType> resp,
                                        std::shared_ptr<simm::common::SimmContext> ctx) {
  if (shard_table_.find(shard_id) == shard_table_.end()) {
    MLOG_ERROR("Shard id {} not exists in client shard table (sync call)", shard_id);
    return ClntErr::ClntLookupShardFailed;
  }

  auto rpc_ctx = ctx->get_rpc_ctx();
  auto retry_delay = std::chrono::milliseconds(100);
  for (auto i = 0; i <= FLAGS_clnt_syncreq_retry_count; ++i) {
    auto ds_ctx = shard_table_[shard_id];
    if (ds_ctx->active.load()) {
      auto tag = ds_ctx->gen_num.load();
      auto connection = ds_ctx->LoadConnection();
      if (connection == nullptr) {
        MLOG_ERROR("Transport connection is null for shard id {}, data server is {}", shard_id, ds_ctx->ip_port);
      } else {
#ifdef SIMM_APIPERF
        auto t1 = std::chrono::steady_clock::now();
#endif
        SIMM_TRACE_POINT(*ctx, simm::trace::TracePointType::CLIENT_CALLSYNC_BEFORE_RPC);

        rpc_client_->SendRequest(connection, req_type, req, resp.get(), rpc_ctx);

        SIMM_TRACE_POINT(*ctx, simm::trace::TracePointType::CLIENT_CALLSYNC_AFTER_RPC);
#ifdef SIMM_APIPERF
        auto t2 = std::chrono::steady_clock::now();
        if constexpr (HasPBKey<RequestType>) {
          MLOG_INFO("Perf-callsync-sendreq key:{} Lat:{} us",
                    req.key(),
                    std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count());
        }
#endif
        if (!rpc_ctx->Failed()) {
          MLOG_DEBUG("call_sync rpc succeed");
          return CommonErr::OK;
        }
      }
      // trigger background failover thread to do global routing table re-fetch and
      // QP connections re-build by specified errors
      ReconnectByErrors(rpc_ctx, ds_ctx, shard_id, tag);
    } else {
      MLOG_WARN("Transport connection is inactive for shard id {}, data server is {}", shard_id, ds_ctx->ip_port);
    }

    if (!FLAGS_clnt_syncreq_enable_retry) {
      MLOG_WARN("Sync requests retry mechanism ({} retries) disabled", FLAGS_clnt_syncreq_retry_count);
      break;
    } else if (i < FLAGS_clnt_syncreq_retry_count) {
      std::this_thread::sleep_for(retry_delay);
      retry_delay *= 2;
    }
  }

  MLOG_ERROR("Failed to send request after {} retries (sync call)",
             FLAGS_clnt_syncreq_enable_retry ? FLAGS_clnt_syncreq_retry_count : 0);
  return ClntErr::ClntSendRPCFailed;
}

template <typename RequestType, typename ResponseType>
error_code_t ClientMessenger::call_async(uint16_t shard_id,
                                         const sicl::rpc::ReqType req_type,
                                         const RequestType &req,
                                         std::shared_ptr<ResponseType> resp,
                                         std::shared_ptr<simm::common::SimmContext> ctx,
                                         Callback callback) {
  if (shard_table_.find(shard_id) == shard_table_.end()) {
    MLOG_ERROR("Shard id {} not exists in client shard table (async call)", shard_id);
    return ClntErr::ClntLookupShardFailed;
  }

  return execute<RequestType, ResponseType>(
      shard_table_.at(shard_id)->ip_port, req_type, req, std::move(resp), std::move(ctx), std::move(callback));
}

template <typename RequestType, typename ResponseType>
error_code_t ClientMessenger::execute(const std::string &addr,
                                      const sicl::rpc::ReqType req_type,
                                      const RequestType &req,
                                      std::shared_ptr<ResponseType> resp,
                                      std::shared_ptr<simm::common::SimmContext> ctx,
                                      Callback callback) {
  auto ds_ctx = GetOrCreateConnectionContext(addr);
  if (!ds_ctx->active.load()) {
    // If one data server is new added and can't be connected, background failover thread will try
    // reconnect action by reinit
    auto ret = build_connection(addr, BuildConnWaitMode::kNoWait);
    if (ret != CommonErr::OK) {
      MLOG_ERROR("Connect with {} failed :{}", addr, ret);
      return ret;
    }
    MLOG_DEBUG("Connect with {} succeed", addr);
  }
  auto connection = ds_ctx->LoadConnection();
  if (connection == nullptr) {
    MLOG_ERROR("Connection with {} is null", addr);
    return ClntErr::BuildConnectionFailed;
  }

  auto rpc_ctx = ctx->get_rpc_ctx();

  if (callback) {
#ifdef SIMM_APIPERF
    auto t1 = std::chrono::steady_clock::now();
#endif
    rpc_client_->SendRequest(connection, req_type, req, resp.get(), rpc_ctx, std::move(callback));
#ifdef SIMM_APIPERF
    auto t2 = std::chrono::steady_clock::now();
    if constexpr (HasPBKey<RequestType>) {
      MLOG_INFO("Perf-exec-sendreq key:{} Lat:{} us",
                req.key(),
                std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count());
      ;
    }
#endif
    MLOG_DEBUG("Call async rpc succeed");
  } else {
    MLOG_DEBUG("Call sync rpc to {}", addr);
    rpc_client_->SendRequest(connection, req_type, req, resp.get(), rpc_ctx);
    if (rpc_ctx->Failed()) {
      MLOG_ERROR("Call sync rpc failed, error: {}({})", rpc_ctx->ErrorCode(), rpc_ctx->ErrorText());
      return ClntErr::ClntSendRPCFailed;
    }
    MLOG_DEBUG("Call sync rpc succeed");
  }
  return CommonErr::OK;
}

std::pair<error_code_t, std::shared_ptr<QueryShardRoutingTableAllResponsePB>> ClientMessenger::update_all_route_table(
    const std::string &ip_port) {
  auto addr = simm::common::NodeAddress::ParseFromString(ip_port);
  if (!addr) {
    MLOG_ERROR("Invalid ip_port string when update all route table: {}", ip_port);
    return {ClntErr::GetRoutingTableFailed, nullptr};
  }

#if defined(SIMM_UNIT_TEST)
  if (test_route_query_hook_) {
    auto [ret, resp] = test_route_query_hook_(ip_port);
    if (ret != CommonErr::OK || resp == nullptr) {
      return {ret, nullptr};
    }
    if (resp->ret_code() != 0) {
      return {ClntErr::GetRoutingTableFailed, nullptr};
    }
    return {CommonErr::OK, resp};
  }
#endif

  // Query shard routing table from Cluster Manager
  QueryShardRoutingTableAllRequestPB req;

  // NOTE: The cluster maanager may return an empty list for unknown reasons.
  // Retry if that happens.
  auto query_retry_delay = std::chrono::milliseconds(100);
  auto query_retry_count = 3;
  for (auto i = 0; i <= query_retry_count; ++i) {
    auto connection = rpc_client_->connect(addr->node_ip_, addr->node_port_);
    if (connection == nullptr) {
      MLOG_ERROR("Build connection with cluster manager {} failed", ip_port);
      return {ClntErr::BuildConnectionFailed, nullptr};
    }

    auto resp = std::make_shared<QueryShardRoutingTableAllResponsePB>();
    auto ctx = std::make_shared<simm::common::SimmContext>();
    sicl::rpc::RpcContext *ctx_p;
    sicl::rpc::RpcContext::newInstance(ctx_p);
    auto rpc_ctx = std::shared_ptr<sicl::rpc::RpcContext>(ctx_p);
    ctx->set_rpc_ctx(rpc_ctx);
    rpc_ctx->set_timeout(sync_req_timeout_ms_);
    rpc_client_->SendRequest(connection,
                             static_cast<sicl::rpc::ReqType>(cm::ClusterManagerRpcType::RPC_ROUTING_TABLE_QUERY_ALL),
                             req,
                             resp.get(),
                             rpc_ctx);
    if (rpc_ctx->Failed()) {
      MLOG_ERROR("RPC to cluster manager({}) failed: {}({})", ip_port, rpc_ctx->ErrorCode(), rpc_ctx->ErrorText());
      return {ClntErr::ClntSendRPCFailed, nullptr};
    } else if (resp->ret_code() != 0) {
      MLOG_ERROR("Cluster manager({}) respond with error: {}", ip_port, resp->ret_code());
      return {ClntErr::GetRoutingTableFailed, nullptr};
    }
    if (resp->shard_info().size() != 0) {
      MLOG_INFO("QueryShardRoutingTableAll from Cluster manager({}) succeed, total shards num: {}",
                ip_port,
                resp->shard_info().size());
      return {CommonErr::OK, resp};
    }
    if (i == query_retry_count) {
      MLOG_ERROR(
          "QueryShardRoutingTableAll from Cluster manager({}) failed after {} retries", ip_port, query_retry_count);
      return {ClntErr::GetRoutingTableTimeout, nullptr};
    } else {
      std::this_thread::sleep_for(query_retry_delay);
      query_retry_delay *= 2;
    }
  }
  return {ClntErr::GetRoutingTableTimeout, nullptr};
}

error_code_t ClientMessenger::ApplyRouteTableDiff(const QueryShardRoutingTableAllResponsePB &routing) {
  std::unordered_set<std::string> live_servers;
  std::vector<std::string> servers_to_connect;

  shard_table_.clear();
  for (const auto &entry : routing.shard_info()) {
    std::string ip = entry.data_server_address().ip();
    uint16_t port = static_cast<uint16_t>(entry.data_server_address().port());
    const auto address = ip + ":" + std::to_string(port);
    auto ds_ctx = GetOrCreateConnectionContext(address);
    if (live_servers.insert(address).second && (!ds_ctx->active.load() || ds_ctx->LoadConnection() == nullptr)) {
      servers_to_connect.push_back(address);
    }
    for (auto shard_id : entry.shard_ids()) {
      shard_table_.insert_or_assign(shard_id, ds_ctx);
    }
  }
  PruneStaleConnectionContexts(live_servers);

  for (const auto &addr : servers_to_connect) {
    auto ret = build_connection(addr);
    if (ret != CommonErr::OK) {
      MLOG_ERROR("Build connection with {} failed during route diff apply, error: {}", addr, ret);
    }
  }

  return CommonErr::OK;
}

void ClientMessenger::ReconnectByErrors(std::shared_ptr<sicl::rpc::RpcContext> rpc_ctx,
                                        std::shared_ptr<ConnectionContext> ds_ctx,
                                        uint16_t shard_id,
                                        size_t old_conn_gen_num) {
  SIMM_ASSERT(rpc_ctx != nullptr, "rpc conecxt is nullptr");
  SIMM_ASSERT(ds_ctx != nullptr, "dataserver connection context is nullptr");
  SIMM_ASSERT(rpc_ctx->Failed(), "rpc request not failed, no need to reconnect");

  if (rpc_ctx->ErrorCode() == sicl::transport::SICL_ERR_INVALID_STATE ||
      rpc_ctx->ErrorCode() == sicl::transport::SICL_ERR_VERBS_WC_ERROR ||
      rpc_ctx->ErrorCode() == sicl::transport::SICL_ERR_VERBS_POST_SEND) {
    MLOG_WARN("Encountered transport error {} for shard id {}, will try to reconnect (sync call)",
              rpc_ctx->ErrorCode(),
              shard_id);
    if (ds_ctx->gen_num.load() == old_conn_gen_num) {
      bool flag = true;
      if (ds_ctx->active.compare_exchange_strong(flag, false)) {
        std::lock_guard guard(failover_mutex_);
        failover_condv_.notify_one();
      }
    } else {
      MLOG_WARN("Connection already reestablished for shard id {}, curr gen:{}, old gen:{}",
                shard_id,
                ds_ctx->gen_num.load(),
                old_conn_gen_num);
    }
  }
}

void ClientMessenger::HandleAsyncRequestFailure(std::shared_ptr<sicl::rpc::RpcContext> rpc_ctx,
                                                std::shared_ptr<ConnectionContext> request_ds_ctx,
                                                uint16_t shard_id,
                                                size_t old_conn_gen_num) {
  ReconnectByErrors(std::move(rpc_ctx), std::move(request_ds_ctx), shard_id, old_conn_gen_num);
}

error_code_t ClientMessenger::Put(const std::string &key,
                                  std::shared_ptr<simm::common::MemBlock> memp,
                                  std::shared_ptr<simm::common::SimmContext> ctx) {
  uint16_t shard_id = hashkit_->generate_16bit_hash_value(key.c_str(), key.length()) % shard_num_;

  // memblock must have descr ptr
  sicl::transport::MemDesc *mem_desc;
  if (memp->descr == nullptr) {
    MLOG_ERROR("Put memblock has no descr ptr")
    return CommonErr::InvalidArgument;
  } else {
    mem_desc = static_cast<sicl::transport::MemDesc *>(memp->descr);
  }

  // build KVPut rpc
  sicl::rpc::RpcContext *ctx_p;
  sicl::rpc::RpcContext::newInstance(ctx_p);
  std::shared_ptr<sicl::rpc::RpcContext> rpc_ctx = std::shared_ptr<sicl::rpc::RpcContext>(ctx_p);
  ctx->set_rpc_ctx(rpc_ctx);
  rpc_ctx->set_timeout(sync_req_timeout_ms_);
  KVPutRequestPB req;
  req.set_shard_id(static_cast<uint32_t>(shard_id));
  req.set_key(key.c_str());
  req.set_val_len(memp->len);
  req.set_buf_addr(reinterpret_cast<google::protobuf::uint64>(memp->buf));
  req.set_buf_ofs(0);
  req.set_buf_len(static_cast<google::protobuf::uint64>(mem_desc->getSize()));
  for (const auto rkey : mem_desc->getRemoteKeys()) {
    req.add_buf_rkey(static_cast<google::protobuf::uint32>(rkey));
  }
  auto resp = std::make_shared<KVPutResponsePB>();

  SIMM_TRACE_POINT(*ctx, simm::trace::TracePointType::CLIENT_PUT_MESSENGER_START);

  auto ret = call_sync<KVPutRequestPB, KVPutResponsePB>(
      shard_id, static_cast<sicl::rpc::ReqType>(simm::ds::KVServerRpcType::RPC_CLIENT_KV_PUT), req, resp, ctx);
  if (ret != CommonErr::OK) {
    MLOG_ERROR("Put key {} rpc failed: {}({})", key, rpc_ctx->ErrorText(), rpc_ctx->ErrorCode());
    return ret;
  } else {
    MLOG_DEBUG("{}: key({}) ret_code({})", resp->GetTypeName(), key, resp->ret_code());
    if (resp->ret_code() != 0) {
      MLOG_ERROR("KVPut key {} failed: {}", key, resp->ret_code());

      // FIXME(szzhao): remove workaround after it's implemented in data server
      if (resp->ret_code() == DsErr::DataAlreadyExists) {
        return DsErr::DataAlreadyExists;
      }

      return ClntErr::ClntPutObjectFailed;
    }
  }
  SIMM_TRACE_POINT(*ctx, simm::trace::TracePointType::CLIENT_PUT_MESSENGER_END);

  return CommonErr::OK;
}

error_code_t ClientMessenger::AsyncPut(const std::string &key,
                                       std::shared_ptr<simm::common::MemBlock> memp,
                                       std::function<void(int)> callback,
                                       std::shared_ptr<simm::common::SimmContext> ctx) {
  uint16_t shard_id = hashkit_->generate_16bit_hash_value(key.c_str(), key.length()) % shard_num_;

  sicl::transport::MemDesc *mem_desc;
  if (memp->descr == nullptr) {
    MLOG_ERROR("AsyncPut KV MemBlock has no descr ptr");
    return CommonErr::InvalidArgument;
  } else {
    mem_desc = static_cast<sicl::transport::MemDesc *>(memp->descr);
  }

  // build KVPut rpc
  sicl::rpc::RpcContext *ctx_p;
  sicl::rpc::RpcContext::newInstance(ctx_p);
  std::shared_ptr<sicl::rpc::RpcContext> rpc_ctx = std::shared_ptr<sicl::rpc::RpcContext>(ctx_p);
  ctx->set_rpc_ctx(rpc_ctx);
  rpc_ctx->set_timeout(async_req_timeout_ms_);
  KVPutRequestPB req;
  req.set_shard_id(static_cast<uint32_t>(shard_id));
  req.set_key(key.c_str());
  req.set_val_len(memp->len);
  req.set_buf_addr(reinterpret_cast<google::protobuf::uint64>(memp->buf));
  req.set_buf_ofs(0);
  req.set_buf_len(static_cast<google::protobuf::uint64>(mem_desc->getSize()));
  for (const auto rkey : mem_desc->getRemoteKeys()) {
    req.add_buf_rkey(static_cast<google::protobuf::uint32>(rkey));
  }
  auto resp = std::make_shared<KVPutResponsePB>();
  auto ds_ctx_before_req = shard_table_.at(shard_id);
  auto gen_num_before_req = ds_ctx_before_req->gen_num.load();
  auto done = [this, resp, key, ds_ctx_before_req, gen_num_before_req, shard_id, cb = std::move(callback)](
                  const google::protobuf::Message *rsp, const std::shared_ptr<sicl::rpc::RpcContext> rpc_ctx) {
    if (rpc_ctx->Failed()) {
      MLOG_ERROR("Async put key {} rpc failed: {}({})", key, rpc_ctx->ErrorText(), rpc_ctx->ErrorCode());
      HandleAsyncRequestFailure(rpc_ctx, ds_ctx_before_req, shard_id, gen_num_before_req);
      cb(rpc_ctx->ErrorCode());
    } else {
      auto new_resp = static_cast<const KVPutResponsePB *>(rsp);
      MLOG_DEBUG("{}: key({}) ret_code({})", new_resp->GetTypeName(), key, new_resp->ret_code());
      if (new_resp->ret_code() != 0) {
        MLOG_ERROR("AsyncKVPut key {} failed: {}", key, new_resp->ret_code());
      }
      cb(new_resp->ret_code());
    }
  };

#ifdef SIMM_APIPERF
  auto t1 = std::chrono::steady_clock::now();
#endif
  call_async<KVPutRequestPB, KVPutResponsePB>(
      shard_id,
      static_cast<sicl::rpc::ReqType>(simm::ds::KVServerRpcType::RPC_CLIENT_KV_PUT),
      req,
      resp,
      ctx,
      std::move(done));
#ifdef SIMM_APIPERF
  auto t2 = std::chrono::steady_clock::now();
  MLOG_INFO("Perf-aput-callasync key:{} Lat:{} us",
            key,
            std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count());
#endif

  return CommonErr::OK;
}

int32_t ClientMessenger::Get(const std::string &key,
                             std::shared_ptr<simm::common::MemBlock> memp,
                             std::shared_ptr<simm::common::SimmContext> ctx) {
  uint16_t shard_id = hashkit_->generate_16bit_hash_value(key.c_str(), key.length()) % shard_num_;

  sicl::transport::MemDesc *mem_desc;
  if (memp->descr == nullptr) {
    MLOG_ERROR("Get KV MemBlock has no descr ptr");
    return CommonErr::InvalidArgument;
  } else {
    mem_desc = static_cast<sicl::transport::MemDesc *>(memp->descr);
  }

  // build KVGet rpc
  sicl::rpc::RpcContext *ctx_p;
  sicl::rpc::RpcContext::newInstance(ctx_p);
  std::shared_ptr<sicl::rpc::RpcContext> rpc_ctx = std::shared_ptr<sicl::rpc::RpcContext>(ctx_p);
  ctx->set_rpc_ctx(rpc_ctx);
  rpc_ctx->set_timeout(sync_req_timeout_ms_);
  KVGetRequestPB req;
  req.set_shard_id(static_cast<uint32_t>(shard_id));
  req.set_key(key.c_str());
  req.set_buf_addr(reinterpret_cast<google::protobuf::uint64>(memp->buf));
  req.set_buf_ofs(0);
  req.set_buf_len(static_cast<google::protobuf::uint64>(mem_desc->getSize()));
  for (const auto rkey : mem_desc->getRemoteKeys()) {
    req.add_buf_rkey(static_cast<google::protobuf::uint32>(rkey));
  }
  auto resp = std::make_shared<KVGetResponsePB>();
#ifdef SIMM_APIPERF
  auto t1 = std::chrono::steady_clock::now();
#endif
  auto ret = call_sync<KVGetRequestPB, KVGetResponsePB>(
      shard_id, static_cast<sicl::rpc::ReqType>(simm::ds::KVServerRpcType::RPC_CLIENT_KV_GET), req, resp, ctx);
#ifdef SIMM_APIPERF
  auto t2 = std::chrono::steady_clock::now();
  MLOG_INFO("Perf-get-callsync key:{} Lat:{} us",
            key,
            std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count());
#endif
  if (ret != CommonErr::OK) {
    MLOG_ERROR("Get key {} rpc failed: {}({})", key, rpc_ctx->ErrorText(), rpc_ctx->ErrorCode());
    return ret;
  } else {
    MLOG_DEBUG("{}: key({}) ret_code({}) val_len({})", resp->GetTypeName(), key, resp->ret_code(), resp->val_len());
    if (resp->ret_code() != 0) {
      MLOG_ERROR("KVGet key {} failed: {}", key, resp->ret_code());
      return ClntErr::ClntGetObjectFailed;
    }
  }

  return resp->val_len();
}

error_code_t ClientMessenger::AsyncGet(const std::string &key,
                                       std::shared_ptr<simm::common::MemBlock> memp,
                                       std::function<void(int)> callback,
                                       std::shared_ptr<simm::common::SimmContext> ctx) {
  uint16_t shard_id = hashkit_->generate_16bit_hash_value(key.c_str(), key.length()) % shard_num_;

  // memblock must have descr ptr
  sicl::transport::MemDesc *mem_desc;
  if (memp->descr == nullptr) {
    MLOG_ERROR("Get KV MemBlock has no descr ptr");
    return CommonErr::InvalidArgument;
  } else {
    mem_desc = static_cast<sicl::transport::MemDesc *>(memp->descr);
  }

  // build KVGet rpc
  sicl::rpc::RpcContext *ctx_p;
  sicl::rpc::RpcContext::newInstance(ctx_p);
  std::shared_ptr<sicl::rpc::RpcContext> rpc_ctx = std::shared_ptr<sicl::rpc::RpcContext>(ctx_p);
  ctx->set_rpc_ctx(rpc_ctx);
  rpc_ctx->set_timeout(async_req_timeout_ms_);
  KVGetRequestPB req;
  req.set_shard_id(static_cast<uint32_t>(shard_id));
  req.set_key(key.c_str());
  req.set_buf_addr(reinterpret_cast<google::protobuf::uint64>(memp->buf));
  req.set_buf_ofs(0);
  req.set_buf_len(static_cast<google::protobuf::uint64>(mem_desc->getSize()));
  for (const auto rkey : mem_desc->getRemoteKeys()) {
    req.add_buf_rkey(static_cast<google::protobuf::uint32>(rkey));
  }
  auto resp = std::make_shared<KVGetResponsePB>();
  auto ds_ctx_before_req = shard_table_.at(shard_id);
  auto gen_num_before_req = ds_ctx_before_req->gen_num.load();
  auto done = [this, resp, key, ds_ctx_before_req, gen_num_before_req, shard_id, cb = std::move(callback)](
                  const google::protobuf::Message *rsp, const std::shared_ptr<sicl::rpc::RpcContext> rpc_ctx) {
    if (rpc_ctx->Failed()) {
      MLOG_ERROR("Async get key {} rpc failed: {}({})", key, rpc_ctx->ErrorText(), rpc_ctx->ErrorCode());
      HandleAsyncRequestFailure(rpc_ctx, ds_ctx_before_req, shard_id, gen_num_before_req);
      cb(rpc_ctx->ErrorCode());
    } else {
      auto new_resp = static_cast<const KVGetResponsePB *>(rsp);
      MLOG_DEBUG("{}: key({}) ret_code({}) val_len({})",
                 new_resp->GetTypeName(),
                 key,
                 new_resp->ret_code(),
                 new_resp->val_len());
      if (new_resp->ret_code() != 0) {
        MLOG_ERROR("AsyncKVGet key {} failed: {}", key, new_resp->ret_code());
        cb(new_resp->ret_code());
      } else {
        cb(new_resp->val_len());
      }
    }
  };

#ifdef SIMM_APIPERF
  auto t1 = std::chrono::steady_clock::now();
#endif
  call_async<KVGetRequestPB, KVGetResponsePB>(
      shard_id,
      static_cast<sicl::rpc::ReqType>(simm::ds::KVServerRpcType::RPC_CLIENT_KV_GET),
      req,
      resp,
      ctx,
      std::move(done));
#ifdef SIMM_APIPERF
  auto t2 = std::chrono::steady_clock::now();
  MLOG_INFO("Perf-aget-callasync key:{} Lat:{} us",
            key,
            std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count());
#endif

  return CommonErr::OK;
}

error_code_t ClientMessenger::Delete(const std::string &key, std::shared_ptr<simm::common::SimmContext> ctx) {
  uint16_t shard_id = hashkit_->generate_16bit_hash_value(key.c_str(), key.length()) % shard_num_;

  // build KVDel rpc
  sicl::rpc::RpcContext *ctx_p;
  sicl::rpc::RpcContext::newInstance(ctx_p);
  std::shared_ptr<sicl::rpc::RpcContext> rpc_ctx = std::shared_ptr<sicl::rpc::RpcContext>(ctx_p);
  ctx->set_rpc_ctx(rpc_ctx);
  rpc_ctx->set_timeout(sync_req_timeout_ms_);
  KVDelRequestPB req;
  req.set_shard_id(static_cast<uint32_t>(shard_id));
  req.set_key(key.c_str());
  auto resp = std::make_shared<KVDelResponsePB>();
  auto ret = call_sync<KVDelRequestPB, KVDelResponsePB>(
      shard_id, static_cast<sicl::rpc::ReqType>(simm::ds::KVServerRpcType::RPC_CLIENT_KV_DEL), req, resp, ctx);
  if (ret != CommonErr::OK) {
    MLOG_ERROR("Delete key {} rpc failed: {}({})", key, rpc_ctx->ErrorText(), rpc_ctx->ErrorCode());
    return ret;
  } else {
    MLOG_DEBUG("{}: key({}) ret_code({})", resp->GetTypeName(), key, resp->ret_code());
    if (resp->ret_code() != 0) {
      MLOG_ERROR("KVDelete key {} failed: {}", key, resp->ret_code());
      return static_cast<error_code_t>(resp->ret_code());
    }
  }

  return CommonErr::OK;
}

error_code_t ClientMessenger::AsyncDelete(const std::string &key,
                                          std::function<void(int)> callback,
                                          std::shared_ptr<simm::common::SimmContext> ctx) {
  uint16_t shard_id = hashkit_->generate_16bit_hash_value(key.c_str(), key.length()) % shard_num_;

  // build KVDel rpc
  sicl::rpc::RpcContext *ctx_p;
  sicl::rpc::RpcContext::newInstance(ctx_p);
  std::shared_ptr<sicl::rpc::RpcContext> rpc_ctx = std::shared_ptr<sicl::rpc::RpcContext>(ctx_p);
  ctx->set_rpc_ctx(rpc_ctx);
  rpc_ctx->set_timeout(async_req_timeout_ms_);
  KVDelRequestPB req;
  req.set_shard_id(static_cast<uint32_t>(shard_id));
  req.set_key(key.c_str());
  auto resp = std::make_shared<KVDelResponsePB>();
  auto ds_ctx_before_req = shard_table_.at(shard_id);
  auto gen_num_before_req = ds_ctx_before_req->gen_num.load();
  auto done = [this, resp, key, ds_ctx_before_req, gen_num_before_req, shard_id, cb = std::move(callback)](
                  const google::protobuf::Message *rsp, const std::shared_ptr<sicl::rpc::RpcContext> rpc_ctx) {
    if (rpc_ctx->Failed()) {
      MLOG_ERROR("AsyncDelete key {} rpc failed: {}({})", key, rpc_ctx->ErrorText(), rpc_ctx->ErrorCode());
      HandleAsyncRequestFailure(rpc_ctx, ds_ctx_before_req, shard_id, gen_num_before_req);
      cb(rpc_ctx->ErrorCode());
    } else {
      auto new_resp = static_cast<const KVDelResponsePB *>(rsp);
      MLOG_DEBUG("{}: key({}) ret_code({})", new_resp->GetTypeName(), key, new_resp->ret_code());
      if (new_resp->ret_code() != 0) {
        MLOG_ERROR("AsyncKVDelete key {} failed: {}", key, new_resp->ret_code());
      }
      cb(new_resp->ret_code());
    }
  };
  call_async<KVDelRequestPB, KVDelResponsePB>(
      shard_id,
      static_cast<sicl::rpc::ReqType>(simm::ds::KVServerRpcType::RPC_CLIENT_KV_DEL),
      req,
      resp,
      ctx,
      std::move(done));

  return CommonErr::OK;
}

error_code_t ClientMessenger::Exists(const std::string &key, std::shared_ptr<simm::common::SimmContext> ctx) {
  uint16_t shard_id = hashkit_->generate_16bit_hash_value(key.c_str(), key.length()) % shard_num_;

  // build KVLookup rpc
  sicl::rpc::RpcContext *ctx_p;
  sicl::rpc::RpcContext::newInstance(ctx_p);
  std::shared_ptr<sicl::rpc::RpcContext> rpc_ctx = std::shared_ptr<sicl::rpc::RpcContext>(ctx_p);
  ctx->set_rpc_ctx(rpc_ctx);
  rpc_ctx->set_timeout(sync_req_timeout_ms_);
  KVLookupRequestPB req;
  req.set_shard_id(static_cast<uint32_t>(shard_id));
  req.set_key(key.c_str());
  auto resp = std::make_shared<KVLookupResponsePB>();
  auto ret = call_sync<KVLookupRequestPB, KVLookupResponsePB>(
      shard_id, static_cast<sicl::rpc::ReqType>(simm::ds::KVServerRpcType::RPC_CLIENT_KV_LOOKUP), req, resp, ctx);
  if (ret != CommonErr::OK) {
    MLOG_ERROR("Lookup key {} rpc failed: {}({})", key, rpc_ctx->ErrorText(), rpc_ctx->ErrorCode());
    return ret;
  } else {
    MLOG_DEBUG("{}: key({}) ret_code({})", resp->GetTypeName(), key, resp->ret_code());
    if (resp->ret_code() != 0) {
      MLOG_ERROR("KVLookup key {} failed: {}", key, resp->ret_code());
      return resp->ret_code();
    }
  }

  return CommonErr::OK;
}

error_code_t ClientMessenger::AsyncExists(const std::string &key,
                                          std::function<void(int)> callback,
                                          std::shared_ptr<simm::common::SimmContext> ctx) {
  uint16_t shard_id = hashkit_->generate_16bit_hash_value(key.c_str(), key.length()) % shard_num_;

  // build KVLookup rpc
  sicl::rpc::RpcContext *ctx_p;
  sicl::rpc::RpcContext::newInstance(ctx_p);
  std::shared_ptr<sicl::rpc::RpcContext> rpc_ctx = std::shared_ptr<sicl::rpc::RpcContext>(ctx_p);
  ctx->set_rpc_ctx(rpc_ctx);
  rpc_ctx->set_timeout(async_req_timeout_ms_);
  KVLookupRequestPB req;
  req.set_shard_id(static_cast<uint32_t>(shard_id));
  req.set_key(key.c_str());
  auto resp = std::make_shared<KVLookupResponsePB>();
  auto ds_ctx_before_req = shard_table_.at(shard_id);
  auto gen_num_before_req = ds_ctx_before_req->gen_num.load();
  auto done = [this, resp, key, ds_ctx_before_req, gen_num_before_req, shard_id, cb = std::move(callback)](
                  const google::protobuf::Message *rsp, const std::shared_ptr<sicl::rpc::RpcContext> rpc_ctx) {
    if (rpc_ctx->Failed()) {
      MLOG_ERROR("Async lookup key {} rpc failed: {}({})", key, rpc_ctx->ErrorText(), rpc_ctx->ErrorCode());
      HandleAsyncRequestFailure(rpc_ctx, ds_ctx_before_req, shard_id, gen_num_before_req);
      cb(rpc_ctx->ErrorCode());
    } else {
      auto new_resp = static_cast<const KVLookupResponsePB *>(rsp);
      MLOG_DEBUG("{}: key({}) ret_code({})", new_resp->GetTypeName(), key, new_resp->ret_code());
      if (new_resp->ret_code() != 0) {
        MLOG_ERROR("AsyncKVLookup key {} failed: {}", key, new_resp->ret_code());
      }
      cb(new_resp->ret_code());
    }
  };
  call_async<KVLookupRequestPB, KVLookupResponsePB>(
      shard_id,
      static_cast<sicl::rpc::ReqType>(simm::ds::KVServerRpcType::RPC_CLIENT_KV_LOOKUP),
      req,
      resp,
      ctx,
      std::move(done));

  return CommonErr::OK;
}

std::vector<error_code_t> ClientMessenger::MultiPut(const std::vector<std::string> &keys,
                                                    std::vector<std::shared_ptr<simm::common::MemBlock>> datas) {
  if (keys.size() != datas.size()) {
    MLOG_ERROR("MultiPut: keys size {} not equal to datas size {}", keys.size(), datas.size());
    return std::vector<error_code_t>(keys.size(), CommonErr::InvalidArgument);
  }
  auto multiGetResults =
      folly::coro::blockingWait([this, datas, keys]() -> folly::coro::Task<std::vector<error_code_t>> {
        std::vector<folly::Future<error_code_t>> tasks;
        for (size_t i = 0; i < keys.size(); ++i) {
          auto key = keys[i];
          auto memp = datas[i];
          auto ctx = std::make_shared<simm::common::SimmContext>();
          tasks.push_back(
              folly::via(&executor_, [this, key, memp, ctx]() -> error_code_t { return this->Put(key, memp, ctx); }));
        }
        auto results = co_await folly::coro::collectAllRange(std::move(tasks));
        co_return results;
      }());

  // Error handling and logging will happen in this->Put() and upper calls(like KVStore::MGet).
  // Now just return error code.
  return multiGetResults;
}

std::vector<int32_t> ClientMessenger::MultiGet(const std::vector<std::string> &keys,
                                               std::vector<std::shared_ptr<simm::common::MemBlock>> datas) {
  if (keys.size() != datas.size()) {
    MLOG_ERROR("MultiGet: keys size {} not equal to datas size {}", keys.size(), datas.size());
    return std::vector<int32_t>(keys.size(), CommonErr::InvalidArgument);
  }
  auto multiGetResults = folly::coro::blockingWait([this, keys, datas]() -> folly::coro::Task<std::vector<int32_t>> {
    std::vector<folly::Future<int32_t>> tasks;
    for (size_t i = 0; i < keys.size(); ++i) {
      auto key = keys[i];
      auto memp = datas[i];
      auto ctx = std::make_shared<simm::common::SimmContext>();
      tasks.push_back(
          folly::via(&executor_, [this, key, memp, ctx]() -> int32_t { return this->Get(key, memp, ctx); }));
    }
    auto results = co_await folly::coro::collectAllRange(std::move(tasks));
    co_return results;
  }());

  // Error handling and logging will happen in this->Get() and upper calls(like KVStore::MGet).
  // Now just return error code.
  return multiGetResults;
}

std::vector<error_code_t> ClientMessenger::MultiExists(const std::vector<std::string> &keys) {
  auto multiExistsResults = folly::coro::blockingWait([this, keys]() -> folly::coro::Task<std::vector<error_code_t>> {
    std::vector<folly::Future<error_code_t>> tasks;
    for (size_t i = 0; i < keys.size(); ++i) {
      auto key = keys[i];
      auto ctx = std::make_shared<simm::common::SimmContext>();
      tasks.push_back(folly::via(&executor_, [this, key, ctx]() -> error_code_t { return this->Exists(key, ctx); }));
    }
    auto results = co_await folly::coro::collectAllRange(std::move(tasks));
    co_return results;
  }());

  // Error handling and logging will happen in this->Exists() and upper calls(like KVStore::MExists).
  // Now just return error code.
  return multiExistsResults;
}

inline sicl::transport::TimerTick ClientMessenger::convert_timeout_setting_to_timer_tick(int32_t timeout_ms) {
  if (timeout_ms < 0) {
    // if timeout is set to negative value, means wait forever
    return sicl::transport::TimerTick::TIMER_END;
  } else if (timeout_ms <= 1) {
    return sicl::transport::TimerTick::TIMER_1MS;
  } else if (timeout_ms <= 3) {
    return sicl::transport::TimerTick::TIMER_3MS;
  } else if (timeout_ms <= 30) {
    return sicl::transport::TimerTick::TIMER_30MS;
  } else if (timeout_ms <= 300) {
    return sicl::transport::TimerTick::TIMER_300MS;
  } else if (timeout_ms <= 1000) {
    return sicl::transport::TimerTick::TIMER_1S;
  } else if (timeout_ms <= 3000) {
    return sicl::transport::TimerTick::TIMER_3S;
  } else if (timeout_ms <= 5000) {
    return sicl::transport::TimerTick::TIMER_5S;
  } else if (timeout_ms <= 10000) {
    return sicl::transport::TimerTick::TIMER_10S;
  } else if (timeout_ms <= 30000) {
    return sicl::transport::TimerTick::TIMER_30S;
  } else {
    return sicl::transport::TimerTick::TIMER_60S;
  }
}

}  // namespace clnt
}  // namespace simm
