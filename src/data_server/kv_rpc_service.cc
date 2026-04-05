#include <rpc/rpc.h>
#include <atomic>
#include <cstdlib>
#include <mutex>
#include <string>

#include <gflags/gflags.h>

#include <folly/Likely.h>
#include <folly/hash/SpookyHashV2.h>

#include "cluster_manager/cm_rpc_handler.h"
#include "common/base/consts.h"
#include "common/context/context.h"
#include "common/errcode/errcode_def.h"
#include "common/logging/logging.h"
#include "common/rpc_handlers/common_rpc_handlers.h"
#include "common/trace/trace.h"
#include "common/utils/ip_util.h"
#include "common/utils/k8s_util.h"
#include "common/utils/sys_util.h"
#include "common/utils/time_util.h"
#include "data_server/kv_cache_evictor.h"
#include "data_server/kv_cache_pool.h"
#include "data_server/kv_hash_table.h"
#include "data_server/kv_object_pool.h"
#include "data_server/kv_rpc_handler.h"
#include "common/admin/admin_server.h"
#include "data_server/kv_rpc_service.h"
#include "proto/common.pb.h"

DECLARE_LOG_MODULE("data_server");

DECLARE_uint32(cm_connect_retry_interval_sec);
DECLARE_uint32(ds_free_memory_usable_ratio);
DECLARE_int32(ds_initial_blocks);
DECLARE_uint64(memory_limit_bytes);
DECLARE_string(cm_namespace);
DECLARE_string(cm_svc_name);
DECLARE_string(cm_port_name);
DECLARE_uint32(cm_hb_tolerance_count);
DECLARE_bool(ds_process_exit_cm_disconnection);
DECLARE_bool(simm_enable_trace);

namespace simm {
namespace ds {

inline uint64_t Hasher(const std::string &key_str) {
  return folly::hash::SpookyHashV2::Hash64(key_str.c_str(), key_str.length(), FLAGS_ds_hash_seed);
}

KVRpcService::KVRpcService() = default;
KVRpcService::~KVRpcService() {
  Stop();
  if (keepalive_thread_) {
    {
      std::unique_lock<std::mutex> heartbeat_lock(heartbeat_mutex_);
      heartbeat_condv_.notify_all();
    }
    keepalive_thread_->join();
    keepalive_thread_.reset();
  }
  (void)io_service_.reset();
  (void)mgmt_client_.reset();
  (void)mgt_service_.reset();
  (void)admin_rpc_service_.reset();
  (void)object_pool_.release();  // static variable cannot be deleted
  cache_pool_.reset();
  cache_evictor_.reset();
  for (auto [_, table] : all_tables_) {
    if (table)
      delete table;
  }
  all_tables_.clear();
}

error_code_t KVRpcService::Init() {
  if (!initialized_) {
    sicl::rpc::SiRPC *io_service = nullptr;
    auto res = sicl::rpc::SiRPC::newInstance(io_service, true);
    if (res != sicl::transport::Result::SICL_SUCCESS) {
      MLOG_ERROR("KVRpcService::Init new SiRPC failed, res:{}", std::to_string(res));
      return DsErr::InitFailed;
    }
    io_service_.reset(io_service);
    sicl::rpc::SiRPC *mgt_client = nullptr;
    res = sicl::rpc::SiRPC::newInstance(mgt_client, false);
    if (res != sicl::transport::Result::SICL_SUCCESS) {
      MLOG_ERROR("KVRpcService::Init new SiRPC failed, res:{}", std::to_string(res));
      return DsErr::InitFailed;
    }
    mgmt_client_.reset(mgt_client);
    sicl::rpc::SiRPC *mgt_service = nullptr;
    res = sicl::rpc::SiRPC::newInstance(mgt_service, true);
    if (res != sicl::transport::Result::SICL_SUCCESS) {
      MLOG_ERROR("KVRpcService::Init new SiRPC failed, res:{}", std::to_string(res));
      return DsErr::InitFailed;
    }
    mgt_service_.reset(mgt_service);
    sicl::rpc::SiRPC *admin_rpc_service = nullptr;
    res = sicl::rpc::SiRPC::newInstance(admin_rpc_service, true);
    if (res != sicl::transport::Result::SICL_SUCCESS) {
      MLOG_ERROR("KVRpcService::Init new SiRPC failed, res:{}", std::to_string(res));
      return DsErr::InitFailed;
    }
    admin_rpc_service_.reset(admin_rpc_service);
    object_pool_.reset(&KVObjectPool::Instance());
    cache_pool_ = std::make_unique<KVCachePool>();
    cache_evictor_ = std::make_unique<KVCacheEvictor>(this);
    uint64_t system_level_free_memory = FLAGS_memory_limit_bytes;
    if (system_level_free_memory == 0) {
      auto result = utils::GetMemoryFreeToUse();
      if (result < 0) {
        MLOG_ERROR("KVRpcService::Init get system level free memory failed, ret:{}", result);
        return DsErr::InitFailed;
      }
      system_level_free_memory = static_cast<uint64_t>(result);
    }
    auto cache_bound_bytes = system_level_free_memory * FLAGS_ds_free_memory_usable_ratio / 100;
    int ret =
        cache_pool_->init(cache_bound_bytes, io_service->GetMempool(), cache_evictor_.get(), FLAGS_ds_initial_blocks);
    if (ret) {
      MLOG_ERROR("KVRpcService::Init new cache pool failed, ret:{}", ret);
      return DsErr::InitFailed;
    }
    all_tables_.reserve(FLAGS_shard_total_num);
    shard_used_bytes_.resize(FLAGS_shard_total_num);
    for (size_t i = 0; i < FLAGS_shard_total_num; i++) {
      all_tables_[i] = nullptr;
      shard_used_bytes_[i].store(0, std::memory_order_relaxed);
    }
    SetCMAddressFromK8S();
    initialized_ = true;
  }
  return CommonErr::OK;  // Success
}

error_code_t KVRpcService::Start() {
  if (is_running_) {
    return CommonErr::OK;  // Already running
  }
  auto ret = StartRPCServices();
  if (ret != CommonErr::OK) {
    return ret;
  }
  ret = RegisterHandlers();
  if (ret != CommonErr::OK) {
    StopRPCServices();
    return ret;
  }
  is_running_ = true;
  // start keepalive thread only if cm_primary_node_ip was specified
  if (!keepalive_thread_ && !FLAGS_cm_primary_node_ip.empty()) {
    keepalive_thread_ = std::make_unique<std::thread>(&KVRpcService::KeepAlive, this);
    pthread_setname_np(keepalive_thread_->native_handle(), "keepalive");
  }

#ifdef SIMM_ENABLE_TRACE
  simm::trace::TraceManager::Instance().SetEnabled(FLAGS_simm_enable_trace);
#endif
  return CommonErr::OK;  // Success
}

error_code_t KVRpcService::Stop() {
  if (!is_running_) {
    return CommonErr::OK;  // Already stopped
  }
  is_registered_ = true;
  register_condv_.post();
  is_running_ = false;
  StopRPCServices();
  return CommonErr::OK;
}

error_code_t KVRpcService::StartRPCServices() {
  auto res = io_service_->Start(FLAGS_io_service_port);
  if (static_cast<sicl::transport::Result>(res) != sicl::transport::Result::SICL_SUCCESS) {
    MLOG_ERROR("Failed to start RPC IO service on port:{}, res:{}", FLAGS_io_service_port, std::to_string(res));
    return DsErr::InitRPCServiceFailed;
  }
  res = mgt_service_->Start(FLAGS_mgt_service_port);
  if (static_cast<sicl::transport::Result>(res) != sicl::transport::Result::SICL_SUCCESS) {
    MLOG_ERROR(
        "Failed to start RPC management service on port:{}, res:{}", FLAGS_mgt_service_port, std::to_string(res));
    return DsErr::InitRPCServiceFailed;
  }
  res = admin_rpc_service_->Start(FLAGS_ds_rpc_admin_port);
  if (static_cast<sicl::transport::Result>(res) != sicl::transport::Result::SICL_SUCCESS) {
    MLOG_ERROR("Failed to start RPC admin service on port:{}, res:{}", FLAGS_ds_rpc_admin_port, std::to_string(res));
    return DsErr::InitRPCServiceFailed;
  }
  local_ip_ = utils::GetHostIp();
  return CommonErr::OK;  // Success
}

error_code_t KVRpcService::RegisterHandlers() {
  bool res;
  res = io_service_->RegisterHandler(static_cast<sicl::rpc::ReqType>(ds::KVServerRpcType::RPC_CLIENT_KV_GET),
                                     new KVGetHandler(this, new KVGetRequestPB));
  if (!res) {
    MLOG_ERROR("RegisterHandler for RPC_CLIENT_KV_GET({}) failed",
               static_cast<int>(ds::KVServerRpcType::RPC_CLIENT_KV_GET));
    return DsErr::RegisterRPCHandlerFailed;
  }
  res = io_service_->RegisterHandler(static_cast<sicl::rpc::ReqType>(ds::KVServerRpcType::RPC_CLIENT_KV_PUT),
                                     new KVPutHandler(this, new KVPutRequestPB));
  if (!res) {
    MLOG_ERROR("RegisterHandler for RPC_CLIENT_KV_PUT({}) failed",
               static_cast<int>(ds::KVServerRpcType::RPC_CLIENT_KV_PUT));
    return DsErr::RegisterRPCHandlerFailed;
  }
  res = io_service_->RegisterHandler(static_cast<sicl::rpc::ReqType>(ds::KVServerRpcType::RPC_CLIENT_KV_DEL),
                                     new KVDelHandler(this, new KVDelRequestPB));
  if (!res) {
    MLOG_ERROR("RegisterHandler for RPC_CLIENT_KV_DEL({}) failed",
               static_cast<int>(ds::KVServerRpcType::RPC_CLIENT_KV_DEL));
    return DsErr::RegisterRPCHandlerFailed;
  }
  res = io_service_->RegisterHandler(static_cast<sicl::rpc::ReqType>(ds::KVServerRpcType::RPC_CLIENT_KV_LOOKUP),
                                     new KVLookupHandler(this, new KVLookupRequestPB));
  if (!res) {
    MLOG_ERROR("RegisterHandler for RPC_CLIENT_KV_LOOKUP({}) failed",
               static_cast<int>(ds::KVServerRpcType::RPC_CLIENT_KV_LOOKUP));
    return DsErr::RegisterRPCHandlerFailed;
  }
  // control plane
  res = mgt_service_->RegisterHandler(
      static_cast<sicl::rpc::ReqType>(cm::ClusterManagerRpcType::RPC_DATASERVER_RESOURCE_QUERY),
      new MgtResourceHandler(this, new DataServerResourceRequestPB));
  if (!res) {
    MLOG_ERROR("RegisterHandler for RPC_DATASERVER_RESOURCE_QUERY({}) failed",
               static_cast<int>(cm::ClusterManagerRpcType::RPC_DATASERVER_RESOURCE_QUERY));
    return DsErr::RegisterRPCHandlerFailed;
  }
  // admin
  res = admin_rpc_service_->RegisterHandler(
      static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_GET_GFLAG_REQ),
      new simm::common::GetGFlagHandler(admin_rpc_service_.get(), new proto::common::GetGFlagValueRequestPB));
  if (!res) {
    MLOG_ERROR("RegisterHandler for RPC_GET_GFLAG_REQ({}) failed",
               static_cast<int>(simm::common::CommonRpcType::RPC_GET_GFLAG_REQ));
    return DsErr::RegisterRPCHandlerFailed;
  }
  res = admin_rpc_service_->RegisterHandler(
      static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_SET_GFLAG_REQ),
      new simm::common::SetGFlagHandler(admin_rpc_service_.get(), new proto::common::SetGFlagValueRequestPB));
  if (!res) {
    MLOG_ERROR("RegisterHandler for RPC_SET_GFLAG_REQ({}) failed",
               static_cast<int>(simm::common::CommonRpcType::RPC_SET_GFLAG_REQ));
    return DsErr::RegisterRPCHandlerFailed;
  }
  res = admin_rpc_service_->RegisterHandler(
      static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_LIST_GFLAGS_REQ),
      new simm::common::ListGFlagsHandler(admin_rpc_service_.get(), new proto::common::ListAllGFlagsRequestPB));
  if (!res) {
    MLOG_ERROR("RegisterHandler for RPC_LIST_GFLAGS_REQ({}) failed",
               static_cast<int>(simm::common::CommonRpcType::RPC_LIST_GFLAGS_REQ));
    return DsErr::RegisterRPCHandlerFailed;
  }
#ifdef SIMM_ENABLE_TRACE
  res = admin_rpc_service_->RegisterHandler(
      static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_TRACE_TOGGLE_REQ),
      new simm::common::TraceToggleHandler(admin_rpc_service_.get(), new proto::common::TraceToggleRequestPB));
  if (!res) {
    MLOG_ERROR("RegisterHandler for RPC_TRACE_TOGGLE_REQ({}) failed",
               static_cast<int>(simm::common::CommonRpcType::RPC_TRACE_TOGGLE_REQ));
    return DsErr::RegisterRPCHandlerFailed;
  }
#endif
  return CommonErr::OK;  // Success
}

error_code_t KVRpcService::StopRPCServices() {
  // io_service_->RunUntilAskedToQuit();
  // mgt_service_->RunUntilAskedToQuit();
  return CommonErr::OK;
}

void KVRpcService::SetCMAddressFromK8S() {
  // get cluster manager pod info from K8S api, get vector of [pod_name, pod_ip]
  // TODO: change to get cluster manager info from etcd
  auto [ret, cm_ips] =
      simm::utils::GetPodIpsOfHeadlessService(FLAGS_cm_namespace, FLAGS_cm_svc_name, FLAGS_cm_port_name);
  if (ret != 0) {
    MLOG_ERROR("get cluster manager service info from k8s failed");
    return;
  }
  // online cluster manager pod can only be 1
  if (cm_ips.size() != 1) {
    MLOG_ERROR("Invalid cluster manager svc pods: {}", cm_ips.size());
    return;
  }
  auto cm_addr = simm::common::NodeAddress::ParseFromString(cm_ips[0].second);
  if (!cm_addr) {
    MLOG_ERROR("Invalid server address format: {}", cm_ips[0].second);
    return;
  }
  gflags::SetCommandLineOption("cm_primary_node_ip", cm_addr->node_ip_.c_str());
  gflags::SetCommandLineOption("cm_primary_node_port", std::to_string(cm_addr->node_port_).c_str());
}

void KVRpcService::KeepAlive() {
  while (true) {
    while (!is_registered_) {
      RegisterOnCluster();
      register_condv_.try_wait_for(std::chrono::seconds(FLAGS_register_cooldown_sec));
      register_condv_.reset();  // folly::Bation should be reset before reuse
    }
    while (!cm_ready_) {
      register_condv_.reset();

      sicl::rpc::SiRPC *mgt_client = nullptr;
      sicl::rpc::SiRPC::newInstance(mgt_client, false);
      mgmt_client_.reset(mgt_client);
      SetCMAddressFromK8S();

      RegisterToRestartedManager();
      register_condv_.try_wait_for(std::chrono::seconds(FLAGS_cm_connect_retry_interval_sec));
    }
    {
      std::unique_lock<std::mutex> heartbeat_lock(heartbeat_mutex_);
      heartbeat_condv_.wait_for(
          heartbeat_lock, std::chrono::seconds(FLAGS_heartbeat_cooldown_sec), [this] { return !is_running_; });
      if (!is_running_)
        break;
    }
    // do heartbeat with cluster manager
    HeartBeatToCluster();
  }
}

void KVRpcService::RegisterOnCluster() {
  std::string ip = local_ip_;
  int port = FLAGS_io_service_port;

  sicl::rpc::RpcContext *ctx = nullptr;
  sicl::rpc::RpcContext::newInstance(ctx);
  auto handshake_ctx = std::shared_ptr<sicl::rpc::RpcContext>(ctx);
  handshake_ctx->set_timeout(sicl::transport::TimerTick::TIMER_3S);
  NewNodeHandShakeRequestPB handshake_req;
  handshake_req.mutable_node()->set_ip(ip);
  handshake_req.mutable_node()->set_port(port);
  auto handshake_rsp = std::make_shared<NewNodeHandShakeResponsePB>();
  auto handshake_done = [handshake_rsp, this](const google::protobuf::Message *rsp,
                                              const std::shared_ptr<sicl::rpc::RpcContext> ctx) {
    if (!ctx->Failed()) {
      auto response = dynamic_cast<const NewNodeHandShakeResponsePB *>(rsp);
      if (response->ret_code() == CommonErr::OK) {
        is_registered_ = true;
        register_condv_.post();
      } else {
        MLOG_ERROR("RegisterOnCluster return not OK");
      }
    } else {
      MLOG_ERROR(
          "Failed to RegisterOnCluster to Cluster Manager {}:{}", FLAGS_cm_primary_node_ip, FLAGS_cm_rpc_inter_port);
    }
  };

  mgmt_client_->SendRequest(FLAGS_cm_primary_node_ip,
                            FLAGS_cm_rpc_inter_port,
                            static_cast<sicl::rpc::ReqType>(cm::ClusterManagerRpcType::RPC_NEW_NODE_HANDSHAKE),
                            handshake_req,
                            handshake_rsp.get(),
                            handshake_ctx,
                            handshake_done);
}

void KVRpcService::RegisterToRestartedManager() {
  std::string ip = local_ip_;
  int port = FLAGS_io_service_port;

  sicl::rpc::RpcContext *ctx = nullptr;
  sicl::rpc::RpcContext::newInstance(ctx);
  auto handshake_ctx = std::shared_ptr<sicl::rpc::RpcContext>(ctx);
  handshake_ctx->set_timeout(sicl::transport::TimerTick::TIMER_300MS);
  NewNodeHandShakeRequestPB handshake_req;
  handshake_req.mutable_node()->set_ip(ip);
  handshake_req.mutable_node()->set_port(port);
  for (const auto [shard_id, table] : all_tables_) {
    if (table && !table->Empty()) {
      handshake_req.mutable_shard_ids()->Add(shard_id);
    }
  }
  auto handshake_rsp = std::make_shared<NewNodeHandShakeResponsePB>();
  auto handshake_done = [handshake_rsp, this](const google::protobuf::Message *rsp,
                                              const std::shared_ptr<sicl::rpc::RpcContext> ctx) {
    if (!ctx->Failed()) {
      auto response = dynamic_cast<const NewNodeHandShakeResponsePB *>(rsp);
      auto ret_code = response->ret_code();
      if (ret_code == CommonErr::OK) {
        heartbeat_failure_count_.store(0);
        cm_ready_ = true;
        register_condv_.post();
      } else {
        MLOG_ERROR("RegisterToRestartedManager return not OK: {}", ret_code);
      }
    } else {
      MLOG_ERROR("Failed to RegisterToRestartedManager to Cluster Manager {}:{}: {} ({})",
                 FLAGS_cm_primary_node_ip,
                 FLAGS_cm_rpc_inter_port,
                 ctx->ErrorText(),
                 ctx->ErrorCode());
    }
  };

  mgmt_client_->SendRequest(FLAGS_cm_primary_node_ip,
                            FLAGS_cm_rpc_inter_port,
                            static_cast<sicl::rpc::ReqType>(cm::ClusterManagerRpcType::RPC_NEW_NODE_HANDSHAKE),
                            handshake_req,
                            handshake_rsp.get(),
                            handshake_ctx,
                            handshake_done);
}

void KVRpcService::HeartBeatToCluster() {
  std::string ip = local_ip_;
  int port = FLAGS_io_service_port;

  sicl::rpc::RpcContext *ctx = nullptr;
  sicl::rpc::RpcContext::newInstance(ctx);
  auto heartbeat_ctx = std::shared_ptr<sicl::rpc::RpcContext>(ctx);
  heartbeat_ctx->set_timeout(sicl::transport::TimerTick::TIMER_3S);
  DataServerHeartBeatRequestPB heartbeat_req;
  heartbeat_req.mutable_node()->set_ip(ip);
  heartbeat_req.mutable_node()->set_port(port);
  auto heartbeat_rsp = std::make_shared<DataServerHeartBeatResponsePB>();
  auto heartbeat_done = [heartbeat_rsp, this](const google::protobuf::Message *rsp,
                                              const std::shared_ptr<sicl::rpc::RpcContext> ctx) {
    if (!ctx->Failed()) {
      auto response = dynamic_cast<const DataServerHeartBeatResponsePB *>(rsp);
      OnHeartbeatResult(false, response->ret_code());
      if (response->ret_code() != CommonErr::OK) {
        MLOG_ERROR("HeartBeatToCluster return not OK");
      }
    } else {
      OnHeartbeatResult(true, CommonErr::InvalidState);
    }
  };

  mgmt_client_->SendRequest(FLAGS_cm_primary_node_ip,
                            FLAGS_cm_rpc_inter_port,
                            static_cast<sicl::rpc::ReqType>(cm::ClusterManagerRpcType::RPC_NODE_HEARTBEAT),
                            heartbeat_req,
                            heartbeat_rsp.get(),
                            heartbeat_ctx,
                            heartbeat_done);
}

void KVRpcService::OnHeartbeatResult(bool rpc_failed, error_code_t ret_code) {
  if (!rpc_failed && ret_code == CommonErr::OK) {
    // reset failure counter after one HB request succeed
    heartbeat_failure_count_.store(0);
    return;
  }

  if (!rpc_failed) {
    return;
  }

  const auto failure_count = heartbeat_failure_count_.fetch_add(1) + 1;
  MLOG_ERROR("Failed to HeartbeatToCluster to Cluster Manager {}:{} (counter={})",
             FLAGS_cm_primary_node_ip,
             FLAGS_cm_rpc_inter_port,
             failure_count);
  if (failure_count >= FLAGS_cm_hb_tolerance_count) {
    heartbeat_failure_count_.store(0);
    if (FLAGS_ds_process_exit_cm_disconnection) {
      MLOG_CRITICAL("Exit data server for disconnection(HB failed {} times) with cluster manager",
                    FLAGS_cm_hb_tolerance_count);
      HandleClusterManagerDisconnect();
    } else {
      MLOG_WARN("HB failed {} times with cluster manager, switch to CM re-register flow", FLAGS_cm_hb_tolerance_count);
      cm_ready_.store(false);
    }
  }
}

void KVRpcService::HandleClusterManagerDisconnect() {
  cluster_disconnect_handler_();
}

error_code_t KVRpcService::KVGet(std::shared_ptr<simm::common::SimmContext> ctx,
                                 const KVGetRequestPB *req,
                                 KVEntryIntrusivePtr &entry) {
  uint32_t shard_id = req->shard_id();
  auto &key_str = req->key();
  uint64_t key_hash = Hasher(key_str);
  KVHashTable *table = all_tables_[shard_id];
  if (!table) {
    std::lock_guard<std::mutex> table_lock(all_table_mtx_);
    table = all_tables_[shard_id];
    if (!table) {
      MLOG_DEBUG("Table for shard {} is not exists", shard_id);
      return DsErr::KeyNotFound;
    }
  }
  std::unique_ptr<KVMeta, KVObjectPool::KVMetaDeleter> key_meta(object_pool_->AcquireKey(),
                                                                KVObjectPool::KVMetaDeleter{});
  key_meta->key_hash = key_hash;
  key_meta->key_len = key_str.length();
  key_meta->key_ptr = (char *)key_str.c_str();
  KVHashKey key(key_meta.get());
  KVEntryIntrusivePtr value;
  bool found = table->Find(key, value);
  if (FOLLY_UNLIKELY(!found))
    return DsErr::KeyNotFound;
  auto status = value->status.load(std::memory_order_acquire);
  if (status == static_cast<uint32_t>(KVStatus::KV_VALID)) {
    bool res = value->ref_cnt_lock.IncRefCnt();
    while (!res) {
      status = value->status.load(std::memory_order_acquire);
      if (FOLLY_UNLIKELY(status != static_cast<uint32_t>(KVStatus::KV_VALID))) {
        MLOG_ERROR("KVGet key {} get invalid status {} in entry: {}",
                   key.ToString(),
                   KVStatusToString(status),
                   (void *)value.get());
        return DsErr::KVStatusNotValid;
      }
      res = value->ref_cnt_lock.IncRefCnt();
    }
    // wait until exclusive bit cleared by other thread
#ifndef NDEBUG
    MLOG_DEBUG(
        "KVGet pre entrance to key {} at entry: {} {} done", key.ToString(), (void *)value.get(), value->ToString());
#endif
    entry = value;
    return CommonErr::OK;
  } else {
    MLOG_ERROR("KVGet key {} get invalid status {} in entry: {}",
               key.ToString(),
               KVStatusToString(status),
               (void *)value.get());
  }
  return DsErr::KVStatusNotValid;
}

void KVRpcService::KVGetCallback(KVEntryIntrusivePtr &entry) {
  // entry->ref_cnt_lock.SetAccessed(); Disable currently and may rely on BloomFilter in the future
  entry->ref_cnt_lock.DecRefCnt();
#ifndef NDEBUG
  MLOG_DEBUG("KVGet post process at entry: {} {} done", (void *)entry.get(), entry->ToString());
#endif
}

error_code_t KVRpcService::KVPut(std::shared_ptr<simm::common::SimmContext> ctx,
                                 const KVPutRequestPB *req,
                                 KVEntryIntrusivePtr &entry) {
  SIMM_TRACE_POINT(*ctx, simm::trace::TracePointType::DS_PUT_START);

  uint32_t shard_id = req->shard_id();
  auto &key_str = req->key();
  uint64_t key_hash = Hasher(key_str);
  KVHashTable *table = all_tables_[shard_id];
  if (!table) {
    std::lock_guard<std::mutex> table_lock(all_table_mtx_);
    table = all_tables_[shard_id];
    if (!table) {
      table = new KVHashTable();
      table->Init(shard_id);
      all_tables_[shard_id] = table;
    }
  }
  std::unique_ptr<KVMeta, KVObjectPool::KVMetaDeleter> key_meta(object_pool_->AcquireKey(),
                                                                KVObjectPool::KVMetaDeleter{});
  key_meta->key_hash = key_hash;
  key_meta->key_len = key_str.length();
  key_meta->key_ptr = (char *)key_str.c_str();
  KVHashKey key(key_meta.get());
  KVEntryIntrusivePtr value;
  bool found = table->Find(key, value);
  if (!found) {
    // emplace
    SIMM_TRACE_POINT(*ctx, simm::trace::TracePointType::DS_PUT_EMPLACE_START);
    value = KVEntry::createIntrusivePtr();
    value->status.store(static_cast<uint32_t>(KVStatus::KV_INITIALIZING), std::memory_order_release);
    bool res = table->Insert(key, value);
    if (res) {
      SIMM_TRACE_POINT(*ctx, simm::trace::TracePointType::DS_PUT_WRITE_START);
#ifndef NDEBUG
      MLOG_DEBUG("KVPut try to bind entry {} to key {}", (void *)value.get(), key.ToString());
#endif
      value->ref_cnt_lock.SetExclusive();
      value->slab_info.shard_id = shard_id;
      res = cache_pool_->allocate(req->val_len(), &value->slab_info);
      if (FOLLY_UNLIKELY(!res)) {
        MLOG_ERROR(
            "KVPut cache pool allocate failed for key: {}, length: {}, res: {}", key.ToString(), req->val_len(), res);
        value->status.store(static_cast<uint32_t>(KVStatus::KV_CLEAR), std::memory_order_release);
        table->Remove(key);
        value->ref_cnt_lock.ClearExclusive();
        return DsErr::CachePoolAllocateFailed;
      }
      shard_used_bytes_[shard_id].fetch_add(SLAB_CLASS_SIZES[value->slab_info.slab_class] + META_SIZE,
                                            std::memory_order_relaxed);
      auto [meta, _] = KVCachePool::GetBufferPair(&value->slab_info);
      meta->key_len = key_str.length();
      // meta->value_len = req->val_len(); since allocate success would write down value_len
      meta->key_hash = key_hash;
      meta->ctime = utils::get_current_seconds();
      memcpy(meta->key, key_str.c_str(), key_str.length());
      KVHashKey key(meta);
      res = table->Commit(key);
      if (res) {
#ifndef NDEBUG
        MLOG_DEBUG("KVPut done reserve for entry {}: {}, data length: {}",
                   (void *)value.get(),
                   value->ToString(),
                   meta->value_len);
#endif
        entry = value;
        SIMM_TRACE_POINT(*ctx, simm::trace::TracePointType::DS_PUT_END);
        return CommonErr::OK;
      } else {
        MLOG_ERROR("KVPut commit key: {} {} in hash table failed", key.ToString(), value->ToString());
        value->status.store(static_cast<uint32_t>(KVStatus::KV_CLEAR), std::memory_order_release);
        table->Remove(key);
        cache_pool_->free(&value->slab_info);
        shard_used_bytes_[shard_id].fetch_sub(SLAB_CLASS_SIZES[value->slab_info.slab_class] + META_SIZE,
                                              std::memory_order_relaxed);
        value->ref_cnt_lock.ClearExclusive();
        return DsErr::HashTableOperationError;
      }
    } else {
      MLOG_ERROR("KVPut insert key {} to hash table failed, already placed by others", key.ToString());
    }
  } else {
    MLOG_DEBUG("KVPut found existed KVEntry {} {}", (void *)value.get(), value->ToString());
  }
  SIMM_TRACE_POINT(*ctx, simm::trace::TracePointType::DS_PUT_END);
  return DsErr::DataAlreadyExists;
}

void KVRpcService::KVPutFailedRewind(uint32_t shard_id, KVEntryIntrusivePtr &entry) {
  KVHashTable *table = all_tables_[shard_id];
  MLOG_DEBUG("KVPutFailed at entry {}: {}, enter into clearing process...", (void *)entry.get(), entry->ToString());
  entry->status.store(static_cast<uint32_t>(KVStatus::KV_CLEAR), std::memory_order_release);
  auto [meta, _] = KVCachePool::GetBufferPair(&entry->slab_info);
  KVHashKey key(meta);
  table->Remove(key);
  cache_pool_->free(&entry->slab_info);
  shard_used_bytes_[shard_id].fetch_sub(SLAB_CLASS_SIZES[entry->slab_info.slab_class] + META_SIZE,
                                        std::memory_order_relaxed);
  entry->ref_cnt_lock.ClearExclusive();
}

void KVRpcService::KVPutSuccessHooks(KVEntryIntrusivePtr &entry) {
  auto [meta, _] = KVCachePool::GetBufferPair(&entry->slab_info);
  if (entry->ref_cnt_lock.SetLinked()) {
    cache_evictor_->enque(entry);
  }
  entry->status.store(static_cast<uint32_t>(KVStatus::KV_VALID), std::memory_order_release);
  entry->ref_cnt_lock.ClearExclusive();
  std::atomic_ref<uint8_t> active_flag(meta->active_flag);
  active_flag.store(1, std::memory_order_release);
#ifndef NDEBUG
  MLOG_DEBUG("KVPutSuccess at entry {}: {}", (void *)entry.get(), entry->ToString());
#endif
}

error_code_t KVRpcService::KVDel(std::shared_ptr<simm::common::SimmContext> ctx, const KVDelRequestPB *req) {
  uint32_t shard_id = req->shard_id();
  auto &key_str = req->key();
  uint64_t key_hash = Hasher(key_str);
  KVHashTable *table = all_tables_[shard_id];
  if (!table) {
    std::lock_guard<std::mutex> table_lock(all_table_mtx_);
    table = all_tables_[shard_id];
    if (!table) {
      MLOG_DEBUG("Table for shard {} is not exists", shard_id);
      return DsErr::KeyNotFound;
    }
  }
  std::unique_ptr<KVMeta, KVObjectPool::KVMetaDeleter> key_meta(object_pool_->AcquireKey(),
                                                                KVObjectPool::KVMetaDeleter{});
  key_meta->key_hash = key_hash;
  key_meta->key_len = key_str.length();
  key_meta->key_ptr = (char *)key_str.c_str();
  KVHashKey key(key_meta.get());
  KVEntryIntrusivePtr value;
  bool found = table->Find(key, value);
  if (FOLLY_UNLIKELY(!found)) {
    MLOG_ERROR("KVDel key {} cannot be found in hash table", key.ToString());
    return DsErr::KeyNotFound;
  }
  auto status = value->status.load(std::memory_order_acquire);
  if (status == static_cast<uint32_t>(KVStatus::KV_VALID)) {
    bool res = value->ref_cnt_lock.SetExclusive();
    while (!res) {
      status = value->status.load(std::memory_order_acquire);
      if (status == static_cast<uint32_t>(KVStatus::KV_CLEAR)) {
        return CommonErr::OK;
      }
      res = value->ref_cnt_lock.SetExclusive();
    }
    // WARNING: Do NOT use a blocking while loop here to wait for the reference counter to reach 0.
    // The counter is decremented asynchronously in the other callback. If we block the current
    // thread with a busy-wait(or sleep-based loop), the callback would never get a chance to
    // run in this thread or event loop. As a result, the counter will never be reduced and
    // the program will deadlock. Always use a non-blocking mechanism or early return instead.
    // The previous while loop assumes that current `KVDel` is not conflict with concurrent `KVPut`.
    //
    // Wrong example:
    // do {
    //   ref_cnt = value->ref_cnt_lock.GetRefCnt();
    // } while (ref_cnt);
    //
    // Following snippet shows early return when counter referenced by other concurrent `KVGet`.
    uint32_t ref_cnt = value->ref_cnt_lock.GetRefCnt();
    if (ref_cnt > 0) {
      bool timeout = false;
      auto beg_ts = utils::current_microseconds();
      while (ref_cnt > 0 && !timeout) {
        ref_cnt = value->ref_cnt_lock.GetRefCnt();
        if (utils::delta_microseconds_to_now(beg_ts) > FLAGS_busy_wait_timeout_us) {
          timeout = true;
        }
      }
      if (ref_cnt > 0) {
        value->ref_cnt_lock.ClearExclusive();
        MLOG_WARN("KVDel key {} at entry: {} found concurrent inflight GET, omitting...",
                  key.ToString(),
                  (void *)value.get());
        return DsErr::ConcurrentDataRace;
      }
    }
    // wait until exclusive bit set by itself and no reference anymore
#ifndef NDEBUG
    MLOG_DEBUG("KVDel key {} done at entry: {} with snapshot {}, enter into cleaning stage...",
               key.ToString(),
               (void *)value.get(),
               value->ToString());
#endif
    value->status.store(static_cast<uint32_t>(KVStatus::KV_CLEAR), std::memory_order_release);
    if (value->ref_cnt_lock.IsLinked()) {
      cache_evictor_->deque(value);
      value->ref_cnt_lock.ClearLinked();
    }
    table->Remove(key);
    if (cache_pool_->is_active(&value->slab_info)) {
      cache_pool_->free(&value->slab_info);
    }
    shard_used_bytes_[shard_id].fetch_sub(SLAB_CLASS_SIZES[value->slab_info.slab_class] + META_SIZE,
                                          std::memory_order_relaxed);
    value->ref_cnt_lock.ClearExclusive();
    return CommonErr::OK;
  } else {
    MLOG_ERROR("KVDel key {} get invalid status {} in entry: {}",
               key.ToString(),
               KVStatusToString(status),
               (void *)value.get());
  }
  return DsErr::KVStatusNotValid;
}

error_code_t KVRpcService::KVLookup(std::shared_ptr<simm::common::SimmContext> ctx, const KVLookupRequestPB *req) {
  uint32_t shard_id = req->shard_id();
  auto &key_str = req->key();
  uint64_t key_hash = Hasher(key_str);
  KVHashTable *table = all_tables_[shard_id];
  if (!table) {
    std::lock_guard<std::mutex> table_lock(all_table_mtx_);
    table = all_tables_[shard_id];
    if (!table) {
      MLOG_DEBUG("Table for shard {} is not exists", shard_id);
      return DsErr::KeyNotFound;
    }
  }
  std::unique_ptr<KVMeta, KVObjectPool::KVMetaDeleter> key_meta(object_pool_->AcquireKey(),
                                                                KVObjectPool::KVMetaDeleter{});
  key_meta->key_hash = key_hash;
  key_meta->key_len = key_str.length();
  key_meta->key_ptr = (char *)key_str.c_str();
  KVHashKey key(key_meta.get());
  KVEntryIntrusivePtr value;
  bool found = table->Find(key, value);
  if (!found) {
    return DsErr::KeyNotFound;
  }
  return CommonErr::OK;
}

void KVRpcService::GetResourceStats(const DataServerResourceRequestPB *req, DataServerResourceResponsePB *rsp) {
  (void)req;
  std::string ip = local_ip_;
  int port = FLAGS_io_service_port;
  rsp->mutable_node()->set_ip(ip);
  rsp->mutable_node()->set_port(port);
  size_t mem_total_bytes = cache_pool_->mem_total_bytes();
  size_t mem_used_bytes = cache_pool_->mem_used_bytes();
  rsp->set_mem_total_bytes(mem_total_bytes);
  rsp->set_mem_used_bytes(mem_used_bytes);
  rsp->set_mem_free_bytes(mem_total_bytes - mem_used_bytes);

  rsp->mutable_shard_mem_infos()->Reserve(FLAGS_shard_total_num);
  for (uint32_t s = 0; s < FLAGS_shard_total_num; s++) {
    size_t shard_used_bytes = shard_used_bytes_[s].load(std::memory_order_relaxed);
    auto info = rsp->add_shard_mem_infos();
    info->set_shard_id(s);
    info->set_shard_mem_used_bytes(shard_used_bytes);
  }
}

error_code_t KVRpcService::RegisterAdminHandlers(simm::common::AdminServer* admin_server) {
  if (admin_server == nullptr || !admin_server->isRunning()) {
    MLOG_ERROR("RegisterAdminHandlers: AdminServer is null or not running");
    return CommonErr::InvalidState;
  }

  admin_server->registerHandler(
      simm::common::AdminMsgType::DS_STATUS,
      [this](const std::string& /* payload */) -> std::string {
        proto::common::AdmDsStatusResponsePB resp;
        resp.set_ret_code(CommonErr::OK);
        resp.set_is_registered(is_registered_.load());
        resp.set_cm_ready(cm_ready_.load());
        resp.set_heartbeat_failure_count(heartbeat_failure_count_.load());
        std::string buf;
        resp.SerializeToString(&buf);
        return buf;
      });

  MLOG_INFO("DS admin handlers registered");
  return CommonErr::OK;
}

}  // namespace ds
}  // namespace simm
