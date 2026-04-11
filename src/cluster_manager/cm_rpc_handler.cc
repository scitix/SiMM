#include <chrono>
#include <memory>
#include <string>
#include <unordered_map>

#include <folly/Likely.h>
#include <gflags/gflags.h>

#include "cm_rpc_handler.h"
#include "common/base/common_types.h"
#include "common/errcode/errcode_def.h"
#include "common/logging/logging.h"
#include "common/metrics/metrics.h"
#include "common/utils/ip_util.h"
#include "proto/cm_clnt_rpcs.pb.h"
#include "proto/ds_cm_rpcs.pb.h"

DECLARE_uint32(shard_total_num);
DECLARE_bool(cm_deferred_reshard_enabled);

DECLARE_LOG_MODULE("cluster_manager");

// SiCL-Transport API ConnectionImpl::SendResponse will use resp object in sync mode, so it's safe
// to pass the reference of object to it without increase the shared_ptr reference count
#define SEND_RESPONSE(ctx, resp)                                                                                 \
  conn->SendResponse(*resp, ctx, [](std::shared_ptr<sicl::rpc::RpcContext> ctx) {                                \
    if (ctx->ErrorCode() != sicl::transport::SICL_SUCCESS) {                                                     \
      MLOG_ERROR(                                                                                                \
          "Failed to send response by connection, err_msg:{}, err_code:{}", ctx->ErrorText(), ctx->ErrorCode()); \
    } else {                                                                                                     \
      MLOG_DEBUG("Response sent successfully by connection");                                                    \
    }                                                                                                            \
  });

namespace simm {
namespace cm {

static inline void FillNodeInfoHelper(const std::string &addr_str,
                                      NodeStatus status,
                                      const std::shared_ptr<simm::common::NodeResource> &resource,
                                      NodeInfoPB *node_info) {
  auto node_addr = simm::common::NodeAddress::ParseFromString(addr_str);
  if (!node_addr) {
    return;
  }

  auto *addr_pb = node_info->mutable_node_address();
  addr_pb->set_ip(node_addr->node_ip_);
  addr_pb->set_port(node_addr->node_port_);
  node_info->set_node_status(static_cast<int32_t>(status));

  if (resource) {
    auto *res_pb = node_info->mutable_resource();
    res_pb->set_mem_free_bytes(resource->mem_free_bytes_);
    res_pb->set_mem_total_bytes(resource->mem_total_bytes_);
    res_pb->set_mem_used_bytes(resource->mem_used_bytes_);
    res_pb->set_mem_allocated_bytes(resource->mem_allocated_bytes_);
    res_pb->set_last_report_timestamp_us(resource->last_report_timestamp_us_);
  }
}

template <typename T>
static inline void FillRespWithRoutingTableEntryHelper(const simm::cm::QueryResultMap &resmap, T *respb) {
  if (resmap.empty()) {
    MLOG_WARN("Query result map is empty, no routing table entries to fill in response");
    return;
  }

  using pbMsgMapType = std::unordered_map<std::shared_ptr<simm::common::NodeAddress>, std::vector<shard_id_t>>;
  pbMsgMapType pb_msg_map;
  for (const auto &entry : resmap) {
    if (!entry.second) {
      continue;
    }
    pb_msg_map[entry.second].push_back(entry.first);
  }
  for (const auto &map_entry : pb_msg_map) {
    auto *resp_entry = respb->add_shard_info();
    auto *resp_entry_ds_info = resp_entry->mutable_data_server_address();
    resp_entry_ds_info->set_ip(map_entry.first->node_ip_);
    resp_entry_ds_info->set_port(map_entry.first->node_port_);
    for (const auto &shard_id : map_entry.second) {
      resp_entry->add_shard_ids(shard_id);
    }
  }
}

static inline bool HasIncompleteRoutingEntries(const simm::cm::QueryResultMap &resmap) {
  for (const auto &[shard_id, node_addr] : resmap) {
    if (!node_addr) {
      MLOG_WARN("Shard {} has no assigned dataserver in routing table", shard_id);
      return true;
    }
  }
  return false;
}

void NewNodeHandshakeHandler::Work(const std::shared_ptr<sicl::rpc::RpcContext> ctx,
                                   const std::shared_ptr<sicl::rpc::Connection> conn,
                                   [[maybe_unused]] const google::protobuf::Message *request) const {
  auto req_begin_ts = std::chrono::steady_clock::now();
  auto req = dynamic_cast<const NewNodeHandShakeRequestPB *>(request);
  auto resp = std::make_shared<NewNodeHandShakeResponsePB>();
  simm::common::NodeAddress node_addr = {req->node().ip(), req->node().port()};
  std::string addr_str = node_addr.toString();
  const std::string &logical_id = req->logical_node_id();
  error_code_t ret = CommonErr::OK;

  if (!logical_id.empty() && simm::common::ModuleServiceState::GetInstance().GracePeriodFinished()) {
    // Post-grace-period with logical_node_id: process handshake via node manager
    std::vector<shard_id_t> reported_shards(req->shard_ids().begin(), req->shard_ids().end());
    auto result = node_manager_->ProcessHandshake(logical_id, addr_str, reported_shards);

    switch (result.action) {
      case HandshakeResult::Action::DEFERRED_RESHARD_REPLACE: {
        // Replacement scenario: collect shards from old IP, assign to new IP
        auto shards = shard_manager_->GetShardsOwnedByNode(result.old_ip_port);
        shard_manager_->BatchAssignRoutingTable(shards, std::make_shared<simm::common::NodeAddress>(node_addr));
        hb_monitor_->OnDeferredReshardResolved(logical_id);
        result.shards_to_assign = std::move(shards);
        MLOG_INFO("Node handshake replace complete: {} -> {} for {} shards",
                  result.old_ip_port,
                  addr_str,
                  result.shards_to_assign.size());
        break;
      }
      case HandshakeResult::Action::IP_UPDATE: {
        // IP changed while RUNNING: update routing table entries to new IP
        auto shards = shard_manager_->GetShardsOwnedByNode(result.old_ip_port);
        shard_manager_->BatchAssignRoutingTable(shards, std::make_shared<simm::common::NodeAddress>(node_addr));
        hb_monitor_->OnDeferredReshardResolved(logical_id);
        result.shards_to_assign = std::move(shards);
        MLOG_INFO("Node handshake IP update: {} -> {} for {} shards",
                  result.old_ip_port,
                  addr_str,
                  result.shards_to_assign.size());
        break;
      }
      case HandshakeResult::Action::NEW_NODE: {
        // Post-grace-period scale-out: a brand-new logical_node_id not seen before.
        // Shard assignment for scale-out is not yet supported; no shards assigned here.
        // TODO: implement scale-out shard assignment
        MLOG_WARN("New node registered post-grace-period (scale-out not yet supported): logical_id={} ip={}",
                  logical_id,
                  addr_str);
        break;
      }
      default:
        ret = CommonErr::CmRegisterNewNodeFailed;
        break;
    }

    // Fill assigned_shard_ids in response
    for (auto sid : result.shards_to_assign) {
      resp->add_assigned_shard_ids(sid);
    }

  } else if (simm::common::ModuleServiceState::GetInstance().GracePeriodFinished()) {
    MLOG_INFO("Grace period is already finished, new dataserver({}) will be waited for joining the cluster", addr_str);
    // Post-grace-period without logical_node_id: legacy behavior
  } else {
    // Still in grace period: register node and assign shards
    MLOG_INFO("Still in Grace period new dataserver({}) will be added into cluster", addr_str);
    std::vector<shard_id_t> reported_shards(req->shard_ids().begin(), req->shard_ids().end());
    shard_manager_->BatchAssignRoutingTable(reported_shards, std::make_shared<simm::common::NodeAddress>(node_addr));

    if (!logical_id.empty()) {
      // ProcessHandshake (Case 1) calls AddNode internally, so no separate AddNode needed.
      node_manager_->ProcessHandshake(logical_id, addr_str, reported_shards);
    } else {
      // No logical_node_id: legacy registration path (no deferred reshard support)
      ret = node_manager_->AddNode(addr_str);
    }

    // Fill assigned_shard_ids in response (echo back what was assigned)
    for (auto sid : reported_shards) {
      resp->add_assigned_shard_ids(sid);
    }
  }

  if (ret != CommonErr::OK) {
    MLOG_ERROR("Failed to register new node({}) into cluster, ret:{}", addr_str, ret);
  }
  resp->set_ret_code(ret);
  simm::common::Metrics::Instance("cluster_manager")
      .ObserveRequestDuration("hand_shake",
                              static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(
                                                      std::chrono::steady_clock::now() - req_begin_ts)
                                                      .count()));
  simm::common::Metrics::Instance("cluster_manager").IncRequestsTotal("hand_shake");
  if (ret != CommonErr::OK) {
    simm::common::Metrics::Instance("cluster_manager").IncErrorsTotal("hand_shake");
  }
  SEND_RESPONSE(ctx, resp);
}

void NodeHeartBeatHandler::Work(const std::shared_ptr<sicl::rpc::RpcContext> ctx,
                                const std::shared_ptr<sicl::rpc::Connection> conn,
                                const google::protobuf::Message *request) const {
  auto req_begin_ts = std::chrono::steady_clock::now();
  auto req = dynamic_cast<const DataServerHeartBeatRequestPB *>(request);
  auto resp = std::make_shared<DataServerHeartBeatResponsePB>();
  error_code_t ret = CommonErr::OK;
  std::string addr_str = req->node().ip() + ":" + std::to_string(req->node().port());
  const std::string &logical_id = req->logical_node_id();

  if (simm::utils::IsValidV4IPAddr(req->node().ip()) && simm::utils::IsValidPortNum(req->node().port())) {
    if (!logical_id.empty()) {
      // heartbeat keyed by logical_node_id
      ret = hb_monitor_->OnRecvNodeHeartbeat(logical_id, addr_str);
    } else {
      // Legacy: heartbeat keyed by ip:port
      ret = hb_monitor_->OnRecvNodeHeartbeat(addr_str);
    }
  } else {
    MLOG_WARN("NodeHeartBeatHandler::Work, invalid node address received:{}", addr_str);
    ret = CommonErr::InvalidArgument;
  }

  if (ret != CommonErr::OK) {
    MLOG_ERROR("NodeHeartBeatHandler::Work failed, ret:{}, node:{}", ret, addr_str);
  } else {
    MLOG_DEBUG("NodeHeartBeatHandler::Work succeed, node:{}", addr_str);
  }

  resp->set_ret_code(ret);
  simm::common::Metrics::Instance("cluster_manager")
      .ObserveRequestDuration("heart_beat",
                              static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(
                                                      std::chrono::steady_clock::now() - req_begin_ts)
                                                      .count()));
  simm::common::Metrics::Instance("cluster_manager").IncRequestsTotal("heart_beat");
  if (ret != CommonErr::OK) {
    simm::common::Metrics::Instance("cluster_manager").IncErrorsTotal("heart_beat");
  }
  SEND_RESPONSE(ctx, resp);
}

void DataServerResourceReportHandler::Work([[maybe_unused]] const std::shared_ptr<sicl::rpc::RpcContext> ctx,
                                           [[maybe_unused]] const std::shared_ptr<sicl::rpc::Connection> conn,
                                           [[maybe_unused]] const google::protobuf::Message *request) const {
  // TODO(ytji): remove later
}

void RoutingTableUpdateHandler::Work([[maybe_unused]] const std::shared_ptr<sicl::rpc::RpcContext> ctx,
                                     [[maybe_unused]] const std::shared_ptr<sicl::rpc::Connection> conn,
                                     [[maybe_unused]] const google::protobuf::Message *request) const {
  // TODO(ytji)
  // still in service start grace period, return error to clients and dataservers
  // if (FOLLY_UNLIKELY(!simm::common::ModuleServiceState::GetInstance().IsServiceReady())) {
  //   MLOG_WARN("Cluster Manager is still in grace period, service is not ready yet");
  //   resp->set_ret_code();
  //   SEND_RESPONSE(ctx, resp);
  //   return;
  // }
}

void RoutingTableQuerySingleHandler::Work(const std::shared_ptr<sicl::rpc::RpcContext> ctx,
                                          const std::shared_ptr<sicl::rpc::Connection> conn,
                                          const google::protobuf::Message *request) const {
  auto req_begin_ts = std::chrono::steady_clock::now();
  auto req = dynamic_cast<const QueryShardRoutingTableSingleRequestPB *>(request);
  auto resp = std::make_shared<QueryShardRoutingTableSingleResponsePB>();
  error_code_t ret = CommonErr::OK;

  // still in service start grace period, return error to clients and dataservers
  if (FOLLY_UNLIKELY(!simm::common::ModuleServiceState::GetInstance().IsServiceReady())) {
    MLOG_WARN("Cluster Manager is still in grace period, service is not ready yet");
    ret = CmErr::InitInGracePeriod;
    resp->set_ret_code(ret);
    simm::common::Metrics::Instance("cluster_manager")
        .ObserveRequestDuration("query_routing_table_single",
                                static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(
                                                        std::chrono::steady_clock::now() - req_begin_ts)
                                                        .count()));
    simm::common::Metrics::Instance("cluster_manager").IncRequestsTotal("query_routing_table_single");
    simm::common::Metrics::Instance("cluster_manager").IncErrorsTotal("query_routing_table_single");
    SEND_RESPONSE(ctx, resp);
    return;
  }

  simm::cm::QueryResultMap query_res = shard_manager_->QueryShardRoutingInfo(static_cast<shard_id_t>(req->shard_id()));
  if (query_res.empty()) {
    MLOG_WARN("Target shard id({}) not found in global routing table", req->shard_id());
    ret = CommonErr::CmTargetShardIdNotFound;
  } else if (HasIncompleteRoutingEntries(query_res)) {
    ret = CommonErr::CmRoutingInfoNotComplete;
  } else {
    FillRespWithRoutingTableEntryHelper(query_res, resp.get());
  }

  resp->set_ret_code(ret);
  simm::common::Metrics::Instance("cluster_manager")
      .ObserveRequestDuration("query_routing_table_single",
                              static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(
                                                      std::chrono::steady_clock::now() - req_begin_ts)
                                                      .count()));
  simm::common::Metrics::Instance("cluster_manager").IncRequestsTotal("query_routing_table_single");
  if (ret != CommonErr::OK) {
    simm::common::Metrics::Instance("cluster_manager").IncErrorsTotal("query_routing_table_single");
  }
  SEND_RESPONSE(ctx, resp);
}

void RoutingTableQueryBatchHandler::Work(const std::shared_ptr<sicl::rpc::RpcContext> ctx,
                                         const std::shared_ptr<sicl::rpc::Connection> conn,
                                         const google::protobuf::Message *request) const {
  auto req_begin_ts = std::chrono::steady_clock::now();
  auto req = dynamic_cast<const QueryShardRoutingTableBatchRequestPB *>(request);
  auto resp = std::make_shared<QueryShardRoutingTableBatchResponsePB>();

  if (FOLLY_UNLIKELY(!simm::common::ModuleServiceState::GetInstance().IsServiceReady())) {
    MLOG_WARN("Cluster Manager is still in grace period, service is not ready yet");
    resp->set_ret_code(CmErr::InitInGracePeriod);
    simm::common::Metrics::Instance("cluster_manager")
        .ObserveRequestDuration("query_routing_table_batch",
                                static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(
                                                        std::chrono::steady_clock::now() - req_begin_ts)
                                                        .count()));
    simm::common::Metrics::Instance("cluster_manager").IncRequestsTotal("query_routing_table_batch");
    simm::common::Metrics::Instance("cluster_manager").IncErrorsTotal("query_routing_table_batch");
    SEND_RESPONSE(ctx, resp);
    return;
  }

  std::vector<shard_id_t> target_shards(req->shard_ids().begin(), req->shard_ids().end());
  simm::cm::QueryResultMap query_res = shard_manager_->BatchQueryShardRoutingInfos(target_shards);
  if (query_res.empty()) {
    MLOG_WARN("None of target shard ids found in global routing table, target_shards: {}", target_shards.size());
    resp->set_ret_code(CommonErr::CmTargetShardIdNotFound);
    simm::common::Metrics::Instance("cluster_manager").IncErrorsTotal("query_routing_table_batch");
  } else if (HasIncompleteRoutingEntries(query_res)) {
    resp->set_ret_code(CommonErr::CmRoutingInfoNotComplete);
    simm::common::Metrics::Instance("cluster_manager").IncErrorsTotal("query_routing_table_batch");
  } else if (query_res.size() != target_shards.size()) {
    MLOG_WARN("Some target shard ids not found in routing table, target_shards: {}, found_shards: {}",
              target_shards.size(),
              query_res.size());
    // TODO(ytji): what error code should we return here?
    resp->set_ret_code(CommonErr::OK);
  } else {
    resp->set_ret_code(CommonErr::OK);
  }

  FillRespWithRoutingTableEntryHelper(query_res, resp.get());

  simm::common::Metrics::Instance("cluster_manager")
      .ObserveRequestDuration("query_routing_table_batch",
                              static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(
                                                      std::chrono::steady_clock::now() - req_begin_ts)
                                                      .count()));
  simm::common::Metrics::Instance("cluster_manager").IncRequestsTotal("query_routing_table_batch");
  SEND_RESPONSE(ctx, resp);
}

void RoutingTableQueryAllHandler::Work(const std::shared_ptr<sicl::rpc::RpcContext> ctx,
                                       const std::shared_ptr<sicl::rpc::Connection> conn,
                                       [[maybe_unused]] const google::protobuf::Message *request) const {
  auto req_begin_ts = std::chrono::steady_clock::now();
  // auto req = dynamic_cast<QueryShardRoutingTableAllRequestPB*>(request);
  auto resp = std::make_shared<QueryShardRoutingTableAllResponsePB>();

  if (FOLLY_UNLIKELY(!simm::common::ModuleServiceState::GetInstance().IsServiceReady())) {
    MLOG_WARN("Cluster Manager is still in grace period, service is not ready yet");
    resp->set_ret_code(CmErr::InitInGracePeriod);
    simm::common::Metrics::Instance("cluster_manager")
        .ObserveRequestDuration("query_routing_table_all",
                                static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(
                                                        std::chrono::steady_clock::now() - req_begin_ts)
                                                        .count()));
    simm::common::Metrics::Instance("cluster_manager").IncRequestsTotal("query_routing_table_all");
    simm::common::Metrics::Instance("cluster_manager").IncErrorsTotal("query_routing_table_all");
    SEND_RESPONSE(ctx, resp);
    return;
    ;
  }

  simm::cm::QueryResultMap query_res = shard_manager_->QueryAllShardRoutingInfos();
  if (query_res.size() != FLAGS_shard_total_num) {
    MLOG_WARN("All routing table info query result is not complete, target_shards: {}, found_shards: {}",
              FLAGS_shard_total_num,
              query_res.size());
    resp->set_ret_code(CommonErr::CmRoutingInfoNotComplete);
    simm::common::Metrics::Instance("cluster_manager").IncErrorsTotal("query_routing_table_all");
  } else if (HasIncompleteRoutingEntries(query_res)) {
    resp->set_ret_code(CommonErr::CmRoutingInfoNotComplete);
    simm::common::Metrics::Instance("cluster_manager").IncErrorsTotal("query_routing_table_all");
  } else {
    resp->set_ret_code(CommonErr::OK);
  }

  FillRespWithRoutingTableEntryHelper(query_res, resp.get());

  simm::common::Metrics::Instance("cluster_manager")
      .ObserveRequestDuration("query_routing_table_all",
                              static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(
                                                      std::chrono::steady_clock::now() - req_begin_ts)
                                                      .count()));
  simm::common::Metrics::Instance("cluster_manager").IncRequestsTotal("query_routing_table_all");
  SEND_RESPONSE(ctx, resp);
}

void ListNodesHandler::Work(const std::shared_ptr<sicl::rpc::RpcContext> ctx,
                            const std::shared_ptr<sicl::rpc::Connection> conn,
                            [[maybe_unused]] const google::protobuf::Message *request) const {
  auto req_begin_ts = std::chrono::steady_clock::now();
  auto resp = std::make_shared<ListNodesResponsePB>();

  if (FOLLY_UNLIKELY(!simm::common::ModuleServiceState::GetInstance().IsServiceReady())) {
    MLOG_WARN("Cluster Manager is still in grace period, service is not ready yet");
    resp->set_ret_code(CmErr::InitInGracePeriod);
    simm::common::Metrics::Instance("cluster_manager")
        .ObserveRequestDuration("list_nodes",
                                static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(
                                                        std::chrono::steady_clock::now() - req_begin_ts)
                                                        .count()));
    simm::common::Metrics::Instance("cluster_manager").IncRequestsTotal("list_nodes");
    simm::common::Metrics::Instance("cluster_manager").IncErrorsTotal("list_nodes");
    SEND_RESPONSE(ctx, resp);
    return;
  }

  const auto node_stat_list = node_manager_->GetAllNodeStatus();
  const auto node_res_list = node_manager_->GetAllNodeResource();

  // Build address string to resource map for quick lookup
  std::unordered_map<std::string, std::shared_ptr<simm::common::NodeResource>> res_map;
  for (const auto &[addr_str, resource] : node_res_list) {
    res_map[addr_str] = resource;
  }

  // Build response with node info (address, status, resource)
  for (const auto &[addr_str, status] : node_stat_list) {
    auto *node_info = resp->add_nodes();
    auto res_it = res_map.find(addr_str);
    FillNodeInfoHelper(addr_str, status, res_it != res_map.end() ? res_it->second : nullptr, node_info);
  }

  resp->set_ret_code(CommonErr::OK);
  simm::common::Metrics::Instance("cluster_manager")
      .ObserveRequestDuration("list_nodes",
                              static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(
                                                      std::chrono::steady_clock::now() - req_begin_ts)
                                                      .count()));
  simm::common::Metrics::Instance("cluster_manager").IncRequestsTotal("list_nodes");
  SEND_RESPONSE(ctx, resp);
}

void GetNodeResourceHandler::Work(const std::shared_ptr<sicl::rpc::RpcContext> ctx,
                                  const std::shared_ptr<sicl::rpc::Connection> conn,
                                  const google::protobuf::Message *request) const {
  auto req_begin_ts = std::chrono::steady_clock::now();
  auto req = dynamic_cast<const GetNodeResourceRequestPB *>(request);
  auto resp = std::make_shared<GetNodeResourceResponsePB>();
  const std::string addr_str = req->node().ip() + ":" + std::to_string(req->node().port());

  if (FOLLY_UNLIKELY(!simm::common::ModuleServiceState::GetInstance().IsServiceReady())) {
    resp->set_ret_code(CmErr::InitInGracePeriod);
    simm::common::Metrics::Instance("cluster_manager").IncErrorsTotal("get_node_resource");
    simm::common::Metrics::Instance("cluster_manager")
        .ObserveRequestDuration("get_node_resource",
                                static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(
                                                        std::chrono::steady_clock::now() - req_begin_ts)
                                                        .count()));
    simm::common::Metrics::Instance("cluster_manager").IncRequestsTotal("get_node_resource");
    SEND_RESPONSE(ctx, resp);
    return;
  }

  if (!node_manager_->QueryNodeExists(addr_str)) {
    resp->set_ret_code(CommonErr::TargetNotFound);
    simm::common::Metrics::Instance("cluster_manager").IncErrorsTotal("get_node_resource");
    simm::common::Metrics::Instance("cluster_manager")
        .ObserveRequestDuration("get_node_resource",
                                static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(
                                                        std::chrono::steady_clock::now() - req_begin_ts)
                                                        .count()));
    simm::common::Metrics::Instance("cluster_manager").IncRequestsTotal("get_node_resource");
    SEND_RESPONSE(ctx, resp);
    return;
  }

  auto resource = node_manager_->GetNodeResource(addr_str);
  if (!resource) {
    resource = node_manager_->RefreshNodeResource(addr_str);
  }
  if (!resource) {
    resp->set_ret_code(CommonErr::TargetUnavailable);
    simm::common::Metrics::Instance("cluster_manager").IncErrorsTotal("get_node_resource");
    simm::common::Metrics::Instance("cluster_manager")
        .ObserveRequestDuration("get_node_resource",
                                static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(
                                                        std::chrono::steady_clock::now() - req_begin_ts)
                                                        .count()));
    simm::common::Metrics::Instance("cluster_manager").IncRequestsTotal("get_node_resource");
    SEND_RESPONSE(ctx, resp);
    return;
  }

  FillNodeInfoHelper(addr_str, node_manager_->QueryNodeStatus(addr_str), resource, resp->mutable_node());
  for (const auto &shard_resource : resource->shard_mem_infos_) {
    auto *shard_pb = resp->add_shard_resources();
    shard_pb->set_shard_id(shard_resource.shard_id_);
    shard_pb->set_mem_used_bytes(shard_resource.mem_used_bytes_);
  }

  resp->set_ret_code(CommonErr::OK);
  simm::common::Metrics::Instance("cluster_manager")
      .ObserveRequestDuration("get_node_resource",
                              static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(
                                                      std::chrono::steady_clock::now() - req_begin_ts)
                                                      .count()));
  simm::common::Metrics::Instance("cluster_manager").IncRequestsTotal("get_node_resource");
  SEND_RESPONSE(ctx, resp);
}

void SetNodeStatusHandler::Work(const std::shared_ptr<sicl::rpc::RpcContext> ctx,
                                const std::shared_ptr<sicl::rpc::Connection> conn,
                                const google::protobuf::Message *request) const {
  auto req_begin_ts = std::chrono::steady_clock::now();
  auto req = dynamic_cast<const SetNodeStatusRequestPB *>(request);
  auto resp = std::make_shared<SetNodeStatusResponsePB>();
  error_code_t ret = CommonErr::OK;

  if (FOLLY_UNLIKELY(!simm::common::ModuleServiceState::GetInstance().IsServiceReady())) {
    MLOG_WARN("Cluster Manager is still in grace period, service is not ready yet");
    resp->set_ret_code(CommonErr::TargetUnavailable);
    simm::common::Metrics::Instance("cluster_manager")
        .ObserveRequestDuration("set_node_status",
                                static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(
                                                        std::chrono::steady_clock::now() - req_begin_ts)
                                                        .count()));
    simm::common::Metrics::Instance("cluster_manager").IncRequestsTotal("set_node_status");
    simm::common::Metrics::Instance("cluster_manager").IncErrorsTotal("set_node_status");
    SEND_RESPONSE(ctx, resp);
    return;
  }

  // Not exist should return error directly, use another method
  std::string addr_str = req->node().ip() + ":" + std::to_string(req->node().port());
  if (!node_manager_->QueryNodeExists(addr_str)) {
    MLOG_ERROR("Target node({}) does not exist in cluster, cannot set status", addr_str);
    ret = CommonErr::TargetNotFound;
    resp->set_ret_code(ret);
    simm::common::Metrics::Instance("cluster_manager")
        .ObserveRequestDuration("set_node_status",
                                static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(
                                                        std::chrono::steady_clock::now() - req_begin_ts)
                                                        .count()));
    simm::common::Metrics::Instance("cluster_manager").IncRequestsTotal("set_node_status");
    simm::common::Metrics::Instance("cluster_manager").IncErrorsTotal("set_node_status");
    SEND_RESPONSE(ctx, resp);
    return;
  }

  // TODO: use sliding window to avoid frequent status update
  ret = node_manager_->UpdateNodeStatus(addr_str, static_cast<NodeStatus>(req->node_status()));
  if (ret != CommonErr::OK) {
    MLOG_ERROR("Failed to set node({}) status to {}, ret:{}", addr_str, req->node_status(), ret);
  } else {
    MLOG_INFO("Set node({}) status to {} succeed", addr_str, req->node_status());
  }

  resp->set_ret_code(ret);
  simm::common::Metrics::Instance("cluster_manager")
      .ObserveRequestDuration("set_node_status",
                              static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(
                                                      std::chrono::steady_clock::now() - req_begin_ts)
                                                      .count()));
  simm::common::Metrics::Instance("cluster_manager").IncRequestsTotal("set_node_status");
  if (ret != CommonErr::OK) {
    simm::common::Metrics::Instance("cluster_manager").IncErrorsTotal("set_node_status");
  }
  SEND_RESPONSE(ctx, resp);
}
}  // namespace cm
}  // namespace simm
