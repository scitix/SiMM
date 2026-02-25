#include <chrono>
#include <memory>
#include <string>
#include <unordered_map>

#include <gflags/gflags.h>
#include <folly/Likely.h>

#include "cm_rpc_handler.h"
#include "common/base/common_types.h"
#include "common/errcode/errcode_def.h"
#include "common/logging/logging.h"
#include "common/metrics/metrics.h"
#include "common/utils/ip_util.h"
#include "proto/cm_clnt_rpcs.pb.h"
#include "proto/ds_cm_rpcs.pb.h"

DECLARE_uint32(shard_total_num);

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

template <typename T>
static inline void FillRespWithRoutingTableEntryHelper(const simm::cm::QueryResultMap &resmap, T *respb) {
  if (resmap.empty()) {
    MLOG_WARN("Query result map is empty, no routing table entries to fill in response");
    return;
  }

  using pbMsgMapType = std::unordered_map<std::shared_ptr<simm::common::NodeAddress>, std::vector<shard_id_t>>;
  pbMsgMapType pb_msg_map;
  for (const auto &entry : resmap) {
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

void NewNodeHandshakeHandler::Work(const std::shared_ptr<sicl::rpc::RpcContext> ctx,
                                   const std::shared_ptr<sicl::rpc::Connection> conn,
                  [[maybe_unused]] const google::protobuf::Message *request) const {
  auto req_begin_ts = std::chrono::steady_clock::now();
  auto req = dynamic_cast<const NewNodeHandShakeRequestPB *>(request);
  auto resp = std::make_shared<NewNodeHandShakeResponsePB>();
  simm::common::NodeAddress node_addr = {req->node().ip(), req->node().port()};
  error_code_t ret = CommonErr::OK;
  if (simm::common::ModuleServiceState::GetInstance().GracePeriodFinished()) {
    MLOG_INFO("Grace period is already finished, new dataserver({}) will be waited for joining the cluster",
                node_addr.toString());
    // already out of grace period, new dataserver nodes will be hold for
    // one timewindow, and be added in batch after current timewindow finishes
  } else {
    // still in grace period, just add nodes
    // FIXME(ytji): needn't to use NodeAddress as intermediary struct
    MLOG_INFO("Still in Grace period new dataserver({}) will be added into cluster", node_addr.toString());
    ret = node_manager_->AddNode(node_addr.toString());
    shard_manager_->BatchAssignRoutingTable(std::vector<shard_id_t>(req->shard_ids().begin(), req->shard_ids().end()), 
        std::make_shared<simm::common::NodeAddress>(node_addr));
  }
  
  if (ret != CommonErr::OK) {
    MLOG_ERROR("Failed to register new node({}) into cluster, ret:{}", node_addr.toString(), ret);
  }
  resp->set_ret_code(ret);
  simm::common::Metrics::Instance("cluster_manager").ObserveRequestDuration("hand_shake", static_cast<double>(
      std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - req_begin_ts).count()));
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

  if (simm::utils::IsValidV4IPAddr(req->node().ip()) && simm::utils::IsValidPortNum(req->node().port())) {
    ret = hb_monitor_->OnRecvNodeHeartbeat(addr_str);
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
  simm::common::Metrics::Instance("cluster_manager").ObserveRequestDuration("heart_beat", static_cast<double>(
      std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - req_begin_ts).count()));
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
    simm::common::Metrics::Instance("cluster_manager").ObserveRequestDuration("query_routing_table_single", static_cast<double>(
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - req_begin_ts).count()));
    simm::common::Metrics::Instance("cluster_manager").IncRequestsTotal("query_routing_table_single");
    simm::common::Metrics::Instance("cluster_manager").IncErrorsTotal("query_routing_table_single");
    SEND_RESPONSE(ctx, resp);
    return;
  }

  simm::cm::QueryResultMap query_res = shard_manager_->QueryShardRoutingInfo(static_cast<shard_id_t>(req->shard_id()));
  if (query_res.empty()) {
    MLOG_WARN("Target shard id({}) not found in global routing table", req->shard_id());
    ret = CommonErr::CmTargetShardIdNotFound;
  } else {
    FillRespWithRoutingTableEntryHelper(query_res, resp.get());
  }

  resp->set_ret_code(ret);
  simm::common::Metrics::Instance("cluster_manager").ObserveRequestDuration("query_routing_table_single", static_cast<double>(
      std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - req_begin_ts).count()));
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
    simm::common::Metrics::Instance("cluster_manager").ObserveRequestDuration("query_routing_table_batch", static_cast<double>(
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - req_begin_ts).count()));
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
  } else if (query_res.size() != target_shards.size()) {
    MLOG_WARN("Some target shard ids not found in routing table, target_shards: {}, found_shards: {}",
              target_shards.size(), query_res.size());
    // TODO(ytji): what error code should we return here?
    resp->set_ret_code(CommonErr::OK);
  } else {
    resp->set_ret_code(CommonErr::OK);
  }

  FillRespWithRoutingTableEntryHelper(query_res, resp.get());

  simm::common::Metrics::Instance("cluster_manager").ObserveRequestDuration("query_routing_table_batch", static_cast<double>(
      std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - req_begin_ts).count()));
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
    simm::common::Metrics::Instance("cluster_manager").ObserveRequestDuration("query_routing_table_all", static_cast<double>(
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - req_begin_ts).count()));
    simm::common::Metrics::Instance("cluster_manager").IncRequestsTotal("query_routing_table_all");
    simm::common::Metrics::Instance("cluster_manager").IncErrorsTotal("query_routing_table_all");
    SEND_RESPONSE(ctx, resp);
    return;;
  }

  simm::cm::QueryResultMap query_res = shard_manager_->QueryAllShardRoutingInfos();
  if (query_res.size() != FLAGS_shard_total_num) {
    MLOG_WARN("All routing table info query result is not complete, target_shards: {}, found_shards: {}",
              FLAGS_shard_total_num, query_res.size());
    resp->set_ret_code(CommonErr::CmRoutingInfoNotComplete);
    simm::common::Metrics::Instance("cluster_manager").IncErrorsTotal("query_routing_table_all");
  } else {
    resp->set_ret_code(CommonErr::OK);
  }

  FillRespWithRoutingTableEntryHelper(query_res, resp.get());

  simm::common::Metrics::Instance("cluster_manager").ObserveRequestDuration("query_routing_table_all", static_cast<double>(
      std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - req_begin_ts).count()));
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
    simm::common::Metrics::Instance("cluster_manager").ObserveRequestDuration("list_nodes", static_cast<double>(
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - req_begin_ts).count()));
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
    // Parse address string (ip:port format)
    auto node_addr = simm::common::NodeAddress::ParseFromString(addr_str);
    if (!node_addr) {
      MLOG_ERROR("Failed to parse node address string({})", addr_str);
      continue;
    }

    auto *node_info = resp->add_nodes();

    // Set node address
    auto *addr_pb = node_info->mutable_node_address();
    addr_pb->set_ip(node_addr->node_ip_);
    addr_pb->set_port(node_addr->node_port_);

    // Set node status
    node_info->set_node_status(static_cast<int32_t>(status));

    // Set node resource if available
    auto res_it = res_map.find(addr_str);
    if (res_it != res_map.end() && res_it->second) {
      auto *res_pb = node_info->mutable_resource();
      res_pb->set_mem_free_bytes(res_it->second->mem_free_bytes_);
      res_pb->set_mem_total_bytes(res_it->second->mem_total_bytes_);
      res_pb->set_mem_used_bytes(res_it->second->mem_used_bytes_);
    }
  }

  resp->set_ret_code(CommonErr::OK);
  simm::common::Metrics::Instance("cluster_manager").ObserveRequestDuration("list_nodes", static_cast<double>(
      std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - req_begin_ts).count()));
  simm::common::Metrics::Instance("cluster_manager").IncRequestsTotal("list_nodes");
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
    simm::common::Metrics::Instance("cluster_manager").ObserveRequestDuration("set_node_status", static_cast<double>(
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - req_begin_ts).count()));
    simm::common::Metrics::Instance("cluster_manager").IncRequestsTotal("set_node_status");
    simm::common::Metrics::Instance("cluster_manager").IncErrorsTotal("set_node_status");
    SEND_RESPONSE(ctx, resp);
    return;
  }

  // Not exist should return error directly, use another method
  std::string addr_str = req->node().ip() + ":" + std::to_string(req->node().port());
  if(!node_manager_->QueryNodeExists(addr_str)) {
    MLOG_ERROR("Target node({}) does not exist in cluster, cannot set status", addr_str);
    ret = CommonErr::TargetNotFound;
    resp->set_ret_code(ret);
    simm::common::Metrics::Instance("cluster_manager").ObserveRequestDuration("set_node_status", static_cast<double>(
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - req_begin_ts).count()));
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
  simm::common::Metrics::Instance("cluster_manager").ObserveRequestDuration("set_node_status", static_cast<double>(
      std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - req_begin_ts).count()));
  simm::common::Metrics::Instance("cluster_manager").IncRequestsTotal("set_node_status");
  if (ret != CommonErr::OK) {
    simm::common::Metrics::Instance("cluster_manager").IncErrorsTotal("set_node_status");
  }
  SEND_RESPONSE(ctx, resp);
}
}  // namespace cm
}  // namespace simm
