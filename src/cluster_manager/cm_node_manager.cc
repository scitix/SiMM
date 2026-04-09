#include <chrono>
#include <functional>
#include <optional>
#include <pthread.h>

#include <gflags/gflags.h>
#include <google/protobuf/message.h>

#include <folly/concurrency/ConcurrentHashMap.h>

#include "proto/ds_cm_rpcs.pb.h"
#include "rpc/connection.h"
#include "rpc/rpc.h"

#include "common/logging/logging.h"
#include "common/utils/time_util.h"
#include "cm_node_manager.h"

DECLARE_string(dataserver_namespace);
DECLARE_string(dataserver_svc_name);
DECLARE_string(dataserver_port_name);
DECLARE_uint32(rpc_timeout_inSecs);
DECLARE_uint32(dataserver_resource_interval_inSecs);
DECLARE_bool(cm_deferred_reshard_enabled);
DECLARE_uint32(cm_deferred_reshard_window_inSecs);

DECLARE_LOG_MODULE("cluster_manager");

namespace simm {
namespace cm {

ClusterManagerNodeManager::ClusterManagerNodeManager() {
  sicl::rpc::SiRPC::newInstance(rpc_client_, false);
  node_info_map_ = folly::ConcurrentHashMap<std::string, std::shared_ptr<simm::common::NodeResource>>{};
  node_status_map_ = folly::ConcurrentHashMap<std::string, NodeStatus>{};
}

ClusterManagerNodeManager::~ClusterManagerNodeManager() {
  MLOG_INFO("Start destruct Node Manager");
  if (rpc_client_ != nullptr) {
    delete rpc_client_;
    rpc_client_ = nullptr;
  }
}

void ClusterManagerNodeManager::Init() {
  start_timestamp_us_ = simm::utils::current_microseconds();
  resource_thread_stop_.store(false);

  // start get resource thread
  auto self = shared_from_this();
  std::function<void()> resource_loop = [self]() {
    while (!self->resource_thread_stop_.load()) {
      self->updateAllNodeResource();
      self->resource_thread_baton_.timed_wait(std::chrono::milliseconds(FLAGS_dataserver_resource_interval_inSecs * 1000));
      self->resource_thread_baton_.reset();
    }
  };
  resource_thread_ = new std::thread(resource_loop);
  pthread_setname_np(resource_thread_->native_handle(), "query_ds_resource");
  MLOG_INFO("Cluster Manager Node Manager init succeed");
}

void ClusterManagerNodeManager::Stop() {
  resource_thread_stop_.store(true);
  resource_thread_baton_.post();
  if (resource_thread_ != nullptr && resource_thread_->joinable()) {
    resource_thread_->join();
  }
  MLOG_INFO("Delete resource query thread in Node Manager succeed");
}

std::vector<std::shared_ptr<simm::common::NodeAddress>> ClusterManagerNodeManager::GetAllNodeAddress(bool alive) {
  std::vector<std::shared_ptr<simm::common::NodeAddress>> addrs = {};
  for (auto &pair : node_status_map_) {
    if (alive && (pair.second != NodeStatus::RUNNING)) {
      continue;
    }
    auto ds_addr = simm::common::NodeAddress::ParseFromString(pair.first);
    if (!ds_addr) {
      MLOG_ERROR("Null results return when parse ds address string({})", pair.first);
    } else {
      // FIXME(ytji): ugly, need optimized
      addrs.push_back(std::make_shared<simm::common::NodeAddress>(ds_addr->node_ip_, ds_addr->node_port_));
    }
  }
  return addrs;
}

error_code_t ClusterManagerNodeManager::AddNode(const std::string & addr_str) {
  auto result = node_status_map_.insert_or_assign(addr_str, NodeStatus::RUNNING);
  if (!result.second) {
    MLOG_ERROR("Add node {} in Node Manager status map failed", addr_str);
    return CmErr::NodeManagerAddNodeFailed;
  }

  // FIXME(ytji): for test, just comment below codes
  // // get node resource info, only print log if error
  // auto resource_ret = getNodeResource(addr_str);
  // if (resource_ret == nullptr) {
  //   MLOG_ERROR("Get node {} resource info failed", addr_str);
  //   return CommonErr::OK;
  // }

  // node_info_map_.insert_or_assign(addr_str, resource_ret);
  // MLOG_DEBUG("Add node {} in Node Manager succeed", addr_str);
  return CommonErr::OK;
}

error_code_t ClusterManagerNodeManager::DelNode(const std::string & addr_str) {
  node_info_map_.erase(addr_str);
  node_status_map_.erase(addr_str);
  MLOG_DEBUG("Delete node {} in Node Manager succeed", addr_str);
  return CommonErr::OK;
}

error_code_t ClusterManagerNodeManager::UpdateNodeStatus(const std::string & addr_str, NodeStatus status) {
  node_status_map_.insert_or_assign(addr_str, status);
  MLOG_DEBUG("Update node {} status in Node Manager succeed", addr_str);
  return CommonErr::OK;
}

std::shared_ptr<simm::common::NodeResource> ClusterManagerNodeManager::getNodeResource(
    const std::string & addr_str) {
  DataServerResourceRequestPB req;
  auto resp = new DataServerResourceResponsePB;
  sicl::rpc::RpcContext *ctx_p;
  sicl::rpc::RpcContext::newInstance(ctx_p);
  std::shared_ptr<sicl::rpc::RpcContext> ctx = std::shared_ptr<sicl::rpc::RpcContext>(ctx_p);

  auto ds_addr = simm::common::NodeAddress::ParseFromString(addr_str);
  if (!ds_addr) {
    MLOG_ERROR("Null results return when parse ds address string({})", addr_str);
    return nullptr;
  }

  rpc_client_->SendRequest(ds_addr->node_ip_, ds_addr->node_port_, static_cast<sicl::rpc::ReqType>(0), req, resp, ctx, nullptr);
  if (ctx->Failed()) {
    std::string errmsg = ctx->ErrorText();
    MLOG_ERROR("Get node {} resource failed, err:{}", addr_str, errmsg);
    return nullptr;
  }

  auto resource = std::make_shared<simm::common::NodeResource>(static_cast<int64_t>(resp->mem_total_bytes()),
                                                               static_cast<int64_t>(resp->mem_free_bytes()),
                                                               static_cast<int64_t>(resp->mem_used_bytes()));

  MLOG_DEBUG("Get node {} resource info in Node Manager succeed", addr_str);
  return resource;
}

void ClusterManagerNodeManager::updateAllNodeResource() {
  for (auto &[addr, status] : node_status_map_) {
    if (status != NodeStatus::RUNNING) {
      continue;
    }
    auto resource_ret = getNodeResource(addr);
    if (resource_ret == nullptr) {
      continue;
    }
    node_info_map_.insert_or_assign(addr, resource_ret);
  }

  MLOG_DEBUG("Update all nodes resource info in Node Manager succeed");
}

bool ClusterManagerNodeManager::QueryNodeExists(const std::string & addr_str) {
  auto it = node_status_map_.find(addr_str);
  if (it == node_status_map_.end()) {
    MLOG_DEBUG("Node addr({}) not found in NodeManager", addr_str);
    return false;
  }
  return true;
}

NodeStatus ClusterManagerNodeManager::QueryNodeStatus(const std::string & addr_str) {
  auto it = node_status_map_.find(addr_str);
  if (it == node_status_map_.end()) {
    MLOG_DEBUG("Node addr({}) not found in NodeManager", addr_str);
    return NodeStatus::DEAD;
  }
  return it->second;
}

std::shared_ptr<simm::common::NodeResource> ClusterManagerNodeManager::GetNodeResource(const std::string & addr_str) {
  auto it = node_info_map_.find(addr_str);
  if (it == node_info_map_.end()) {
    MLOG_DEBUG("Node addr({}) not found in NodeManager", addr_str);
    return nullptr;
  }
  return it->second;
}

std::unordered_map<std::string, NodeStatus> ClusterManagerNodeManager::GetAllNodeStatus() {
  std::unordered_map<std::string, NodeStatus> status_map;
  for (auto &pair : node_status_map_) {
    status_map[pair.first] = pair.second;
  }
  return status_map;
}

std::unordered_map<std::string, std::shared_ptr<simm::common::NodeResource>>
                                              ClusterManagerNodeManager::GetAllNodeResource() {
  std::unordered_map<std::string, std::shared_ptr<simm::common::NodeResource>> resource_map;
  for (auto &pair : node_info_map_) {
    resource_map[pair.first] = pair.second;
  }
  return resource_map;
}

// logical_node_id based methods

void ClusterManagerNodeManager::migrateNodeIp(const std::string& logical_id,
                                               NodeEntry& entry,
                                               const std::string& new_ip_port) {
  addr_to_logical_.erase(entry.current_ip_port);
  addr_to_logical_.insert_or_assign(new_ip_port, logical_id);
  node_status_map_.erase(entry.current_ip_port);
  node_info_map_.erase(entry.current_ip_port);
  AddNode(new_ip_port);
  entry.current_ip_port = new_ip_port;
}

HandshakeResult ClusterManagerNodeManager::ProcessHandshake(
    const std::string& logical_id,
    const std::string& new_ip_port,
    const std::vector<shard_id_t>& reported_shards) {

  HandshakeResult result;

  auto it = logical_node_table_.find(logical_id);
  if (it == logical_node_table_.end()) {
    // Case 1: logical_id not seen before → new node registration
    NodeEntry entry;
    entry.logical_node_id = logical_id;
    entry.current_ip_port = new_ip_port;
    entry.status = NodeStatus::RUNNING;
    logical_node_table_.insert_or_assign(logical_id, entry);
    addr_to_logical_.insert_or_assign(new_ip_port, logical_id);

    // Also register in legacy ip:port maps
    AddNode(new_ip_port);

    result.action = HandshakeResult::Action::NEW_NODE;
    result.shards_to_assign = reported_shards;
    MLOG_INFO("New node registered: logical_id={} ip={}", logical_id, new_ip_port);
    return result;
  }

  // ConcurrentHashMap iterators yield const refs — copy, mutate, assign back
  NodeEntry entry = it->second;

  switch (entry.status) {
    case NodeStatus::DEFERRED_RESHARD: {
      // Case 2: replacement DS registered while waiting — in-place IP update
      result.action = HandshakeResult::Action::DEFERRED_RESHARD_REPLACE;
      result.old_ip_port = entry.current_ip_port;
      migrateNodeIp(logical_id, entry, new_ip_port);
      entry.status = NodeStatus::RUNNING;
      entry.deferred_reshard_since = {};
      logical_node_table_.assign(logical_id, entry);
      MLOG_INFO("Node replacement: logical_id={} old_ip={} new_ip={}",
                logical_id, result.old_ip_port, new_ip_port);
      return result;
    }

    case NodeStatus::RUNNING: {
      result.action = HandshakeResult::Action::IP_UPDATE;
      result.old_ip_port = entry.current_ip_port;
      if (entry.current_ip_port == new_ip_port) {
        // Case 3: DS restarted before HB timeout, same IP. Treat as IP_UPDATE so the
        // handler looks up shards via GetShardsOwnedByNode and returns them to the DS.
        // BatchAssignRoutingTable with same addr is a no-op on the routing table.
        MLOG_INFO("Node re-registration (fast restart): logical_id={} ip={}", logical_id, new_ip_port);
      } else {
        // Case 4: IP changed while still RUNNING (rare IP drift)
        migrateNodeIp(logical_id, entry, new_ip_port);
        logical_node_table_.assign(logical_id, entry);
        MLOG_INFO("Node IP update: logical_id={} old_ip={} new_ip={}",
                  logical_id, result.old_ip_port, new_ip_port);
      }
      return result;
    }

    case NodeStatus::DEAD:
    default: {
      // Case 5: node was DEAD (reshard already happened), treat as new
      migrateNodeIp(logical_id, entry, new_ip_port);
      entry.status = NodeStatus::RUNNING;
      entry.deferred_reshard_since = {};
      logical_node_table_.assign(logical_id, entry);
      result.action = HandshakeResult::Action::NEW_NODE;
      result.shards_to_assign = reported_shards;
      MLOG_INFO("Node rejoin after DEAD: logical_id={} ip={}", logical_id, new_ip_port);
      return result;
    }
  }
}

std::optional<NodeEntry> ClusterManagerNodeManager::GetNodeEntry(const std::string& logical_id) const {
  auto it = logical_node_table_.find(logical_id);
  if (it == logical_node_table_.end()) {
    return std::nullopt;
  }
  return it->second;
}

error_code_t ClusterManagerNodeManager::OnHeartbeat(const std::string& logical_id,
                                                     const std::string& ip_port) {
  auto it = logical_node_table_.find(logical_id);
  if (it != logical_node_table_.end()) {
    auto entry = it->second;
    if (entry.current_ip_port != ip_port) {
      migrateNodeIp(logical_id, entry, ip_port);
      logical_node_table_.assign(logical_id, entry);
    }
  }
  return CommonErr::OK;
}

error_code_t ClusterManagerNodeManager::SetNodeStatus(
    const std::string& logical_id,
    NodeStatus status,
    std::chrono::steady_clock::time_point ts,
    std::optional<NodeStatus> expected_status) {

  auto it = logical_node_table_.find(logical_id);
  if (it == logical_node_table_.end()) {
    MLOG_WARN("SetNodeStatus: logical_id={} not found", logical_id);
    return CommonErr::TargetNotFound;
  }

  auto entry = it->second;

  // Compare-and-set: if caller specified an expected status, abort if it no
  // longer matches (e.g. ProcessHandshake already moved the node back to RUNNING).
  if (expected_status.has_value() && entry.status != expected_status.value()) {
    MLOG_WARN("SetNodeStatus: logical_id={} status mismatch (expected={} actual={}), skipping",
              logical_id,
              common::NodeStatusToString(expected_status.value()),
              common::NodeStatusToString(entry.status));
    return CommonErr::OK;
  }

  entry.status = status;
  if (status == NodeStatus::DEFERRED_RESHARD) {
    entry.deferred_reshard_since = ts;
  }
  logical_node_table_.assign(logical_id, entry);

  // Keep legacy map in sync
  UpdateNodeStatus(entry.current_ip_port, status);

  MLOG_INFO("SetNodeStatus: logical_id={} ip={} status={}",
            logical_id, entry.current_ip_port, common::NodeStatusToString(status));
  return CommonErr::OK;
}

std::string ClusterManagerNodeManager::ResolveLogicalId(const std::string& ip_port) const {
  auto it = addr_to_logical_.find(ip_port);
  if (it == addr_to_logical_.end()) {
    return "";
  }
  return it->second;
}

}  // namespace cm
}  // namespace simm
