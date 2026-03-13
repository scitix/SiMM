#include <gflags/gflags.h>
#include <memory>
#include <ranges>

#include "cm_shard_manager.h"
#include "common/base/assert.h"
#include "common/logging/logging.h"
#include "folly/Likely.h"
#include <boost/algorithm/string/join.hpp>

DECLARE_LOG_MODULE("cluster_manager");

DECLARE_uint32(shard_total_num);
DECLARE_uint32(dataserver_min_num);
DECLARE_uint32(dataserver_max_num);

namespace simm {
namespace cm {

ClusterManagerShardManager::ClusterManagerShardManager() : mShardNum(FLAGS_shard_total_num) {
  for (uint32_t i = 0; i < mShardNum; ++i) {
    mShardRoutingTable.insert(i, nullptr);
  }
}

ClusterManagerShardManager::~ClusterManagerShardManager() {
  MLOG_DEBUG("{}: dtor func called", __func__);
}

QueryResultMap ClusterManagerShardManager::QueryShardRoutingInfo(shard_id_t target_shard) {
  std::unordered_map<shard_id_t, std::shared_ptr<simm::common::NodeAddress>> result;
  auto it = mShardRoutingTable.find(target_shard);
  if (it != mShardRoutingTable.end()) {
    result[target_shard] = it->second;
  } else {
    // TODO(ytji): just return empty result of return more error codes?
    MLOG_WARN("{}: target shard id({}) not found in global routing table", __func__, target_shard);
  }
  return result;
}

QueryResultMap ClusterManagerShardManager::BatchQueryShardRoutingInfos(const std::vector<shard_id_t> &target_shards) {
  std::unordered_map<shard_id_t, std::shared_ptr<simm::common::NodeAddress>> result;
  for (const auto &shard : target_shards) {
    auto it = mShardRoutingTable.find(shard);
    if (it != mShardRoutingTable.end()) {
      result[shard] = it->second;
    } else {
      // TODO(ytji): just return partial result of return more error codes?
      MLOG_WARN("{}: target shard id({}) not found in global routing table", __func__, shard);
    }
  }
  return result;
}

QueryResultMap ClusterManagerShardManager::QueryAllShardRoutingInfos() {
  // TODO(ytji): maybe we shouldn't convert it to unordered_map with loop
  //             just copy it as folly::ConcurrentHashMap and covert it
  //             to unordered_map before respond the RPC request
  std::unordered_map<shard_id_t, std::shared_ptr<simm::common::NodeAddress>> result;
  for (const auto &entry : mShardRoutingTable) {
    result[entry.first] = entry.second;
  }
  return result;
}

error_code_t ClusterManagerShardManager::ModifyRoutingTable(shard_id_t target_shard,
                                                            std::shared_ptr<simm::common::NodeAddress> target_server) {
  // check shard id validity
  if (target_shard < mShardNum) {
    mShardRoutingTable.assign(target_shard, target_server);
  } else {
    MLOG_ERROR("{}: invalid shard id({}) passed in", __func__, target_shard);
    // TODO(ytji): maybe we should use module-level error code?
    return CommonErr::InvalidArgument;
  }
  return CommonErr::OK;
}

error_code_t ClusterManagerShardManager::BatchAssignRoutingTable(
    const std::vector<shard_id_t> &target_shards,
    const std::shared_ptr<simm::common::NodeAddress> target_server) {
  for (size_t i = 0; i < target_shards.size(); ++i) {
    if (target_shards.at(i) >= mShardNum) {
      MLOG_WARN("{}: invalid shard id({}) passed in", __func__, target_shards.at(i));
      continue;  // skip invalid shard id
    }
    auto target_shard = target_shards.at(i);
    mShardRoutingTable.assign(target_shard, target_server);
  }
  return CommonErr::OK;
}

error_code_t ClusterManagerShardManager::BatchModifyRoutingTable(
    const std::vector<shard_id_t> &target_shards,
    const std::vector<std::shared_ptr<simm::common::NodeAddress>> &target_servers) {
  SIMM_ASSERT(target_shards.size() == target_servers.size(), "target shards size not equal to target server size");
  for (size_t i = 0; i < target_shards.size(); ++i) {
    if (target_shards.at(i) >= mShardNum) {
      MLOG_WARN("{}: invalid shard id({}) passed in", __func__, target_shards.at(i));
      continue;  // skip invalid shard id
    }
    auto target_shard = target_shards.at(i);
    auto target_server = target_servers.at(i);
    mShardRoutingTable.assign(target_shard, target_server);
  }
  return CommonErr::OK;
}

error_code_t ClusterManagerShardManager::InitShardRoutingTable(
    const std::vector<std::shared_ptr<simm::common::NodeAddress>> &all_servers) {
  std::size_t server_num = all_servers.size();
  // TODO(ytji): how to check value invalidity in all_servers
  if (server_num == 0 || server_num < FLAGS_dataserver_min_num) {
    MLOG_ERROR(
        "{}: dataserver nodes number({}) is too small, minimum is {}", __func__, server_num, FLAGS_dataserver_min_num);
    return CmErr::InitDataserverNodesTooFew;
  } else if (server_num > FLAGS_dataserver_max_num) {
    MLOG_ERROR(
        "{}: dataserver nodes number({}) is too large, maximum is {}", __func__, server_num, FLAGS_dataserver_max_num);
    return CmErr::InitDataserverNodesTooMany;
  }

  uint32_t server_index = 0;
  for (uint32_t i = 0; i < mShardNum; ++i) {
    if (mShardRoutingTable[i] != nullptr) {
      continue;
    }
    mShardRoutingTable.assign(i, all_servers.at(server_index % server_num));
    ++server_index;
  }

  return CommonErr::OK;
}

error_code_t ClusterManagerShardManager::RebalanceShardsAfterNodeFailure(
    const std::vector<std::string>& dead_node_addresses,
    const std::vector<std::shared_ptr<simm::common::NodeAddress>>& alive_servers) {
  MLOG_DEBUG("Starting shard rebalance after node failure, dead nodes: {}", boost::algorithm::join(dead_node_addresses, ", "));

  if (FOLLY_UNLIKELY(alive_servers.empty())) {
    return CmErr::NoAvailableDataservers;
  }
  
  if (FOLLY_UNLIKELY(alive_servers.size() < FLAGS_dataserver_min_num)) {
    return CmErr::InsufficientDataservers;
  }

  // Find all orphaned shards from dead nodes
  // TODO(zbhe): change data structure for faster lookup / backlink
  std::vector<shard_id_t> orphaned_shards;
  for (const auto& entry : mShardRoutingTable) {
    if (entry.second) {
      std::string node_addr = entry.second->node_ip_ + ":" + std::to_string(entry.second->node_port_);
      for (const auto& dead_addr : dead_node_addresses) {
        if (node_addr == dead_addr) {
          orphaned_shards.push_back(entry.first);
          break;
        }
      }
    }
  }
  
  MLOG_DEBUG("Found {} orphaned shards from dead nodes", orphaned_shards.size());

  // Reassign
  return ReassignOrphanedShards(orphaned_shards, alive_servers);
}

// TODO(zbhe): reuse logic for init and rebalance
error_code_t ClusterManagerShardManager::ReassignOrphanedShards(
    const std::vector<shard_id_t>& orphaned_shards,
    const std::vector<std::shared_ptr<simm::common::NodeAddress>>& alive_servers) {
  
  if (orphaned_shards.empty() || alive_servers.empty()) {
    return CommonErr::OK;
  }
  
  // Current: Reassign based on average distribution
  size_t server_count = alive_servers.size();
  size_t shards_per_server = orphaned_shards.size() / server_count;
  size_t remaining_shards = orphaned_shards.size() % server_count;
  
  std::vector<shard_id_t> target_shards;
  std::vector<std::shared_ptr<simm::common::NodeAddress>> target_servers;
  
  size_t shard_index = 0;
  for (size_t server_idx = 0; server_idx < server_count && shard_index < orphaned_shards.size(); ++server_idx) {
    size_t shards_for_this_server = shards_per_server + (server_idx < remaining_shards ? 1 : 0);
    
    for (size_t i = 0; i < shards_for_this_server && shard_index < orphaned_shards.size(); ++i) {
      target_shards.push_back(orphaned_shards[shard_index]);
      target_servers.push_back(alive_servers[server_idx]);
      
      MLOG_DEBUG("Planning to reassign shard {} to server {}:{}", 
                 orphaned_shards[shard_index], 
                 alive_servers[server_idx]->node_ip_, 
                 alive_servers[server_idx]->node_port_);
      
      shard_index++;
    }
  }
  
  error_code_t ret = BatchModifyRoutingTable(target_shards, target_servers); // TODO: migrate with szzhao
  if (ret != CommonErr::OK) {
    MLOG_ERROR("Failed to batch modify routing table during rebalance, ret: {}", ret);
    return ret;
  }
  
  MLOG_DEBUG("Redistributed {} shards to {} alive servers", 
            target_shards.size(), server_count);
  
  return CommonErr::OK;
}

// TODO(ytji): update args type and implement this function
error_code_t ClusterManagerShardManager::TriggerShardsMigration(
    const std::vector<shard_id_t> &target_shards,
    const std::vector<simm::common::NodeAddress *> &target_servers) {
  SIMM_ASSERT(target_shards.size() == target_servers.size(), "target shards size not equal to target server size");

  return CommonErr::NotImplemented;
}

// TODO(ytji): update args type and implement this function
error_code_t ClusterManagerShardManager::OnRecvShardMigrationResponse(
    /* arguments ... */) {
  return CommonErr::NotImplemented;
}

}  // namespace cm
}  // namespace simm
