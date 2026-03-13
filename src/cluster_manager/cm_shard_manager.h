#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include <folly/concurrency/ConcurrentHashMap.h>

#include "common/base/common_types.h"
#include "common/errcode/errcode_def.h"

namespace simm {
namespace cm {

using QueryResultMap = std::unordered_map<shard_id_t, std::shared_ptr<simm::common::NodeAddress>>;

class ClusterManagerShardManager : public std::enable_shared_from_this<ClusterManagerShardManager> {
 public:
  // ShardManager is upper layer sub-module, it may depends on
  // NodeManager/HBMonitor/ResourceManager
  // TODO(ytji): should pass in NodeManager/HBMonitor/ResourceManager objects to
  // ctor function ShardManager get dataserver list from NodeManager, get
  // heartbeat info from HBMonitor, get resource info from ResourceManager.
  ClusterManagerShardManager();
  virtual ~ClusterManagerShardManager();

  ClusterManagerShardManager(const ClusterManagerShardManager &) = delete;
  ClusterManagerShardManager &operator=(const ClusterManagerShardManager &) = delete;
  ClusterManagerShardManager(ClusterManagerShardManager &&) = delete;
  ClusterManagerShardManager &operator=(ClusterManagerShardManager &&) = delete;

 public:
  uint32_t GetTotalShardNum() const { return mShardNum; }

  // Called in cluster init phase, the cluster manager will wait for specified
  // grace period (e.g. 5 minutes), it will accept all valid new node handshake
  // requests to register nodes info into ClusterManagerNodeManager. Then
  // ClusterManagerShardManager can get the dataserver list to init shard
  // routing table.
  error_code_t InitShardRoutingTable(const std::vector<std::shared_ptr<simm::common::NodeAddress>> &all_servers);

  // Query shard's dataserver address info
  QueryResultMap QueryShardRoutingInfo(shard_id_t target_shard);

  // Batch query several shards' dataserver address infos
  QueryResultMap BatchQueryShardRoutingInfos(const std::vector<shard_id_t> &target_shards);

  // Query all shards' routing infos
  QueryResultMap QueryAllShardRoutingInfos();

  // Update target shard's routing info after migration task finish
  error_code_t ModifyRoutingTable(shard_id_t target_shard, std::shared_ptr<simm::common::NodeAddress> target_server);

  // Batch update target shards' routing infos after migration tasks finish
  error_code_t BatchAssignRoutingTable(const std::vector<shard_id_t> &target_shards,
                                       const std::shared_ptr<simm::common::NodeAddress> target_server);

  // Batch update target shards' routing infos after migration tasks finish
  error_code_t BatchModifyRoutingTable(const std::vector<shard_id_t> &target_shards,
                                       const std::vector<std::shared_ptr<simm::common::NodeAddress>> &target_servers);

  // Rebalance shards after failure
  error_code_t RebalanceShardsAfterNodeFailure(
      const std::vector<std::string> &dead_node_addresses,
      const std::vector<std::shared_ptr<simm::common::NodeAddress>> &alive_servers);

  // Reassign orphan shards
  error_code_t ReassignOrphanedShards(const std::vector<shard_id_t> &orphaned_shards,
                                      const std::vector<std::shared_ptr<simm::common::NodeAddress>> &alive_servers);

  // should we need this interface?
  void CleanRoutingTable() { mShardRoutingTable.clear(); };

  // TODO(ytji): create policy factory to create different shard placement
  // policies
  //  but we need to decide, which class object will manager policy?
  //  ShardManager or ShardPlacementScheduler? Maybe ShardPlacementScheduler is
  //  more situable?

 private:
  // Based shard placement policy, cm will trigger some shards migration to
  // target servers if some conditions meet, and send RPC requests to related
  // source dataservers. The RPC request is non-blocking, source dataservers
  // should reply response after migration task launched successfully on it
  // instead of waiting for tasks finish.
  error_code_t TriggerShardsMigration(const std::vector<shard_id_t> &target_shards,
                                      const std::vector<simm::common::NodeAddress *> &target_servers);

  // When source dataservers finish migration tasks, it will send respone to
  // ClusterManager, RPC handlers should call ClusterManagerShardManager to
  // update shard routing table
  error_code_t OnRecvShardMigrationResponse(/* arguments ... */);

 private:
  // high performance concurrent hash table to store [shard_id, ds_addr_str]
  folly::ConcurrentHashMap<shard_id_t, std::shared_ptr<simm::common::NodeAddress>> mShardRoutingTable;
  uint32_t mShardNum{0};

  // only for UT test
#if defined(SIMM_UNIT_TEST)
  FRIEND_TEST(ClusterManagerShardManagerTest, TestTriggerShardsMigration);
  FRIEND_TEST(ClusterManagerShardManagerTest, TestOnRecvShardMigrationResponse);
#endif
};

}  // namespace cm
}  // namespace simm
