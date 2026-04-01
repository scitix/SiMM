#pragma once

#include <atomic>
#include <memory>

#include "rpc/rpc.h"
#include "rpc/rpc_context.h"
#include "transport/types.h"

#include "cm_hb_monitor.h"
#include "cm_node_manager.h"
#include "cm_shard_manager.h"
#include "common/errcode/errcode_def.h"

namespace simm {
namespace cm {

class ClusterManagerService {
 public:
  enum class MarkDeadStrategy {
    FIXED_PERIOD,
    FIXED_HB_ROUNDS,
    TIME_WINDOW_SAMPLE,
  };

  enum class MarkAliveStrategy {
    TIME_WINDOW_SAMPLE,
  };

  ClusterManagerService()
      : shard_manager_(std::make_shared<simm::cm::ClusterManagerShardManager>()),
        node_manager_(std::make_shared<simm::cm::ClusterManagerNodeManager>()),
        hb_monitor_(std::make_shared<simm::cm::ClusterManagerHBMonitor>(node_manager_, shard_manager_)) {}
  ClusterManagerService(std::shared_ptr<simm::cm::ClusterManagerShardManager> sm,
                        std::shared_ptr<simm::cm::ClusterManagerNodeManager> nm,
                        std::shared_ptr<simm::cm::ClusterManagerHBMonitor> hbm)
      : shard_manager_(sm), node_manager_(nm), hb_monitor_(hbm) {}
  virtual ~ClusterManagerService();

  ClusterManagerService(const ClusterManagerService &) = delete;
  ClusterManagerService &operator=(const ClusterManagerService &) = delete;
  ClusterManagerService(ClusterManagerService &&) = delete;
  ClusterManagerService &operator=(ClusterManagerService &&) = delete;

 public:
  error_code_t Init();
  error_code_t Start();
  error_code_t Stop();

  bool IsRunning() const { return is_running_.load(); }
  bool IsStopped() const { return !is_running_.load(); }

 private:
  error_code_t StartRPCServices();
  error_code_t StopRPCServices();

 private:
  // for intra-cm communication, including data sync
  std::unique_ptr<sicl::rpc::SiRPC> intra_rpc_service_{nullptr};
  // for in-cluster communication, with dataserver or client
  std::unique_ptr<sicl::rpc::SiRPC> inter_rpc_service_{nullptr};
  // for maintainence scenario
  std::unique_ptr<sicl::rpc::SiRPC> admin_rpc_service_{nullptr};

  std::shared_ptr<simm::cm::ClusterManagerShardManager> shard_manager_{nullptr};
  std::shared_ptr<simm::cm::ClusterManagerNodeManager> node_manager_{nullptr};
  std::shared_ptr<simm::cm::ClusterManagerHBMonitor> hb_monitor_{nullptr};

  std::atomic<bool> is_running_{false};

  // Node failure and rebalance
  void HandleNodeFailure(const std::vector<std::string> &dead_node_addresses);

#if defined(SIMM_UNIT_TEST)
  FRIEND_TEST(ClusterManagerServiceTest, TestQueryRoutingTableInfoRPCs);
  FRIEND_TEST(ClusterManagerServiceTest, TestQueryRoutingTableRejectsRequestsDuringGracePeriod);
  FRIEND_TEST(ClusterManagerServiceTest, TestQueryRoutingTableReturnsIncompleteWhenShardUnavailable);
  FRIEND_TEST(ClusterManagerServiceTest, TestNodeRejoinRPC);
#endif
};

}  // namespace cm
}  // namespace simm
