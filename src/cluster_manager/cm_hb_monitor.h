#pragma once

#include <deque>
#include <string>
#include <thread>
#include <unordered_map>

#include <folly/coro/Synchronized.h>
#include <folly/synchronization/Baton.h>

#include "cm_shard_manager.h"
#include "cm_node_manager.h"
#include "common/base/common_types.h"
#include "common/errcode/errcode_def.h"

namespace simm {
namespace cm {

class ClusterManagerHBMonitor : public std::enable_shared_from_this<ClusterManagerHBMonitor> {
 public:
  ClusterManagerHBMonitor() = default;
  ClusterManagerHBMonitor(std::shared_ptr<simm::cm::ClusterManagerNodeManager> nm_ptr,
                          std::shared_ptr<simm::cm::ClusterManagerShardManager> sm_ptr)
    : cm_node_manager_ptr_(nm_ptr), cm_shard_manager_ptr_(sm_ptr) {}
  virtual ~ClusterManagerHBMonitor();

  ClusterManagerHBMonitor(const ClusterManagerHBMonitor&) = delete;
  ClusterManagerHBMonitor& operator=(const ClusterManagerHBMonitor&) = delete;
  ClusterManagerHBMonitor(ClusterManagerHBMonitor&&) = delete;
  ClusterManagerHBMonitor& operator=(ClusterManagerHBMonitor&&) = delete;

 public:
  error_code_t Init();
  error_code_t Start();
  error_code_t Stop();

  error_code_t OnRecvNodeHeartbeat(const std::string &node_addr_str);

 private:
  void BgHBScanLoop();

  void HandleNodeFailure(const std::vector<std::string> &dead_dataservers);

 private:
  // record all dataservers' heartbeat timestamps
  // key : ds address string(ip:port)
  // val : deque of latest N(default is 100) heartbeat timestamps
  using InnerUOMap = std::unordered_map<std::string, std::deque<simm::common::NodeHeartbeatTs>>;
  folly::Synchronized<InnerUOMap> ds_hb_records_;

  std::shared_ptr<simm::cm::ClusterManagerNodeManager> cm_node_manager_ptr_{nullptr};
  std::shared_ptr<simm::cm::ClusterManagerShardManager> cm_shard_manager_ptr_{nullptr};

  std::atomic<bool> stop_flag_{false};
  folly::Baton<> bg_scan_thread_baton_;
  std::jthread bg_scan_thread_;

#if defined(SIMM_UNIT_TEST)
  FRIEND_TEST(ClusterManagerHBMonitorTest, TestHBMonitor);
#endif
};

}  // namespace cm
}  // namespace simm
