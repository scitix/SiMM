#include <chrono>
#include <ctime>

#include <gflags/gflags.h>

#include "cm_hb_monitor.h"
#include "cm_node_manager.h"
#include "common/base/assert.h"
#include "common/logging/logging.h"

DECLARE_LOG_MODULE("cluster_manager");

DECLARE_uint32(cm_heartbeat_records_perserver);
DECLARE_uint32(cm_heartbeat_bg_scan_interval_inSecs);
DECLARE_uint32(cm_heartbeat_timeout_inSecs);

namespace simm {
namespace cm {

ClusterManagerHBMonitor::~ClusterManagerHBMonitor() {
  Stop();
}

error_code_t ClusterManagerHBMonitor::Init() {
  return CommonErr::OK;
}

error_code_t ClusterManagerHBMonitor::Start() {
  stop_flag_.store(false);
  bg_scan_thread_baton_.reset();
  if (bg_scan_thread_.joinable()) {
    MLOG_WARN("Background HB scan thread is still joinable, not start new thread");
    return CommonErr::OK;
  }
  auto self = shared_from_this();
  bg_scan_thread_ = std::jthread([self]() { self->BgHBScanLoop(); });
  return CommonErr::OK;
}

error_code_t ClusterManagerHBMonitor::Stop() {
  stop_flag_.store(true);
  bg_scan_thread_baton_.post();

  if (bg_scan_thread_.joinable()) {
    bg_scan_thread_.join();
  }

  return CommonErr::OK;
}

error_code_t ClusterManagerHBMonitor::OnRecvNodeHeartbeat(const std::string &node_addr_str) {
  simm::common::NodeHeartbeatTs hb_ts(std::chrono::steady_clock::now(), std::chrono::system_clock::now());
  auto uomap_locked = ds_hb_records_.wlock();
  auto &entry = (*uomap_locked)[node_addr_str];
  if (entry.size() >= FLAGS_cm_heartbeat_records_perserver) {
    MLOG_DEBUG("Heartbeat records(current:{}) for node({}) is full(limit:{}), removing oldest records",
               entry.size(),
               node_addr_str,
               FLAGS_cm_heartbeat_records_perserver);
    entry.pop_front();
  }
  entry.push_back(hb_ts);
  // RAII, no need to call unlock()

  return CommonErr::OK;
}

void ClusterManagerHBMonitor::BgHBScanLoop() {
  while (!stop_flag_.load()) {
    std::vector<std::string> dead_dataservers{};
    {  // code block for rlock
      auto now = std::chrono::steady_clock::now();
      auto uomap_locked = ds_hb_records_.rlock();
      for (auto it = uomap_locked->begin(); it != uomap_locked->end();) {
        auto &hb_records = it->second;
        if (hb_records.empty()) {
          // it = uomap_locked->erase(it);
          MLOG_WARN("Dataserver node({}) has no heartbeat records, skip it.", it->first);
          ++it;
          continue;
        }
        // TODO(ytji): add more mark dead strategies
        // Check the latest heartbeat record
        auto previous_hb_ts = hb_records.back();
        if (now - previous_hb_ts.monotonic_tp_ > std::chrono::seconds(FLAGS_cm_heartbeat_timeout_inSecs)) {
          MLOG_ERROR("Dataserver node({}) heartbeat timeout({} secs), mark it as DEAD",
                     it->first,
                     FLAGS_cm_heartbeat_timeout_inSecs);
          if (cm_node_manager_ptr_->QueryNodeStatus(it->first) == NodeStatus::DEAD) {
            ++it;
            continue;
          } else {
            // mark server as dead state
            cm_node_manager_ptr_->UpdateNodeStatus(it->first, NodeStatus::DEAD);
            // record all dead servers in current scan round to trigger shard manager update in batch
            dead_dataservers.emplace_back(it->first);
            // FIXME(ytji): we still keep the heartbeat records for the node,
            // it = uomap_locked->erase(it);
          }
        }
        ++it;
      }

    }  // release rlock

    // dead dataservers will trigger shard table refresh
    if (!dead_dataservers.empty()) {
      HandleNodeFailure(dead_dataservers);
    }

    MLOG_DEBUG("ClusterManagerHBMonitor::BgHBScanLoop wait some seconds before next round...");
    // sleep some seconds before next scan
    bg_scan_thread_baton_.timed_wait(std::chrono::milliseconds(FLAGS_cm_heartbeat_bg_scan_interval_inSecs * 1000));
    bg_scan_thread_baton_.reset();
  }
}

void ClusterManagerHBMonitor::HandleNodeFailure(const std::vector<std::string> &dead_node_addresses) {
  MLOG_WARN("Handling failure of {} nodes", dead_node_addresses.size());

  auto alive_servers = cm_node_manager_ptr_->GetAllNodeAddress(true /* alive only */);

  // rebalance shards after node failure
  error_code_t ret = cm_shard_manager_ptr_->RebalanceShardsAfterNodeFailure(dead_node_addresses, alive_servers);
  if (ret != CommonErr::OK) {
    MLOG_ERROR("Failed to rebalance shards after node failure, ret: {}", ret);
  } else {
    MLOG_DEBUG("Successfully rebalanced shards after node failure");
  }
}
}  // namespace cm
}  // namespace simm
