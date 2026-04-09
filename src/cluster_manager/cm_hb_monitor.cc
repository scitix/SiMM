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
DECLARE_bool(cm_deferred_reshard_enabled);
DECLARE_uint32(cm_deferred_reshard_window_inSecs);
DECLARE_uint32(dataserver_min_num);

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

// Legacy: heartbeat keyed by ip:port (backward compat)
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

// heartbeat keyed by logical_node_id
error_code_t ClusterManagerHBMonitor::OnRecvNodeHeartbeat(const std::string &logical_node_id,
                                                           const std::string &ip_port) {
  simm::common::NodeHeartbeatTs hb_ts(std::chrono::steady_clock::now(), std::chrono::system_clock::now());

  // Store heartbeat keyed by logical_node_id (not ip:port)
  auto uomap_locked = ds_hb_records_.wlock();
  auto &entry = (*uomap_locked)[logical_node_id];
  if (entry.size() >= FLAGS_cm_heartbeat_records_perserver) {
    entry.pop_front();
  }
  entry.push_back(hb_ts);

  // Sync NodeManager: update logical_id → ip mapping if needed
  cm_node_manager_ptr_->OnHeartbeat(logical_node_id, ip_port);

  return CommonErr::OK;
}

void ClusterManagerHBMonitor::OnDeferredReshardResolved(const std::string &logical_node_id) {
  // Reset heartbeat records for this logical_id so the new DS starts fresh
  auto uomap_locked = ds_hb_records_.wlock();
  auto it = uomap_locked->find(logical_node_id);
  if (it != uomap_locked->end()) {
    it->second.clear();
    // Push a fresh heartbeat so the new DS doesn't immediately time out
    simm::common::NodeHeartbeatTs hb_ts(std::chrono::steady_clock::now(), std::chrono::system_clock::now());
    it->second.push_back(hb_ts);
  }
  MLOG_INFO("Node handshake resolved: logical_id={}, heartbeat records reset", logical_node_id);
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
          MLOG_WARN("Dataserver node({}) has no heartbeat records, skip it.", it->first);
          ++it;
          continue;
        }
        // Check the latest heartbeat record
        auto previous_hb_ts = hb_records.back();
        if (now - previous_hb_ts.monotonic_tp_ > std::chrono::seconds(FLAGS_cm_heartbeat_timeout_inSecs)) {
          const std::string& hb_key = it->first;  // logical_node_id or ip:port

          // Try to resolve logical_node_id for this key
          // If the key is a logical_node_id, GetNodeEntry will find it directly.
          // If the key is an ip:port (legacy), try reverse lookup.
          auto entry_opt = cm_node_manager_ptr_->GetNodeEntry(hb_key);
          std::string logical_id = hb_key;
          if (!entry_opt) {
            // Maybe hb_key is an ip:port — try reverse lookup
            logical_id = cm_node_manager_ptr_->ResolveLogicalId(hb_key);
            if (!logical_id.empty()) {
              entry_opt = cm_node_manager_ptr_->GetNodeEntry(logical_id);
            }
          }

          if (entry_opt && FLAGS_cm_deferred_reshard_enabled) {
            // logical_node_id available, deferred reshard enabled
            auto& entry = *entry_opt;

            if (entry.status == NodeStatus::DEFERRED_RESHARD) {
              // Already waiting — check if window expired
              auto elapsed = now - entry.deferred_reshard_since;
              if (elapsed > std::chrono::seconds(FLAGS_cm_deferred_reshard_window_inSecs)) {
                // Window expired → fallback to standard reshard
                // Pre-check: is reshard feasible (enough alive nodes after marking DEAD)?
                // DEFERRED_RESHARD nodes are already excluded from GetAllNodeAddress(alive=true)
                // since legacy map has them as non-RUNNING, so no need to subtract 1.
                auto alive = cm_node_manager_ptr_->GetAllNodeAddress(true);
                if (alive.size() >= FLAGS_dataserver_min_num) {
                  MLOG_WARN("Deferred window expired for logical_id={} ({}s), triggering reshard",
                            logical_id, FLAGS_cm_deferred_reshard_window_inSecs);
                  // CAS: only mark DEAD if still in DEFERRED_RESHARD (guards against concurrent
                  // ProcessHandshake that already moved it back to RUNNING)
                  auto ret = cm_node_manager_ptr_->SetNodeStatus(
                      logical_id, NodeStatus::DEAD, {}, NodeStatus::DEFERRED_RESHARD);
                  if (ret == CommonErr::OK) {
                    dead_dataservers.emplace_back(entry.current_ip_port);
                  }
                } else {
                  // Not safe to reshard — keep waiting in DEFERRED_RESHARD
                  MLOG_WARN("Deferred window expired for logical_id={}, but alive={} < min_required={}. "
                            "Staying in DEFERRED_RESHARD to avoid orphaned shards.",
                            logical_id, alive.size(), FLAGS_dataserver_min_num);
                }
              }
              // else: window not expired, keep waiting

            } else if (entry.status == NodeStatus::RUNNING) {
              // First timeout → enter DEFERRED_RESHARD state
              // CAS: only transition if still RUNNING (guards against concurrent ProcessHandshake
              // that may have re-registered the node after we read the snapshot)
              MLOG_WARN("Node heartbeat timeout: logical_id={} (ip={}), entering DEFERRED_RESHARD",
                        logical_id, entry.current_ip_port);
              cm_node_manager_ptr_->SetNodeStatus(
                  logical_id, NodeStatus::DEFERRED_RESHARD, now, NodeStatus::RUNNING);

            } else if (entry.status == NodeStatus::DEAD) {
              // Already dead, skip
            }

          } else if (entry_opt && !FLAGS_cm_deferred_reshard_enabled) {
            // logical_node_id available, deferred reshard disabled
            // Immediate DEAD + reshard
            auto& entry = *entry_opt;
            if (entry.status == NodeStatus::DEAD) {
              // Already dead, skip
            } else {
              // Pre-check: is reshard feasible?
              auto alive = cm_node_manager_ptr_->GetAllNodeAddress(true);
              size_t alive_after = alive.size() > 0 ? alive.size() - 1 : 0;
              if (alive_after >= FLAGS_dataserver_min_num) {
                MLOG_WARN("Node heartbeat timeout: logical_id={} (ip={}), marking DEAD",
                          logical_id, entry.current_ip_port);
                // CAS: only mark DEAD if the node hasn't been concurrently restored
                auto ret = cm_node_manager_ptr_->SetNodeStatus(
                    logical_id, NodeStatus::DEAD, {}, entry.status);
                if (ret == CommonErr::OK) {
                  dead_dataservers.emplace_back(entry.current_ip_port);
                }
              } else {
                // Not enough alive nodes for reshard — enter DEFERRED_RESHARD as safety net
                MLOG_WARN("Node heartbeat timeout: logical_id={} (ip={}), alive_after={} < min={}. "
                          "Entering DEFERRED_RESHARD as safety net to avoid orphaned shards.",
                          logical_id, entry.current_ip_port, alive_after, FLAGS_dataserver_min_num);
                cm_node_manager_ptr_->SetNodeStatus(
                    logical_id, NodeStatus::DEFERRED_RESHARD, now, entry.status);
              }
            }

          } else {
            // Legacy path (no logical_node_id): immediate reshard
            if (cm_node_manager_ptr_->QueryNodeStatus(hb_key) == NodeStatus::DEAD) {
              ++it;
              continue;
            }
            // Pre-check: is reshard feasible?
            auto alive = cm_node_manager_ptr_->GetAllNodeAddress(true);
            size_t alive_after = alive.size() > 0 ? alive.size() - 1 : 0;
            if (alive_after >= FLAGS_dataserver_min_num) {
              MLOG_ERROR("Dataserver node({}) heartbeat timeout({} secs), mark it as DEAD",
                         hb_key, FLAGS_cm_heartbeat_timeout_inSecs);
              cm_node_manager_ptr_->UpdateNodeStatus(hb_key, NodeStatus::DEAD);
              dead_dataservers.emplace_back(hb_key);
            } else {
              MLOG_WARN("Dataserver node({}) heartbeat timeout, but alive_after={} < min={}. "
                        "Skipping DEAD to avoid orphaned shards.",
                        hb_key, alive_after, FLAGS_dataserver_min_num);
            }
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
  if (alive_servers.empty()) {
    return;
  }

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
