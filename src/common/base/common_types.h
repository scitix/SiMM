#pragma once

#include <string.h>
#include <atomic>
#include <chrono>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

using shard_id_t = uint32_t;

namespace simm {
namespace common {

#define FOREACH_NODESTATUS(C) \
  C(UNKNOWN)                  \
  C(RUNNING)                  \
  C(DEAD)

#define NODESTATUS_ENUM(name) name,
#define NODESTATUS_STRING(name) #name,

enum NodeStatus { FOREACH_NODESTATUS(NODESTATUS_ENUM) };
const std::vector<std::string_view> kNodeStatusStrVec = {FOREACH_NODESTATUS(NODESTATUS_STRING)};

inline constexpr std::string_view NodeStatusToString(NodeStatus status) {
  if (status < 0 || status >= kNodeStatusStrVec.size()) {
    return "UNKNOWN";
  }
  return kNodeStatusStrVec[status];
}

struct NodeAddress {
  NodeAddress() {}
  NodeAddress(const std::string &ip_address, int32_t port_number) : node_ip_(ip_address), node_port_(port_number) {}

  std::string toString() const { return node_ip_ + ":" + std::to_string(node_port_); }

  static std::optional<NodeAddress> ParseFromString(const std::string &str) {
    size_t pos = str.rfind(':');
    if (pos == std::string::npos || pos == 0 || pos == str.size() - 1) {
      return std::nullopt;
    }

    std::string ip_part = str.substr(0, pos);
    if (!ip_part.empty() && ip_part.front() == '[' && ip_part.back() == ']') {
      ip_part = ip_part.substr(1, ip_part.size() - 2);
    }

    std::string port_part = str.substr(pos + 1);
    for (char c : port_part) {
      if (!std::isdigit(static_cast<unsigned char>(c))) {
        return std::nullopt;
      }
    }

    try {
      unsigned long port_num = std::stoul(port_part);
      if (port_num < 1 || port_num > 65535) {
        return std::nullopt;
      }
      return NodeAddress(ip_part, static_cast<uint32_t>(port_num));
    } catch (...) {
      return std::nullopt;
    }
  }

  std::string node_ip_;
  int32_t node_port_{-1};
};

struct NodeResource {
  NodeResource() {}
  NodeResource(int64_t mem_total_bytes, int64_t mem_free_bytes, int64_t mem_used_bytes)
      : mem_free_bytes_(mem_free_bytes), mem_total_bytes_(mem_total_bytes), mem_used_bytes_(mem_used_bytes) {}

  int64_t mem_free_bytes_{0};
  int64_t mem_total_bytes_{0};
  int64_t mem_used_bytes_{0};
};

// timestmap info of node heatbeat
struct NodeHeartbeatTs {
  NodeHeartbeatTs() {}
  NodeHeartbeatTs(const std::chrono::steady_clock::time_point &mtp, const std::chrono::system_clock::time_point &wtp)
      : monotonic_tp_(mtp), wallclock_tp_(wtp) {}

  // Used to check if the heartbeats of dataserver are valid.
  // Monitonic timestamp is able to avoid time adjustment issues,
  // e.g. NTP sync issue, clock transtion issue.
  std::chrono::steady_clock::time_point monotonic_tp_;
  // Wall clock timestamp is used to record human-readable time info.
  std::chrono::system_clock::time_point wallclock_tp_;
};

// record module service state, include start timestamp, grace period
class ModuleServiceState {
 public:
  static ModuleServiceState &GetInstance() {
    static ModuleServiceState inst;
    return inst;
  }

  void SetServiceGracePeriod(uint32_t grace_period_secs) { grace_period_in_secs_ = grace_period_secs; }

  std::chrono::steady_clock::time_point StartTimePoint() const { return start_time_; }

  // check if service grace period already finished
  bool GracePeriodFinished() {
    return std::chrono::steady_clock::now() - start_time_ > std::chrono::seconds(grace_period_in_secs_);
  }

  void MarkServiceReady() { service_ready_.store(true); }

  bool IsServiceReady() const { return service_ready_.load(); }

  // only for UT test
  void Reset(uint32_t grace_period_secs) {
    start_time_ = std::chrono::steady_clock::now();
    grace_period_in_secs_ = grace_period_secs;
    service_ready_.store(false);
  }

 private:
  ModuleServiceState(const ModuleServiceState &) = delete;
  ModuleServiceState operator=(const ModuleServiceState &) = delete;
  ModuleServiceState() : start_time_(std::chrono::steady_clock::now()) {}

  std::chrono::steady_clock::time_point start_time_;
  uint32_t grace_period_in_secs_{0};
  std::atomic<bool> service_ready_{false};
};

}  // namespace common
}  // namespace simm