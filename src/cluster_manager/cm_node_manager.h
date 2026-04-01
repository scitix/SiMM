#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <folly/concurrency/ConcurrentHashMap.h>
#include <folly/synchronization/Baton.h>

#if defined(SIMM_UNIT_TEST)
#include <gtest/gtest_prod.h>
#endif

#include "rpc/connection.h"
#include "rpc/rpc.h"
#include "rpc/rpc_context.h"
#include "transport/types.h"

#include "common/base/common_types.h"
#include "common/errcode/errcode_def.h"

namespace simm {
namespace cm {

using NodeStatus = common::NodeStatus;

class ClusterManagerNodeManager : public std::enable_shared_from_this<ClusterManagerNodeManager> {
 public:
  /**
   * @brief ClusterManagerNodeManager controls all data server nodes. Get all
   * alive data servers when initializing, AddNode/DelNode when HBMonitor get
   * new node up/down.
   */
  ClusterManagerNodeManager();
  virtual ~ClusterManagerNodeManager();

  ClusterManagerNodeManager(const ClusterManagerNodeManager &) = delete;
  ClusterManagerNodeManager &operator=(const ClusterManagerNodeManager &) = delete;
  ClusterManagerNodeManager(ClusterManagerNodeManager &&) = delete;
  ClusterManagerNodeManager &operator=(ClusterManagerNodeManager &&) = delete;

 public:
  // create rpc client and start get resource thread
  void Init();

  // stop backgroup thread
  void Stop();

  // get alive/all nodes address info
  std::vector<std::shared_ptr<simm::common::NodeAddress>> GetAllNodeAddress(bool alive = true);

  // add a new node in node manager, will send a rpc to get node resource info
  error_code_t AddNode(const std::string &addr_str);

  // delete node from node manager, remove all node info from map
  error_code_t DelNode(const std::string &addr_str);

  // update node status (RUNNING/DEAD)
  error_code_t UpdateNodeStatus(const std::string &addr_str, NodeStatus status);

  // query one data node status
  NodeStatus QueryNodeStatus(const std::string &addr_str);

  // query whether data node exists
  bool QueryNodeExists(const std::string &addr_str);

  // get resource info of a single data node
  std::shared_ptr<simm::common::NodeResource> GetNodeResource(const std::string &addr_str);

  // get status of all data nodes
  // Returns an unordered_map of address string to node status
  std::unordered_map<std::string, NodeStatus> GetAllNodeStatus();

  // TODO: will query each resource info from data nodes
  std::unordered_map<std::string, std::shared_ptr<simm::common::NodeResource>> GetAllNodeResource();

 private:
  std::shared_ptr<simm::common::NodeResource> getNodeResource(const std::string &addr_str);
  void updateAllNodeResource();

 private:
  // map to record dataservers' resource stats
  // key   : address string(ip:port)
  // value : struct contains resource stats, e.g. server-level total/free memory, shard-level total/free memory
  folly::ConcurrentHashMap<std::string, std::shared_ptr<simm::common::NodeResource>> node_info_map_;

  // map to record dataservers' node status
  // key   : address string(ip:port)
  // value : node status, e.g. alive, dead, ....
  folly::ConcurrentHashMap<std::string, NodeStatus> node_status_map_;

  sicl::rpc::SiRPC *rpc_client_{nullptr};
  std::thread *resource_thread_{nullptr};
  std::atomic<bool> resource_thread_stop_{false};
  folly::Baton<> resource_thread_baton_;
  uint64_t start_timestamp_us_{0};

  // only for UT test
#if defined(SIMM_UNIT_TEST)
  FRIEND_TEST(ClusterManagerHBMonitorTest, TestHBMonitor);
#endif
};

}  // namespace cm
}  // namespace simm
