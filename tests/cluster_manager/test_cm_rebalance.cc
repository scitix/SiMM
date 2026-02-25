#include <atomic>
#include <chrono>
#include <latch>
#include <memory>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <folly/Random.h>
#include <folly/ScopeGuard.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/synchronization/Baton.h>

#include "cluster_manager/cm_hb_monitor.h"
#include "cluster_manager/cm_node_manager.h"
#include "cluster_manager/cm_rpc_handler.h"
#include "cluster_manager/cm_service.h"
#include "cluster_manager/cm_shard_manager.h"
#include "common/base/common_types.h"
#include "common/errcode/errcode_def.h"
#include "common/logging/logging.h"
#include "proto/ds_cm_rpcs.pb.h"

DECLARE_LOG_MODULE("cluster_manager_test");

DECLARE_uint32(shard_total_num);
DECLARE_int32(cm_rpc_inter_port);
DECLARE_uint32(cm_cluster_init_grace_period_inSecs);
DECLARE_uint32(cm_heartbeat_timeout_inSecs);
DECLARE_uint32(cm_heartbeat_bg_scan_interval_inSecs);
DECLARE_string(cm_log_file);

namespace simm {
namespace cm {

class ClusterManagerRebalanceTest : public ::testing::Test {
 protected:
  void SetUp() override {
    simm::logging::LogConfig cm_log_config = simm::logging::LogConfig{FLAGS_cm_log_file, "DEBUG"};
    simm::logging::LoggerManager::Instance().UpdateConfig("cluster_manager", cm_log_config);

    // Backup original flag values
    original_grace_period_ = FLAGS_cm_cluster_init_grace_period_inSecs;
    original_heartbeat_timeout_ = FLAGS_cm_heartbeat_timeout_inSecs;
    original_heartbeat_scan_interval_ = FLAGS_cm_heartbeat_bg_scan_interval_inSecs;

    // Overwrite
    FLAGS_cm_cluster_init_grace_period_inSecs = 3;
    FLAGS_cm_heartbeat_timeout_inSecs = 5;
    FLAGS_cm_heartbeat_bg_scan_interval_inSecs = 1;
  }

  void TearDown() override {
    // Restore original flag values
    FLAGS_cm_cluster_init_grace_period_inSecs = original_grace_period_;
    FLAGS_cm_heartbeat_timeout_inSecs = original_heartbeat_timeout_;
    FLAGS_cm_heartbeat_bg_scan_interval_inSecs = original_heartbeat_scan_interval_;
  }

 private:
  uint32_t original_grace_period_;
  uint32_t original_heartbeat_timeout_;
  uint32_t original_heartbeat_scan_interval_;
};

// TODO: move MockDS to a common test utils file?
class MockDataServer {
 public:
  MockDataServer(const std::string &ip, int port) : ip_(ip), port_(port), is_running_(false), should_stop_(false) {
    node_address_ = std::make_shared<simm::common::NodeAddress>(ip, port);
  }

  ~MockDataServer() { Stop(); }

  error_code_t Start() {
    if (is_running_.exchange(true)) {
      return CommonErr::OK;  // already running
    }

    should_stop_.store(false);

    SendHandshakeRequest();

    // Start hb loop
    heartbeat_thread_ = std::jthread([this]() { HeartbeatLoop(); });

    MLOG_INFO("MockDataServer {}:{} started", ip_, port_);
    return CommonErr::OK;
  }

  error_code_t Stop() {
    if (!is_running_.exchange(false)) {
      return CommonErr::OK;
    }

    should_stop_.store(true);

    if (heartbeat_thread_.joinable()) {
      heartbeat_thread_.join();
    }

    MLOG_INFO("MockDataServer {}:{} stopped", ip_, port_);
    return CommonErr::OK;
  }

  std::string GetAddressString() const { return ip_ + ":" + std::to_string(port_); }

  std::shared_ptr<simm::common::NodeAddress> GetNodeAddress() const { return node_address_; }

  bool IsRunning() const { return is_running_.load(); }

 private:
  void SendHandshakeRequest() {
    try {
      sicl::rpc::SiRPC *sirpc_client;
      sicl::rpc::SiRPC::newInstance(sirpc_client, false);

      sicl::rpc::RpcContext *ctx_p = nullptr;
      sicl::rpc::RpcContext::newInstance(ctx_p);
      std::shared_ptr<sicl::rpc::RpcContext> ctx(ctx_p);
      ctx->set_timeout(sicl::transport::TimerTick::TIMER_3S);

      // handshake
      NewNodeHandShakeRequestPB hs_req;
      auto hs_resp = new NewNodeHandShakeResponsePB();
      auto node_field = hs_req.mutable_node();
      node_field->set_ip(ip_);
      node_field->set_port(port_);

      auto hs_done_cb = [](const google::protobuf::Message *rsp, const std::shared_ptr<sicl::rpc::RpcContext> ctx) {
        if (ctx->ErrorCode() == sicl::transport::Result::SICL_SUCCESS) {
          auto response = dynamic_cast<const NewNodeHandShakeResponsePB *>(rsp);
          MLOG_INFO("Handshake successful, ret_code: {}", response->ret_code());
        } else {
          MLOG_ERROR("Handshake failed, error: {}", ctx->ErrorCode());
        }
      };

      sirpc_client->SendRequest(
          "127.0.0.1",
          FLAGS_cm_rpc_inter_port,
          static_cast<sicl::rpc::ReqType>(simm::cm::ClusterManagerRpcType::RPC_NEW_NODE_HANDSHAKE),
          hs_req,
          hs_resp,
          ctx,
          hs_done_cb);

    } catch (const std::exception &e) {
      MLOG_ERROR("Exception in SendHandshakeRequest: {}", e.what());
    }
  }

  void HeartbeatLoop() {
    while (!should_stop_.load()) {
      if (!is_running_.load()) {
        break;
      }

      try {
        SendHeartbeat();
      } catch (const std::exception &e) {
        MLOG_ERROR("Exception in HeartbeatLoop: {}", e.what());
      }

      std::this_thread::sleep_for(std::chrono::seconds(2));
    }
  }

  void SendHeartbeat() {
    try {
      sicl::rpc::SiRPC *sirpc_client;
      sicl::rpc::SiRPC::newInstance(sirpc_client, false);

      sicl::rpc::RpcContext *ctx_p = nullptr;
      sicl::rpc::RpcContext::newInstance(ctx_p);
      std::shared_ptr<sicl::rpc::RpcContext> ctx(ctx_p);
      ctx->set_timeout(sicl::transport::TimerTick::TIMER_3S);

      DataServerHeartBeatRequestPB hb_req;
      auto hb_resp = new DataServerHeartBeatResponsePB();
      auto node_field = hb_req.mutable_node();
      node_field->set_ip(ip_);
      node_field->set_port(port_);

      auto hb_done_cb = [](const google::protobuf::Message *rsp, const std::shared_ptr<sicl::rpc::RpcContext> ctx) {
        if (ctx->ErrorCode() == sicl::transport::Result::SICL_SUCCESS) {
          MLOG_DEBUG("Heartbeat successful");
        } else {
          MLOG_ERROR("Heartbeat failed, error: {}", ctx->ErrorCode());
        }
      };

      sirpc_client->SendRequest("127.0.0.1",
                                FLAGS_cm_rpc_inter_port,
                                static_cast<sicl::rpc::ReqType>(simm::cm::ClusterManagerRpcType::RPC_NODE_HEARTBEAT),
                                hb_req,
                                hb_resp,
                                ctx,
                                hb_done_cb);

    } catch (const std::exception &e) {
      MLOG_ERROR("Exception in SendHeartbeat: {}", e.what());
    }
  }

 private:
  std::string ip_;
  int port_;
  std::shared_ptr<simm::common::NodeAddress> node_address_;
  std::atomic<bool> is_running_;
  std::atomic<bool> should_stop_;
  std::jthread heartbeat_thread_;
};

TEST_F(ClusterManagerRebalanceTest, TestShardRebalanceAfterNodeFailure) {
  constexpr uint32_t kDataserverNum = 4;
  const std::string ip_prefix = "192.168.0."; // TODO: remove this?
  const int base_port = 40000;

  std::atomic<bool> cm_started{false};
  std::atomic<bool> test_completed{false};
  folly::Baton<> cm_done;
  folly::CPUThreadPoolExecutor executor(8);

  auto shard_manager_ptr = std::make_shared<simm::cm::ClusterManagerShardManager>();
  auto node_manager_ptr = std::make_shared<simm::cm::ClusterManagerNodeManager>();
  auto hb_monitor_ptr = std::make_shared<simm::cm::ClusterManagerHBMonitor>(node_manager_ptr, shard_manager_ptr);
  auto cm_service_ptr =
      std::make_unique<simm::cm::ClusterManagerService>(shard_manager_ptr, node_manager_ptr, hb_monitor_ptr);

  std::vector<std::unique_ptr<MockDataServer>> dataservers;
  for (uint32_t i = 0; i < kDataserverNum; ++i) {
    std::string ip = ip_prefix + std::to_string(i + 1);
    int port = base_port + i;
    auto ds = std::make_unique<MockDataServer>(ip, port);
    dataservers.push_back(std::move(ds));
  }

  // CM thread
  auto cm_thread = [&]() {
    MLOG_INFO("Starting CM service...");

    simm::common::ModuleServiceState::GetInstance().Reset(FLAGS_cm_cluster_init_grace_period_inSecs);
    
    // Should pre-init DS to avoid TooFew error
    std::vector<std::shared_ptr<simm::common::NodeAddress>> pre_init_servers;
    for (uint32_t i = 0; i < kDataserverNum; ++i) {
      std::string ip = ip_prefix + std::to_string(i + 1);
      int port = base_port + i;
      auto server_addr = std::make_shared<simm::common::NodeAddress>(ip, port);
      pre_init_servers.push_back(server_addr);
      node_manager_ptr->AddNode(server_addr->toString());
    }

    auto ret = cm_service_ptr->Init();
    EXPECT_EQ(ret, CommonErr::OK);

    ret = shard_manager_ptr->InitShardRoutingTable(pre_init_servers);
    EXPECT_EQ(ret, CommonErr::OK);

    ret = cm_service_ptr->Start();
    EXPECT_EQ(ret, CommonErr::OK);
    EXPECT_TRUE(cm_service_ptr->IsRunning());

    cm_started.store(true);
    MLOG_INFO("CM service started successfully");

    // Wait all tests
    while (!test_completed.load()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    MLOG_INFO("Stopping CM service...");
    ret = cm_service_ptr->Stop();
    EXPECT_EQ(ret, CommonErr::OK);
    MLOG_INFO("CM service stopped");

    cm_done.post();
  };

  executor.add(cm_thread);

  while (!cm_started.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  MLOG_INFO("Starting {} DataServers...", kDataserverNum);
  for (auto &ds : dataservers) {
    auto ret = ds->Start();
    EXPECT_EQ(ret, CommonErr::OK);
  }

  MLOG_INFO("Waiting for cluster initialization...");
  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_cm_cluster_init_grace_period_inSecs + 2));

  auto all_nodes = node_manager_ptr->GetAllNodeAddress(true);
  EXPECT_EQ(all_nodes.size(), kDataserverNum);
  MLOG_INFO("All {} DataServers registered successfully", all_nodes.size());

  auto initial_routing = shard_manager_ptr->QueryAllShardRoutingInfos();
  EXPECT_EQ(initial_routing.size(), FLAGS_shard_total_num);

  std::unordered_map<std::string, uint32_t> initial_shard_count;
  for (const auto &entry : initial_routing) {
    if (entry.second) {
      std::string node_addr = entry.second->toString();
      initial_shard_count[node_addr]++;
    }
  }

  MLOG_INFO("Initial shard distribution:");
  for (const auto &entry : initial_shard_count) {
    MLOG_INFO("  Node {}: {} shards", entry.first, entry.second);
  }

  // Shutdown one ds
  MLOG_INFO("Simulating failure of DataServer: {}", dataservers[0]->GetAddressString());
  std::string failed_node_addr = dataservers[0]->GetAddressString();
  auto ret = dataservers[0]->Stop();
  EXPECT_EQ(ret, CommonErr::OK);

  MLOG_INFO("Waiting for heartbeat timeout and rebalance...");
  std::this_thread::sleep_for(
      std::chrono::seconds(FLAGS_cm_heartbeat_timeout_inSecs + FLAGS_cm_heartbeat_bg_scan_interval_inSecs + 2));

  MLOG_INFO("Verifying rebalance results...");

  // Alive nodes num
  auto alive_nodes = node_manager_ptr->GetAllNodeAddress(true);
  EXPECT_EQ(alive_nodes.size(), kDataserverNum - 1);
  MLOG_INFO("Alive nodes after failure: {}", alive_nodes.size());

  auto rebalanced_routing = shard_manager_ptr->QueryAllShardRoutingInfos();
  EXPECT_EQ(rebalanced_routing.size(), FLAGS_shard_total_num);

  std::unordered_map<std::string, uint32_t> rebalanced_shard_count;
  std::unordered_set<std::string> nodes_with_shards;

  for (const auto &entry : rebalanced_routing) {
    if (entry.second) {
      std::string node_addr = entry.second->toString();
      rebalanced_shard_count[node_addr]++;
      nodes_with_shards.insert(node_addr);

      // No shards assigned to failed nodes
      EXPECT_NE(node_addr, failed_node_addr)
          << "Found shard " << entry.first << " still assigned to failed node " << failed_node_addr;
    }
  }

  MLOG_INFO("Rebalanced shard distribution:");
  for (const auto &entry : rebalanced_shard_count) {
    MLOG_INFO("  Node {}: {} shards", entry.first, entry.second);
  }

  // Alive nodes should have shards assigned
  EXPECT_EQ(nodes_with_shards.size(), kDataserverNum - 1);

  // Total shards should remain the same
  uint32_t total_shards = 0;
  for (const auto &entry : rebalanced_shard_count) {
    total_shards += entry.second;
  }
  EXPECT_EQ(total_shards, FLAGS_shard_total_num);

  // TODO: Used average as assertion, better logic? or change if algorithm changes.
  if (!rebalanced_shard_count.empty()) {
    auto min_max = std::minmax_element(rebalanced_shard_count.begin(),
                                       rebalanced_shard_count.end(),
                                       [](const auto &a, const auto &b) { return a.second < b.second; });

    uint32_t min_shards = min_max.first->second;
    uint32_t max_shards = min_max.second->second;
    uint32_t expected_avg = FLAGS_shard_total_num / (kDataserverNum - 1);

    MLOG_INFO("Shard distribution: min={}, max={}, expected_avg={}", min_shards, max_shards, expected_avg);

    EXPECT_LE(max_shards - min_shards, expected_avg / 2 + 1);
  }

  MLOG_INFO("Rebalance test completed successfully");

  // Clean up
  for (size_t i = 1; i < dataservers.size(); ++i) {
    dataservers[i]->Stop();
  }

  test_completed.store(true);

  cm_done.wait();

  executor.join();
}

TEST_F(ClusterManagerRebalanceTest, TestMultipleNodeFailures) {
  constexpr uint32_t kDataserverNum = 6;
  constexpr uint32_t kFailureNum = 2; // TODO: what if remain nodes less than min req?
  const std::string ip_prefix = "192.168.1."; // TODO: remove this?
  const int base_port = 41000;

  std::atomic<bool> cm_started{false};
  std::atomic<bool> test_completed{false};
  folly::Baton<> cm_done;
  folly::CPUThreadPoolExecutor executor(10);

  auto shard_manager_ptr = std::make_shared<simm::cm::ClusterManagerShardManager>();
  auto node_manager_ptr = std::make_shared<simm::cm::ClusterManagerNodeManager>();
  auto hb_monitor_ptr = std::make_shared<simm::cm::ClusterManagerHBMonitor>(node_manager_ptr, shard_manager_ptr);
  auto cm_service_ptr =
      std::make_unique<simm::cm::ClusterManagerService>(shard_manager_ptr, node_manager_ptr, hb_monitor_ptr);

  // CM thread
  auto cm_thread = [&]() {
    MLOG_INFO("Starting CM service for multiple failure test...");

    simm::common::ModuleServiceState::GetInstance().Reset(FLAGS_cm_cluster_init_grace_period_inSecs);

    // Should pre-init DS to avoid TooFew error
    std::vector<std::shared_ptr<simm::common::NodeAddress>> pre_init_servers;
    for (uint32_t i = 0; i < kDataserverNum; ++i) {
      std::string ip = ip_prefix + std::to_string(i + 1);
      int port = base_port + i;
      auto server_addr = std::make_shared<simm::common::NodeAddress>(ip, port);
      pre_init_servers.push_back(server_addr);
      node_manager_ptr->AddNode(server_addr->toString());
    }

    auto ret = cm_service_ptr->Init();
    EXPECT_EQ(ret, CommonErr::OK);

    ret = shard_manager_ptr->InitShardRoutingTable(pre_init_servers);
    EXPECT_EQ(ret, CommonErr::OK);

    ret = cm_service_ptr->Start();
    EXPECT_EQ(ret, CommonErr::OK);

    cm_started.store(true);
    MLOG_INFO("CM service started successfully");

    // Wait all tests
    while (!test_completed.load()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    MLOG_INFO("Stopping CM service...");
    ret = cm_service_ptr->Stop();
    EXPECT_EQ(ret, CommonErr::OK);
    MLOG_INFO("CM service stopped");

    cm_done.post();
  };

  executor.add(cm_thread);

  while (!cm_started.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  std::vector<std::unique_ptr<MockDataServer>> dataservers;
  for (uint32_t i = 0; i < kDataserverNum; ++i) {
    std::string ip = ip_prefix + std::to_string(i + 1);
    int port = base_port + i;
    auto ds = std::make_unique<MockDataServer>(ip, port);
    dataservers.push_back(std::move(ds));
  }

  MLOG_INFO("Starting {} DataServers for multiple failure test...", kDataserverNum);
  for (auto &ds : dataservers) {
    auto ret = ds->Start();
    EXPECT_EQ(ret, CommonErr::OK);
  }

  MLOG_INFO("Waiting for cluster initialization...");
  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_cm_cluster_init_grace_period_inSecs + 2));

  auto all_nodes = node_manager_ptr->GetAllNodeAddress(true);
  EXPECT_EQ(all_nodes.size(), kDataserverNum);
  MLOG_INFO("All {} DataServers registered successfully", all_nodes.size());

  auto initial_routing = shard_manager_ptr->QueryAllShardRoutingInfos();
  EXPECT_EQ(initial_routing.size(), FLAGS_shard_total_num);

  std::unordered_map<std::string, uint32_t> initial_shard_count;
  for (const auto &entry : initial_routing) {
    if (entry.second) {
      std::string node_addr = entry.second->toString();
      initial_shard_count[node_addr]++;
    }
  }

  MLOG_INFO("Initial shard distribution:");
  for (const auto &entry : initial_shard_count) {
    MLOG_INFO("  Node {}: {} shards", entry.first, entry.second);
  }

  // Shutdown multiple ds
  std::vector<std::string> failed_nodes;
  for (uint32_t i = 0; i < kFailureNum; ++i) {
    failed_nodes.push_back(dataservers[i]->GetAddressString());
    MLOG_INFO("Simulating failure of DataServer: {}", failed_nodes.back());
    dataservers[i]->Stop();
  }

  MLOG_INFO("Waiting for heartbeat timeout and rebalance...");
  std::this_thread::sleep_for(
      std::chrono::seconds(FLAGS_cm_heartbeat_timeout_inSecs + FLAGS_cm_heartbeat_bg_scan_interval_inSecs + 3));

  MLOG_INFO("Verifying rebalance results...");

  // Alive nodes num
  auto alive_nodes_after = node_manager_ptr->GetAllNodeAddress(true);
  EXPECT_EQ(alive_nodes_after.size(), kDataserverNum - kFailureNum);
  MLOG_INFO("Alive nodes after failure: {}", alive_nodes_after.size());

  auto rebalanced_routing = shard_manager_ptr->QueryAllShardRoutingInfos();
  EXPECT_EQ(rebalanced_routing.size(), FLAGS_shard_total_num);

  std::unordered_map<std::string, uint32_t> rebalanced_shard_count;
  std::unordered_set<std::string> nodes_with_shards;

  for (const auto &entry : rebalanced_routing) {
    if (entry.second) {
      std::string node_addr = entry.second->toString();
      rebalanced_shard_count[node_addr]++;
      nodes_with_shards.insert(node_addr);

      // No shards assigned to failed nodes
      for (const auto &failed_node : failed_nodes) {
        EXPECT_NE(node_addr, failed_node) 
            << "Found shard " << entry.first << " still assigned to failed node " << failed_node;
      }
    }
  }

  MLOG_INFO("Rebalanced shard distribution:");
  for (const auto &entry : rebalanced_shard_count) {
    MLOG_INFO("  Node {}: {} shards", entry.first, entry.second);
  }

  // Alive nodes should have shards assigned
  EXPECT_EQ(nodes_with_shards.size(), kDataserverNum - kFailureNum);

  // Total shards should remain the same
  uint32_t total_shards = 0;
  for (const auto &entry : rebalanced_shard_count) {
    total_shards += entry.second;
  }
  EXPECT_EQ(total_shards, FLAGS_shard_total_num);

  // TODO: Used average as assertion, better logic? or change if algorithm changes.
  if (!rebalanced_shard_count.empty()) {
    auto min_max = std::minmax_element(rebalanced_shard_count.begin(),
                                       rebalanced_shard_count.end(),
                                       [](const auto &a, const auto &b) { return a.second < b.second; });

    uint32_t min_shards = min_max.first->second;
    uint32_t max_shards = min_max.second->second;
    uint32_t expected_avg = FLAGS_shard_total_num / (kDataserverNum - kFailureNum);

    MLOG_INFO("Shard distribution: min={}, max={}, expected_avg={}", min_shards, max_shards, expected_avg);

    EXPECT_LE(max_shards - min_shards, expected_avg / 2 + 1);
  }

  MLOG_INFO("Multiple node failure test completed successfully");

  // Clean up
  for (size_t i = kFailureNum; i < dataservers.size(); ++i) {
    dataservers[i]->Stop();
  }

  test_completed.store(true);
  cm_done.wait();
  executor.join();
}

TEST_F(ClusterManagerRebalanceTest, TestShardRebalanceOnlyOnce) {
  constexpr uint32_t kDataserverNum = 4;
  const std::string ip_prefix = "192.168.0."; // TODO: remove this?
  const int base_port = 40000;

  std::atomic<bool> cm_started{false};
  std::atomic<bool> test_completed{false};
  folly::Baton<> cm_done;
  folly::CPUThreadPoolExecutor executor(8);

  auto shard_manager_ptr = std::make_shared<simm::cm::ClusterManagerShardManager>();
  auto node_manager_ptr = std::make_shared<simm::cm::ClusterManagerNodeManager>();
  auto hb_monitor_ptr = std::make_shared<simm::cm::ClusterManagerHBMonitor>(node_manager_ptr, shard_manager_ptr);
  auto cm_service_ptr =
      std::make_unique<simm::cm::ClusterManagerService>(shard_manager_ptr, node_manager_ptr, hb_monitor_ptr);

  std::vector<std::unique_ptr<MockDataServer>> dataservers;
  for (uint32_t i = 0; i < kDataserverNum; ++i) {
    std::string ip = ip_prefix + std::to_string(i + 1);
    int port = base_port + i;
    auto ds = std::make_unique<MockDataServer>(ip, port);
    dataservers.push_back(std::move(ds));
  }

  // CM thread
  auto cm_thread = [&]() {
    MLOG_INFO("Starting CM service...");

    simm::common::ModuleServiceState::GetInstance().Reset(FLAGS_cm_cluster_init_grace_period_inSecs);
    
    // Should pre-init DS to avoid TooFew error
    std::vector<std::shared_ptr<simm::common::NodeAddress>> pre_init_servers;
    for (uint32_t i = 0; i < kDataserverNum; ++i) {
      std::string ip = ip_prefix + std::to_string(i + 1);
      int port = base_port + i;
      auto server_addr = std::make_shared<simm::common::NodeAddress>(ip, port);
      pre_init_servers.push_back(server_addr);
      node_manager_ptr->AddNode(server_addr->toString());
    }

    auto ret = cm_service_ptr->Init();
    EXPECT_EQ(ret, CommonErr::OK);

    ret = shard_manager_ptr->InitShardRoutingTable(pre_init_servers);
    EXPECT_EQ(ret, CommonErr::OK);

    ret = cm_service_ptr->Start();
    EXPECT_EQ(ret, CommonErr::OK);
    EXPECT_TRUE(cm_service_ptr->IsRunning());

    cm_started.store(true);
    MLOG_INFO("CM service started successfully");

    // Wait all tests
    while (!test_completed.load()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    MLOG_INFO("Stopping CM service...");
    ret = cm_service_ptr->Stop();
    EXPECT_EQ(ret, CommonErr::OK);
    MLOG_INFO("CM service stopped");

    cm_done.post();
  };

  executor.add(cm_thread);

  while (!cm_started.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  MLOG_INFO("Starting {} DataServers...", kDataserverNum);
  for (auto &ds : dataservers) {
    auto ret = ds->Start();
    EXPECT_EQ(ret, CommonErr::OK);
  }

  MLOG_INFO("Waiting for cluster initialization...");
  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_cm_cluster_init_grace_period_inSecs + 2));

  auto all_nodes = node_manager_ptr->GetAllNodeAddress(true);
  EXPECT_EQ(all_nodes.size(), kDataserverNum);
  MLOG_INFO("All {} DataServers registered successfully", all_nodes.size());

  auto initial_routing = shard_manager_ptr->QueryAllShardRoutingInfos();
  EXPECT_EQ(initial_routing.size(), FLAGS_shard_total_num);

  std::unordered_map<std::string, uint32_t> initial_shard_count;
  for (const auto &entry : initial_routing) {
    if (entry.second) {
      std::string node_addr = entry.second->toString();
      initial_shard_count[node_addr]++;
    }
  }

  MLOG_INFO("Initial shard distribution:");
  for (const auto &entry : initial_shard_count) {
    MLOG_INFO("  Node {}: {} shards", entry.first, entry.second);
  }

  // Shutdown one ds
  MLOG_INFO("Simulating failure of DataServer: {}", dataservers[0]->GetAddressString());
  std::string failed_node_addr = dataservers[0]->GetAddressString();
  auto ret = dataservers[0]->Stop();
  EXPECT_EQ(ret, CommonErr::OK);

  MLOG_INFO("Waiting for heartbeat timeout and rebalance...");
  std::this_thread::sleep_for(
      std::chrono::seconds(FLAGS_cm_heartbeat_timeout_inSecs + FLAGS_cm_heartbeat_bg_scan_interval_inSecs + 2));

  MLOG_INFO("Verifying rebalance results...");

  // Alive nodes num
  auto alive_nodes = node_manager_ptr->GetAllNodeAddress(true);
  EXPECT_EQ(alive_nodes.size(), kDataserverNum - 1);
  MLOG_INFO("Alive nodes after failure: {}", alive_nodes.size());

  auto rebalanced_routing = shard_manager_ptr->QueryAllShardRoutingInfos();
  EXPECT_EQ(rebalanced_routing.size(), FLAGS_shard_total_num);

  std::unordered_map<std::string, uint32_t> rebalanced_shard_count;
  std::unordered_set<std::string> nodes_with_shards;

  for (const auto &entry : rebalanced_routing) {
    if (entry.second) {
      std::string node_addr = entry.second->toString();
      rebalanced_shard_count[node_addr]++;
      nodes_with_shards.insert(node_addr);

      // No shards assigned to failed nodes
      EXPECT_NE(node_addr, failed_node_addr)
          << "Found shard " << entry.first << " still assigned to failed node " << failed_node_addr;
    }
  }

  MLOG_INFO("Rebalanced shard distribution:");
  for (const auto &entry : rebalanced_shard_count) {
    MLOG_INFO("  Node {}: {} shards", entry.first, entry.second);
  }

  // Alive nodes should have shards assigned
  EXPECT_EQ(nodes_with_shards.size(), kDataserverNum - 1);

  // Total shards should remain the same
  uint32_t total_shards = 0;
  for (const auto &entry : rebalanced_shard_count) {
    total_shards += entry.second;
  }
  EXPECT_EQ(total_shards, FLAGS_shard_total_num);

  // TODO: Used average as assertion, better logic? or change if algorithm changes.
  if (!rebalanced_shard_count.empty()) {
    auto min_max = std::minmax_element(rebalanced_shard_count.begin(),
                                       rebalanced_shard_count.end(),
                                       [](const auto &a, const auto &b) { return a.second < b.second; });

    uint32_t min_shards = min_max.first->second;
    uint32_t max_shards = min_max.second->second;
    uint32_t expected_avg = FLAGS_shard_total_num / (kDataserverNum - 1);

    MLOG_INFO("Shard distribution: min={}, max={}, expected_avg={}", min_shards, max_shards, expected_avg);

    EXPECT_LE(max_shards - min_shards, expected_avg / 2 + 1);
  }

  MLOG_INFO("Rebalance test completed successfully");

  // Sleep 2xhb time
  std::this_thread::sleep_for(std::chrono::seconds((FLAGS_cm_heartbeat_timeout_inSecs + FLAGS_cm_heartbeat_bg_scan_interval_inSecs) * 2));

  // Clean up
  for (size_t i = 1; i < dataservers.size(); ++i) {
    dataservers[i]->Stop();
  }

  test_completed.store(true);

  cm_done.wait();

  executor.join();
}

}  // namespace cm
}  // namespace simm

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}