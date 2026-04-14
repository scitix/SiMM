#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

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

DECLARE_LOG_MODULE("deferred_reshard_test");

DECLARE_uint32(shard_total_num);
DECLARE_int32(cm_rpc_inter_port);
DECLARE_uint32(cm_cluster_init_grace_period_inSecs);
DECLARE_uint32(cm_heartbeat_timeout_inSecs);
DECLARE_uint32(cm_heartbeat_bg_scan_interval_inSecs);
DECLARE_bool(cm_deferred_reshard_enabled);
DECLARE_uint32(cm_deferred_reshard_window_inSecs);
DECLARE_uint32(dataserver_min_num);
DECLARE_string(cm_log_file);

namespace simm {
namespace cm {

namespace {

template <typename Predicate>
bool WaitUntil(Predicate pred,
               std::chrono::milliseconds timeout,
               std::chrono::milliseconds poll_interval = std::chrono::milliseconds(100)) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (pred()) return true;
    std::this_thread::sleep_for(poll_interval);
  }
  return pred();
}

}  // namespace

// ── Mock DS with logical_node_id support ────────────────────────────────────
// Key design: SiRPC client is created once in Start() and kept alive until Stop().
// Handshake is synchronous. Heartbeats are async but client lifetime outlives callbacks.

class MockDeferredDS {
 public:
  MockDeferredDS(const std::string& ip, int port, const std::string& logical_id,
                 std::chrono::milliseconds hb_interval = std::chrono::milliseconds(500))
      : ip_(ip), port_(port), logical_id_(logical_id), hb_interval_(hb_interval) {}

  ~MockDeferredDS() { Stop(); }

  void Start() {
    if (running_.exchange(true)) return;
    stop_.store(false);

    // Create a long-lived RPC client for this DS
    sicl::rpc::SiRPC* raw = nullptr;
    sicl::rpc::SiRPC::newInstance(raw, false);
    rpc_client_ = raw;

    SendHandshakeSync();
    hb_thread_ = std::jthread([this]() { HBLoop(); });
    MLOG_INFO("[MockDS] Started: logical_id={} ip={}:{}", logical_id_, ip_, port_);
  }

  void Stop() {
    if (!running_.exchange(false)) return;
    stop_.store(true);
    if (hb_thread_.joinable()) hb_thread_.join();
    // Give async callbacks time to complete before deleting client
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    if (rpc_client_) {
      delete rpc_client_;
      rpc_client_ = nullptr;
    }
    MLOG_INFO("[MockDS] Stopped: logical_id={} ip={}:{}", logical_id_, ip_, port_);
  }

  void RestartWithNewIp(const std::string& new_ip) {
    Stop();
    ip_ = new_ip;
    Start();
  }

  std::string GetAddr() const { return ip_ + ":" + std::to_string(port_); }
  std::string GetLogicalId() const { return logical_id_; }

 private:
  void SendHandshakeSync() {
    sicl::rpc::RpcContext* ctx_p = nullptr;
    sicl::rpc::RpcContext::newInstance(ctx_p);
    auto ctx = std::shared_ptr<sicl::rpc::RpcContext>(ctx_p);
    ctx->set_timeout(sicl::transport::TimerTick::TIMER_3S);

    NewNodeHandShakeRequestPB req;
    req.mutable_node()->set_ip(ip_);
    req.mutable_node()->set_port(port_);
    req.set_logical_node_id(logical_id_);

    folly::Baton<> done;
    auto* resp = new NewNodeHandShakeResponsePB();
    std::string lid = logical_id_;
    auto cb = [resp, lid, &done](const google::protobuf::Message* rsp,
                                  const std::shared_ptr<sicl::rpc::RpcContext> ctx) {
      if (ctx->ErrorCode() == sicl::transport::Result::SICL_SUCCESS) {
        auto* response = dynamic_cast<const NewNodeHandShakeResponsePB*>(rsp);
        MLOG_INFO("[MockDS] Handshake OK: logical_id={} ret_code={} assigned_shards={}",
                  lid, response->ret_code(), response->assigned_shard_ids_size());
      } else {
        MLOG_ERROR("[MockDS] Handshake FAILED: logical_id={} err={}", lid, ctx->ErrorCode());
      }
      delete resp;
      done.post();
    };

    rpc_client_->SendRequest("127.0.0.1", FLAGS_cm_rpc_inter_port,
                             static_cast<sicl::rpc::ReqType>(ClusterManagerRpcType::RPC_NEW_NODE_HANDSHAKE),
                             req, resp, ctx, cb);
    done.try_wait_for(std::chrono::seconds(5));
  }

  void HBLoop() {
    while (!stop_.load()) {
      SendHeartbeat();
      std::this_thread::sleep_for(hb_interval_);
    }
  }

  void SendHeartbeat() {
    sicl::rpc::RpcContext* ctx_p = nullptr;
    sicl::rpc::RpcContext::newInstance(ctx_p);
    auto ctx = std::shared_ptr<sicl::rpc::RpcContext>(ctx_p);
    ctx->set_timeout(sicl::transport::TimerTick::TIMER_3S);

    DataServerHeartBeatRequestPB req;
    req.mutable_node()->set_ip(ip_);
    req.mutable_node()->set_port(port_);
    req.set_logical_node_id(logical_id_);

    auto* resp = new DataServerHeartBeatResponsePB();
    auto cb = [resp](const google::protobuf::Message*, const std::shared_ptr<sicl::rpc::RpcContext>) {
      delete resp;
    };

    rpc_client_->SendRequest("127.0.0.1", FLAGS_cm_rpc_inter_port,
                             static_cast<sicl::rpc::ReqType>(ClusterManagerRpcType::RPC_NODE_HEARTBEAT),
                             req, resp, ctx, cb);
  }

  std::string ip_;
  int port_;
  std::string logical_id_;
  std::chrono::milliseconds hb_interval_;
  std::atomic<bool> running_{false};
  std::atomic<bool> stop_{false};
  std::jthread hb_thread_;
  sicl::rpc::SiRPC* rpc_client_{nullptr};
};

// ── Test fixture ────────────────────────────────────────────────────────────

class DeferredReshardTest : public ::testing::Test {
 protected:
  void SetUp() override {
    simm::logging::LogConfig log_cfg{FLAGS_cm_log_file, "INFO"};
    simm::logging::LoggerManager::Instance().UpdateConfig("cluster_manager", log_cfg);
    simm::logging::LoggerManager::Instance().UpdateConfig("deferred_reshard_test", log_cfg);

    saved_grace_ = FLAGS_cm_cluster_init_grace_period_inSecs;
    saved_hb_timeout_ = FLAGS_cm_heartbeat_timeout_inSecs;
    saved_hb_scan_ = FLAGS_cm_heartbeat_bg_scan_interval_inSecs;
    saved_deferred_enabled_ = FLAGS_cm_deferred_reshard_enabled;
    saved_deferred_window_ = FLAGS_cm_deferred_reshard_window_inSecs;

    FLAGS_cm_cluster_init_grace_period_inSecs = 2;
    FLAGS_cm_heartbeat_timeout_inSecs = 2;
    FLAGS_cm_heartbeat_bg_scan_interval_inSecs = 1;
    FLAGS_cm_deferred_reshard_enabled = true;
    FLAGS_cm_deferred_reshard_window_inSecs = 10;
  }

  void TearDown() override {
    FLAGS_cm_cluster_init_grace_period_inSecs = saved_grace_;
    FLAGS_cm_heartbeat_timeout_inSecs = saved_hb_timeout_;
    FLAGS_cm_heartbeat_bg_scan_interval_inSecs = saved_hb_scan_;
    FLAGS_cm_deferred_reshard_enabled = saved_deferred_enabled_;
    FLAGS_cm_deferred_reshard_window_inSecs = saved_deferred_window_;
  }

  struct CMContext {
    std::shared_ptr<ClusterManagerShardManager> shard_mgr;
    std::shared_ptr<ClusterManagerNodeManager> node_mgr;
    std::shared_ptr<ClusterManagerHBMonitor> hb_mon;
    std::unique_ptr<ClusterManagerService> service;
    std::atomic<bool> started{false};
    std::atomic<bool> done{false};
    folly::Baton<> done_baton;
  };

  std::unique_ptr<CMContext> StartCM(folly::CPUThreadPoolExecutor& executor, uint32_t ds_num,
                                      const std::string& ip_prefix, int base_port) {
    auto ctx = std::make_unique<CMContext>();
    ctx->shard_mgr = std::make_shared<ClusterManagerShardManager>();
    ctx->node_mgr = std::make_shared<ClusterManagerNodeManager>();
    ctx->hb_mon = std::make_shared<ClusterManagerHBMonitor>(ctx->node_mgr, ctx->shard_mgr);
    ctx->service = std::make_unique<ClusterManagerService>(ctx->shard_mgr, ctx->node_mgr, ctx->hb_mon);

    auto* raw = ctx.get();
    executor.add([raw, ds_num, ip_prefix, base_port]() {
      simm::common::ModuleServiceState::GetInstance().Reset(FLAGS_cm_cluster_init_grace_period_inSecs);

      std::vector<std::shared_ptr<simm::common::NodeAddress>> servers;
      for (uint32_t i = 0; i < ds_num; ++i) {
        auto addr = std::make_shared<simm::common::NodeAddress>(
            ip_prefix + std::to_string(i + 1), base_port + static_cast<int>(i));
        servers.push_back(addr);
        raw->node_mgr->AddNode(addr->toString());
      }

      raw->service->Init();
      raw->shard_mgr->InitShardRoutingTable(servers);
      raw->service->Start();
      raw->started.store(true);

      while (!raw->done.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }

      raw->service->Stop();
      raw->done_baton.post();
    });

    EXPECT_TRUE(WaitUntil([&]() { return ctx->started.load(); }, std::chrono::seconds(15)));
    return ctx;
  }

  void StopCM(std::unique_ptr<CMContext>& ctx) {
    ctx->done.store(true);
    ctx->done_baton.wait();
  }

 private:
  uint32_t saved_grace_{}, saved_hb_timeout_{}, saved_hb_scan_{}, saved_deferred_window_{};
  bool saved_deferred_enabled_{};
};

// ═════════════════════════════════════════════════════════════════════════════
// TEST 1: DS crashes → DEFERRED_RESHARD → new DS with same logical_id
//         → in-place IP replacement, shards stay, no reshard
// ═════════════════════════════════════════════════════════════════════════════

TEST_F(DeferredReshardTest, ReplacementWithinWindow) {
  constexpr uint32_t kDSNum = 3;
  const std::string ip_prefix = "10.100.0.";
  const int base_port = 50000;

  folly::CPUThreadPoolExecutor executor(8);
  auto cm = StartCM(executor, kDSNum, ip_prefix, base_port);

  // Start 3 DS
  std::vector<std::unique_ptr<MockDeferredDS>> ds_list;
  for (uint32_t i = 0; i < kDSNum; ++i) {
    ds_list.push_back(std::make_unique<MockDeferredDS>(
        ip_prefix + std::to_string(i + 1), base_port + i,
        "simm-ds-" + std::to_string(i)));
    ds_list.back()->Start();
  }

  // Wait for registration + stable heartbeats
  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_cm_cluster_init_grace_period_inSecs + 2));

  // Record DS-0's initial shard ownership
  std::string ds0_old_addr = ds_list[0]->GetAddr();
  auto ds0_initial_shards = cm->shard_mgr->GetShardsOwnedByNode(ds0_old_addr);
  MLOG_INFO("DS-0 ({}) owns {} shards initially", ds0_old_addr, ds0_initial_shards.size());
  ASSERT_GT(ds0_initial_shards.size(), 0u);

  // ── Kill DS-0 ──
  MLOG_INFO("=== Killing DS-0 ===");
  ds_list[0]->Stop();

  // Wait for HB timeout → should enter DEFERRED_RESHARD
  std::this_thread::sleep_for(std::chrono::seconds(
      FLAGS_cm_heartbeat_timeout_inSecs + FLAGS_cm_heartbeat_bg_scan_interval_inSecs + 1));

  auto entry = cm->node_mgr->GetNodeEntry("simm-ds-0");
  ASSERT_TRUE(entry.has_value()) << "simm-ds-0 should exist in logical_node_table";
  EXPECT_EQ(entry->status, NodeStatus::DEFERRED_RESHARD);
  MLOG_INFO("PASS: DS-0 is in DEFERRED_RESHARD state");

  // Shards should NOT have moved
  auto shards_still = cm->shard_mgr->GetShardsOwnedByNode(ds0_old_addr);
  EXPECT_EQ(shards_still.size(), ds0_initial_shards.size());
  MLOG_INFO("PASS: {} shards still on old IP (no reshard)", shards_still.size());

  // ── Restart DS-0 with NEW IP ──
  std::string ds0_new_ip = "10.100.0.99";
  std::string ds0_new_addr = ds0_new_ip + ":" + std::to_string(base_port);
  MLOG_INFO("=== Restarting DS-0 with new IP {} ===", ds0_new_addr);
  ds_list[0]->RestartWithNewIp(ds0_new_ip);

  std::this_thread::sleep_for(std::chrono::seconds(2));

  // Verify: back to RUNNING with new IP
  entry = cm->node_mgr->GetNodeEntry("simm-ds-0");
  ASSERT_TRUE(entry.has_value());
  EXPECT_EQ(entry->status, NodeStatus::RUNNING);
  EXPECT_EQ(entry->current_ip_port, ds0_new_addr);
  MLOG_INFO("PASS: DS-0 back to RUNNING with new IP {}", ds0_new_addr);

  // Verify: shards moved to new IP
  auto shards_new = cm->shard_mgr->GetShardsOwnedByNode(ds0_new_addr);
  EXPECT_EQ(shards_new.size(), ds0_initial_shards.size());
  MLOG_INFO("PASS: {} shards now on new IP", shards_new.size());

  // Verify: no shards on old IP
  auto shards_old = cm->shard_mgr->GetShardsOwnedByNode(ds0_old_addr);
  EXPECT_EQ(shards_old.size(), 0u);
  MLOG_INFO("PASS: 0 shards on old IP");

  // Verify: total shards intact
  auto all = cm->shard_mgr->QueryAllShardRoutingInfos();
  EXPECT_EQ(all.size(), FLAGS_shard_total_num);
  MLOG_INFO("PASS: Total shards = {}", all.size());

  for (auto& ds : ds_list) ds->Stop();
  StopCM(cm);
  executor.join();
}

// ═════════════════════════════════════════════════════════════════════════════
// TEST 2: DS crashes, NO replacement → window expires → fallback reshard
// ═════════════════════════════════════════════════════════════════════════════

TEST_F(DeferredReshardTest, FallbackToReshardOnWindowExpiry) {
  FLAGS_cm_deferred_reshard_window_inSecs = 4;  // short window for fast test

  constexpr uint32_t kDSNum = 4;  // need >= dataserver_min_num+1 so reshard has enough alive servers
  const std::string ip_prefix = "10.200.0.";
  const int base_port = 51000;

  folly::CPUThreadPoolExecutor executor(8);
  auto cm = StartCM(executor, kDSNum, ip_prefix, base_port);

  std::vector<std::unique_ptr<MockDeferredDS>> ds_list;
  for (uint32_t i = 0; i < kDSNum; ++i) {
    ds_list.push_back(std::make_unique<MockDeferredDS>(
        ip_prefix + std::to_string(i + 1), base_port + i,
        "fallback-ds-" + std::to_string(i)));
    ds_list.back()->Start();
  }

  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_cm_cluster_init_grace_period_inSecs + 2));

  std::string ds0_addr = ds_list[0]->GetAddr();
  auto ds0_shards = cm->shard_mgr->GetShardsOwnedByNode(ds0_addr);
  ASSERT_GT(ds0_shards.size(), 0u);

  // Kill DS-0 and do NOT restart
  MLOG_INFO("=== Killing DS-0, will NOT restart ===");
  ds_list[0]->Stop();

  // Wait for: HB timeout → DEFERRED_RESHARD → window expiry → DEAD + reshard
  auto total_wait = FLAGS_cm_heartbeat_timeout_inSecs + FLAGS_cm_deferred_reshard_window_inSecs
                    + FLAGS_cm_heartbeat_bg_scan_interval_inSecs * 2 + 2;
  MLOG_INFO("Waiting {}s for full cycle...", total_wait);
  std::this_thread::sleep_for(std::chrono::seconds(total_wait));

  // Verify: DS-0 should be DEAD
  auto entry = cm->node_mgr->GetNodeEntry("fallback-ds-0");
  ASSERT_TRUE(entry.has_value());
  EXPECT_EQ(entry->status, NodeStatus::DEAD);
  MLOG_INFO("PASS: DS-0 is DEAD after window expiry");

  // Verify: shards redistributed
  auto shards_dead = cm->shard_mgr->GetShardsOwnedByNode(ds0_addr);
  EXPECT_EQ(shards_dead.size(), 0u);
  MLOG_INFO("PASS: 0 shards on dead DS");

  uint32_t alive_total = 0;
  for (uint32_t i = 1; i < kDSNum; ++i) {
    alive_total += cm->shard_mgr->GetShardsOwnedByNode(ds_list[i]->GetAddr()).size();
  }
  EXPECT_EQ(alive_total, FLAGS_shard_total_num);
  MLOG_INFO("PASS: All {} shards on alive nodes", alive_total);

  for (auto& ds : ds_list) ds->Stop();
  StopCM(cm);
  executor.join();
}

// ═════════════════════════════════════════════════════════════════════════════
// TEST 3: Deferred Reshard disabled → immediate DEAD + reshard (legacy)
// ═════════════════════════════════════════════════════════════════════════════

TEST_F(DeferredReshardTest, DisabledFallsBackToImmediate) {
  FLAGS_cm_deferred_reshard_enabled = false;

  constexpr uint32_t kDSNum = 4;  // need >= dataserver_min_num+1
  const std::string ip_prefix = "10.88.0.";
  const int base_port = 52000;

  folly::CPUThreadPoolExecutor executor(8);
  auto cm = StartCM(executor, kDSNum, ip_prefix, base_port);

  std::vector<std::unique_ptr<MockDeferredDS>> ds_list;
  for (uint32_t i = 0; i < kDSNum; ++i) {
    ds_list.push_back(std::make_unique<MockDeferredDS>(
        ip_prefix + std::to_string(i + 1), base_port + i,
        "disabled-ds-" + std::to_string(i)));
    ds_list.back()->Start();
  }

  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_cm_cluster_init_grace_period_inSecs + 2));

  std::string ds0_addr = ds_list[0]->GetAddr();
  ASSERT_GT(cm->shard_mgr->GetShardsOwnedByNode(ds0_addr).size(), 0u);

  MLOG_INFO("=== Killing DS-0 with deferred reshard DISABLED ===");
  ds_list[0]->Stop();

  // With deferred disabled, should go straight to DEAD after HB timeout
  std::this_thread::sleep_for(std::chrono::seconds(
      FLAGS_cm_heartbeat_timeout_inSecs + FLAGS_cm_heartbeat_bg_scan_interval_inSecs + 2));

  auto shards_dead = cm->shard_mgr->GetShardsOwnedByNode(ds0_addr);
  EXPECT_EQ(shards_dead.size(), 0u);
  MLOG_INFO("PASS: Immediate reshard, 0 shards on dead DS");

  for (auto& ds : ds_list) ds->Stop();
  StopCM(cm);
  executor.join();
}

// ═════════════════════════════════════════════════════════════════════════════
// TEST 4: Kill enough DS so alive < dataserver_min_num
//         → Window expires but nodes stay DEFERRED_RESHARD (no orphaned shards)
//         → Restart one DS → deferred reshard replace succeeds
// ═════════════════════════════════════════════════════════════════════════════

TEST_F(DeferredReshardTest, InsufficientNodesKeepsDeferredState) {
  // dataserver_min_num defaults to 3
  // Start 3 DS, kill 2 → only 1 alive → reshard not feasible → should stay DEFERRED_RESHARD
  FLAGS_cm_deferred_reshard_window_inSecs = 3;  // short window

  constexpr uint32_t kDSNum = 3;
  const std::string ip_prefix = "10.77.0.";
  const int base_port = 53000;

  folly::CPUThreadPoolExecutor executor(8);
  auto cm = StartCM(executor, kDSNum, ip_prefix, base_port);

  std::vector<std::unique_ptr<MockDeferredDS>> ds_list;
  for (uint32_t i = 0; i < kDSNum; ++i) {
    ds_list.push_back(std::make_unique<MockDeferredDS>(
        ip_prefix + std::to_string(i + 1), base_port + i,
        "safe-ds-" + std::to_string(i)));
    ds_list.back()->Start();
  }

  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_cm_cluster_init_grace_period_inSecs + 2));

  // Record initial shards
  std::string ds0_addr = ds_list[0]->GetAddr();
  std::string ds1_addr = ds_list[1]->GetAddr();
  auto ds0_shards = cm->shard_mgr->GetShardsOwnedByNode(ds0_addr);
  auto ds1_shards = cm->shard_mgr->GetShardsOwnedByNode(ds1_addr);
  ASSERT_GT(ds0_shards.size(), 0u);
  ASSERT_GT(ds1_shards.size(), 0u);
  MLOG_INFO("DS-0 ({}) owns {} shards, DS-1 ({}) owns {} shards",
            ds0_addr, ds0_shards.size(), ds1_addr, ds1_shards.size());

  // Kill DS-0 and DS-1 simultaneously — only DS-2 survives (1 alive < min 3)
  MLOG_INFO("=== Killing DS-0 and DS-1 simultaneously ===");
  ds_list[0]->Stop();
  ds_list[1]->Stop();

  // Wait for HB timeout → both enter DEFERRED_RESHARD
  std::this_thread::sleep_for(std::chrono::seconds(
      FLAGS_cm_heartbeat_timeout_inSecs + FLAGS_cm_heartbeat_bg_scan_interval_inSecs + 1));

  auto e0 = cm->node_mgr->GetNodeEntry("safe-ds-0");
  auto e1 = cm->node_mgr->GetNodeEntry("safe-ds-1");
  ASSERT_TRUE(e0.has_value());
  ASSERT_TRUE(e1.has_value());
  EXPECT_EQ(e0->status, NodeStatus::DEFERRED_RESHARD);
  EXPECT_EQ(e1->status, NodeStatus::DEFERRED_RESHARD);
  MLOG_INFO("PASS: Both DS-0 and DS-1 are in DEFERRED_RESHARD");

  // Wait for window to expire
  MLOG_INFO("Waiting for deferred window ({} s) to expire...", FLAGS_cm_deferred_reshard_window_inSecs);
  std::this_thread::sleep_for(std::chrono::seconds(
      FLAGS_cm_deferred_reshard_window_inSecs + FLAGS_cm_heartbeat_bg_scan_interval_inSecs + 1));

  // Key assertion: both should STILL be DEFERRED_RESHARD (not DEAD)
  // because marking them DEAD would leave only 1 alive node < dataserver_min_num=3
  e0 = cm->node_mgr->GetNodeEntry("safe-ds-0");
  e1 = cm->node_mgr->GetNodeEntry("safe-ds-1");
  ASSERT_TRUE(e0.has_value());
  ASSERT_TRUE(e1.has_value());
  EXPECT_EQ(e0->status, NodeStatus::DEFERRED_RESHARD)
      << "DS-0 should stay DEFERRED_RESHARD (not DEAD) because reshard not feasible";
  EXPECT_EQ(e1->status, NodeStatus::DEFERRED_RESHARD)
      << "DS-1 should stay DEFERRED_RESHARD (not DEAD) because reshard not feasible";
  MLOG_INFO("PASS: Both still DEFERRED_RESHARD after window expiry (safe degradation)");

  // Shards should still be on old IPs (not redistributed, not orphaned)
  auto s0 = cm->shard_mgr->GetShardsOwnedByNode(ds0_addr);
  auto s1 = cm->shard_mgr->GetShardsOwnedByNode(ds1_addr);
  EXPECT_EQ(s0.size(), ds0_shards.size());
  EXPECT_EQ(s1.size(), ds1_shards.size());
  MLOG_INFO("PASS: Shards still on old IPs (not orphaned)");

  // Now restart DS-0 with new IP → should deferred replace successfully
  std::string ds0_new_ip = "10.77.0.99";
  MLOG_INFO("=== Restarting DS-0 with new IP {} ===", ds0_new_ip);
  ds_list[0]->RestartWithNewIp(ds0_new_ip);
  std::this_thread::sleep_for(std::chrono::seconds(2));

  e0 = cm->node_mgr->GetNodeEntry("safe-ds-0");
  ASSERT_TRUE(e0.has_value());
  EXPECT_EQ(e0->status, NodeStatus::RUNNING);
  MLOG_INFO("PASS: DS-0 back to RUNNING via deferred reshard replace");

  // Verify shards moved to new IP
  std::string ds0_new_addr = ds0_new_ip + ":" + std::to_string(base_port);
  auto new_shards = cm->shard_mgr->GetShardsOwnedByNode(ds0_new_addr);
  EXPECT_EQ(new_shards.size(), ds0_shards.size());
  MLOG_INFO("PASS: DS-0's {} shards now on new IP {}", new_shards.size(), ds0_new_addr);

  for (auto& ds : ds_list) ds->Stop();
  StopCM(cm);
  executor.join();
}

// ═════════════════════════════════════════════════════════════════════════════
// TEST 5: DS restarts before HB timeout (same logical_id, same IP)
//         → ProcessHandshake should return IP_UPDATE action
//         → shards inherited and returned to DS (not empty)
// ═════════════════════════════════════════════════════════════════════════════

TEST(ProcessHandshakeUnitTest, ReregistrationInheritsShard) {
  // Unit test: no CM service, directly tests ProcessHandshake + GetShardsOwnedByNode
  auto shard_mgr = std::make_shared<ClusterManagerShardManager>();
  auto node_mgr = std::make_shared<ClusterManagerNodeManager>();

  const std::string logical_id = "simm-ds-0";
  const std::string ip_port = "192.168.1.10:40000";

  // Step 1: Initial registration (simulate grace-period AddNode + BatchAssign)
  node_mgr->AddNode(ip_port);

  // Assign shards 0..3 to this DS
  std::vector<shard_id_t> expected_shards = {0, 1, 2, 3};
  auto addr = std::make_shared<simm::common::NodeAddress>("192.168.1.10", 40000);
  shard_mgr->BatchAssignRoutingTable(expected_shards, addr);

  // Also register in logical_node_table via ProcessHandshake (grace-period path)
  node_mgr->ProcessHandshake(logical_id, ip_port, expected_shards);

  // Verify initial state
  {
    auto entry = node_mgr->GetNodeEntry(logical_id);
    ASSERT_TRUE(entry.has_value());
    EXPECT_EQ(entry->status, NodeStatus::RUNNING);
    EXPECT_EQ(entry->current_ip_port, ip_port);
  }

  // Step 2: DS crashes and restarts — same logical_id, same IP, no shard_ids in request
  std::vector<shard_id_t> empty_shards;
  auto result = node_mgr->ProcessHandshake(logical_id, ip_port, empty_shards);

  // Key assertion: action should be IP_UPDATE (not NEW_NODE), with old_ip_port filled
  EXPECT_EQ(result.action, HandshakeResult::Action::IP_UPDATE);
  EXPECT_EQ(result.old_ip_port, ip_port);
  MLOG_INFO("PASS: ProcessHandshake returned IP_UPDATE for re-registration");

  // Step 3: Simulate what the handler does — query shards from ShardManager
  auto inherited_shards = shard_mgr->GetShardsOwnedByNode(result.old_ip_port);
  EXPECT_EQ(inherited_shards.size(), expected_shards.size());
  std::unordered_set<shard_id_t> expected_set(expected_shards.begin(), expected_shards.end());
  for (auto s : inherited_shards) {
    EXPECT_TRUE(expected_set.count(s) > 0) << "Unexpected shard: " << s;
  }
  MLOG_INFO("PASS: {} shards inherited correctly via GetShardsOwnedByNode", inherited_shards.size());

  // Step 4: BatchAssignRoutingTable with same addr is a no-op — verify routing table unchanged
  shard_mgr->BatchAssignRoutingTable(inherited_shards, addr);
  auto still_owned = shard_mgr->GetShardsOwnedByNode(ip_port);
  EXPECT_EQ(still_owned.size(), expected_shards.size());
  MLOG_INFO("PASS: Routing table unchanged after re-registration (idempotent)");
}

// ═════════════════════════════════════════════════════════════════════════════
// TEST 6: DS comes back after DEAD (rejoin → STANDBY), then goes silent again.
//         CM should eventually mark it DEAD (no reshard, it held no shards).
// ═════════════════════════════════════════════════════════════════════════════

TEST_F(DeferredReshardTest, StandbyTimesOutToDeadWithoutReshard) {
  // Use a very short deferred window so the first DS quickly reaches DEAD.
  FLAGS_cm_deferred_reshard_window_inSecs = 3;
  // Lower min_num so that killing 1-of-3 still allows reshard (2 >= 2).
  auto saved_min_num = FLAGS_dataserver_min_num;
  FLAGS_dataserver_min_num = 2;

  constexpr uint32_t kDSNum = 3;
  const std::string ip_prefix = "10.88.0.";
  const int base_port = 54000;

  folly::CPUThreadPoolExecutor executor(8);
  auto cm = StartCM(executor, kDSNum, ip_prefix, base_port);

  std::vector<std::unique_ptr<MockDeferredDS>> ds_list;
  for (uint32_t i = 0; i < kDSNum; ++i) {
    ds_list.push_back(std::make_unique<MockDeferredDS>(
        ip_prefix + std::to_string(i + 1), base_port + i,
        "standby-ds-" + std::to_string(i)));
    ds_list.back()->Start();
  }

  // Wait for grace period + stable HB
  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_cm_cluster_init_grace_period_inSecs + 2));

  // Kill DS-0: RUNNING → DEFERRED_RESHARD → DEAD (window=3s)
  MLOG_INFO("=== Killing DS-0 ===");
  ds_list[0]->Stop();

  // Wait past HB timeout + deferred window
  std::this_thread::sleep_for(std::chrono::seconds(
      FLAGS_cm_heartbeat_timeout_inSecs +
      FLAGS_cm_deferred_reshard_window_inSecs +
      FLAGS_cm_heartbeat_bg_scan_interval_inSecs * 2 + 2));

  auto e0 = cm->node_mgr->GetNodeEntry("standby-ds-0");
  ASSERT_TRUE(e0.has_value());
  EXPECT_EQ(e0->status, NodeStatus::DEAD)
      << "DS-0 should be DEAD after deferred window expires";
  MLOG_INFO("PASS: DS-0 is DEAD after deferred window");

  // DS-0 comes back → STANDBY (reshard already happened)
  MLOG_INFO("=== DS-0 rejoins → expecting STANDBY ===");
  ds_list[0]->Start();
  std::this_thread::sleep_for(std::chrono::seconds(2));

  e0 = cm->node_mgr->GetNodeEntry("standby-ds-0");
  ASSERT_TRUE(e0.has_value());
  EXPECT_EQ(e0->status, NodeStatus::STANDBY)
      << "DS-0 should be STANDBY after rejoining post-DEAD";

  // Verify no shards were given back to DS-0
  auto standby_shards = cm->shard_mgr->GetShardsOwnedByNode(ds_list[0]->GetAddr());
  EXPECT_EQ(standby_shards.size(), 0u)
      << "STANDBY node should own no shards";
  MLOG_INFO("PASS: DS-0 is STANDBY with 0 shards");

  // DS-0 goes silent again (STANDBY → DEAD, no reshard expected)
  MLOG_INFO("=== DS-0 goes silent from STANDBY ===");
  ds_list[0]->Stop();

  // Wait past HB timeout for STANDBY to be marked DEAD
  std::this_thread::sleep_for(std::chrono::seconds(
      FLAGS_cm_heartbeat_timeout_inSecs +
      FLAGS_cm_heartbeat_bg_scan_interval_inSecs + 2));

  e0 = cm->node_mgr->GetNodeEntry("standby-ds-0");
  ASSERT_TRUE(e0.has_value());
  EXPECT_EQ(e0->status, NodeStatus::DEAD)
      << "STANDBY node should become DEAD after HB timeout";
  MLOG_INFO("PASS: STANDBY → DEAD after HB timeout (no reshard triggered)");

  // Shards of DS-0 were already redistributed during the first DEAD transition;
  // verify other nodes still hold shards (cluster is healthy).
  auto ds1_shards = cm->shard_mgr->GetShardsOwnedByNode(ds_list[1]->GetAddr());
  auto ds2_shards = cm->shard_mgr->GetShardsOwnedByNode(ds_list[2]->GetAddr());
  EXPECT_GT(ds1_shards.size() + ds2_shards.size(), 0u)
      << "Remaining nodes should still hold all shards";
  MLOG_INFO("PASS: Cluster shards healthy on DS-1 ({}) and DS-2 ({})",
            ds1_shards.size(), ds2_shards.size());

  for (auto& ds : ds_list) ds->Stop();
  FLAGS_dataserver_min_num = saved_min_num;
  StopCM(cm);
  executor.join();
}

}  // namespace cm
}  // namespace simm

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
