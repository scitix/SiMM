#include <chrono>
#include <latch>
#include <memory>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <folly/Random.h>
#include <folly/ScopeGuard.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/synchronization/Baton.h>

#include "cluster_manager/cm_shard_manager.h"
#include "cluster_manager/cm_hb_monitor.h"
#include "cluster_manager/cm_service.h"
#include "cluster_manager/cm_rpc_handler.h"
#include "cluster_manager/cm_node_manager.h"
#include "common/base/common_types.h"
#include "common/errcode/errcode_def.h"
#include "common/logging/logging.h"
#include "proto/cm_clnt_rpcs.pb.h"
#include "proto/ds_cm_rpcs.pb.h"

DECLARE_LOG_MODULE("cluster_manager");

DECLARE_int32(cm_rpc_inter_port);
DECLARE_uint32(cm_heartbeat_records_perserver);
DECLARE_uint32(cm_heartbeat_bg_scan_interval_inSecs);
DECLARE_uint32(cm_heartbeat_timeout_inSecs);
DECLARE_uint32(cm_cluster_init_grace_period_inSecs);
DECLARE_string(cm_log_file);

namespace simm {
namespace cm {

using CmSMPtr = std::shared_ptr<simm::cm::ClusterManagerShardManager>;
using CmHBPtr = std::shared_ptr<simm::cm::ClusterManagerHBMonitor>;
using CmNMPtr = std::shared_ptr<simm::cm::ClusterManagerNodeManager>;

class ClusterManagerHBMonitorTest : public ::testing::Test {
 protected:
  enum class caseType : int {
    OnRecvNodeHeartbeat = 1,
    bgHBScanLoop = 2
  };

  void SetUp() override {
    #ifdef NDEBUG
      simm::logging::LogConfig cm_log_config = simm::logging::LogConfig{FLAGS_cm_log_file, "INFO"};
    #else
      simm::logging::LogConfig cm_log_config = simm::logging::LogConfig{FLAGS_cm_log_file, "DEBUG"};
    #endif
      simm::logging::LoggerManager::Instance().UpdateConfig("cluster_manager", cm_log_config);

    cm_shard_manager_ptr_ = std::make_shared<simm::cm::ClusterManagerShardManager>();
    cm_node_manager_ptr_ = std::make_shared<simm::cm::ClusterManagerNodeManager>();
    cm_hb_monitor_ptr_ = std::make_shared<simm::cm::ClusterManagerHBMonitor>(cm_node_manager_ptr_, cm_shard_manager_ptr_);
    // cm_hb_monitor_ptr_->Init();
    // error_code_t ret = cm_hb_monitor_ptr_->Start();
    // EXPECT_EQ(ret, CommonErr::OK);
  }

  void TearDown() override {
    // error_code_t ret = cm_hb_monitor_ptr_->Stop();
    // EXPECT_EQ(ret, CommonErr::OK);
    cm_shard_manager_ptr_.reset();
    cm_hb_monitor_ptr_.reset();
    cm_node_manager_ptr_.reset();
  }

  CmSMPtr cm_shard_manager_ptr_{nullptr};
  CmHBPtr cm_hb_monitor_ptr_{nullptr};
  CmNMPtr cm_node_manager_ptr_{nullptr};
};

TEST_F(ClusterManagerHBMonitorTest, TestHBMonitor) {
  std::atomic<bool> serverStarted{false};
  std::atomic<bool> stopServer{false};
  const uint32_t kDataserverNum = 30; 
  const uint32_t kHBNumPerServer = 8;
  std::string ip_addr_prefix{"192.168.0."};
  int32_t server_port = 40000;

  // ------------------------------------------------------------------------
  // (1) Test ClusterManagerHBMonitor::OnRecvNodeHeartbeat() interface
  // ------------------------------------------------------------------------
  MLOG_INFO("Case to test Test ClusterManagerHBMonitor::OnRecvNodeHeartbeat()");
  folly::CPUThreadPoolExecutor executor1(kDataserverNum+2);
  // light-weight cv
  // post()  - signaled state
  // reset() - set to unsignaled state again
  folly::Baton<> serverDone;
  // 1 client thread(8 subclient threads) + 1 server thread
  uint32_t latch_num = kDataserverNum * kHBNumPerServer - 4;
  caseType ctype = caseType::OnRecvNodeHeartbeat;

  uint32_t old_flag_val_1 = FLAGS_cm_cluster_init_grace_period_inSecs;
  uint32_t old_flag_val_2 = FLAGS_cm_heartbeat_records_perserver;
  FLAGS_cm_cluster_init_grace_period_inSecs = 5; // shorten grace period in cm service start
  FLAGS_cm_heartbeat_records_perserver = 3;

  auto server_thread = [&]() {
    // start RPC service and wait for requests
    simm::common::ModuleServiceState::GetInstance().Reset(FLAGS_cm_cluster_init_grace_period_inSecs);
    MLOG_INFO("Mark cm into grace period, duration is {} seconds", FLAGS_cm_cluster_init_grace_period_inSecs);
    simm::cm::ClusterManagerService cm_service(cm_shard_manager_ptr_, this->cm_node_manager_ptr_, this->cm_hb_monitor_ptr_);
    error_code_t ret = cm_service.Init();
    EXPECT_EQ(ret, CommonErr::OK);
    ret = cm_service.Start();
    EXPECT_EQ(ret, CommonErr::OK);
    EXPECT_TRUE(cm_service.IsRunning());

    serverStarted.store(true);
    MLOG_INFO("Server thread started successfully!");

    while (!stopServer.load()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    MLOG_INFO("Server thread start to stop!");
    ret = cm_service.Stop();
    EXPECT_EQ(ret, CommonErr::OK);
    MLOG_INFO("Server thread stopped successfully!");

    serverStarted.store(false);
    serverDone.post();  // noitfy cv

    MLOG_INFO("Server thread exit!");
  };

  auto client_thread = [&](folly::CPUThreadPoolExecutor & executor) {
    // wait for server enters into grace period state
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    std::latch hb_done_latch{latch_num};
    std::latch hs_done_latch{kDataserverNum};

    auto client_sub_thread = [&](uint32_t thread_num) {
      // FIXME(ytji): use dataserver flags later instead of hard codes
      std::string server_addr_str = ip_addr_prefix + std::to_string(thread_num);

      // create sirpc object as rpc client
      sicl::rpc::SiRPC* sirpc_client;
      sicl::rpc::SiRPC::newInstance(sirpc_client, false);
      sicl::rpc::RpcContext* ctx_p = nullptr;
      sicl::rpc::RpcContext::newInstance(ctx_p);
      std::shared_ptr<sicl::rpc::RpcContext> ctx = std::shared_ptr<sicl::rpc::RpcContext>(ctx_p);
      //FIXME(ytji): URGENT! - 100 servers under 1s timeout will make rpc timeout issue occasionally
      ctx->set_timeout(sicl::transport::TimerTick::TIMER_3S);

      // send handshake rpc to cm to join into the cluster
      NewNodeHandShakeRequestPB hs_req;
      auto hs_resp = new NewNodeHandShakeResponsePB();
      auto _field = hs_req.mutable_node();
      _field->set_ip(server_addr_str);
      _field->set_port(server_port);
      auto hs_done_cb_ok = [&](const google::protobuf::Message* rsp,
                               const std::shared_ptr<sicl::rpc::RpcContext> ctx) {
        EXPECT_EQ(ctx->ErrorCode(), sicl::transport::Result::SICL_SUCCESS);
        auto response = dynamic_cast<const NewNodeHandShakeResponsePB*>(rsp);
        EXPECT_EQ(response->ret_code(), CommonErr::OK);
        // clean up response object
        delete hs_resp;
        hs_resp = nullptr;
        hs_done_latch.count_down();
      };

      sirpc_client->SendRequest(
          "127.0.0.1",
          FLAGS_cm_rpc_inter_port,
          static_cast<sicl::rpc::ReqType>(simm::cm::ClusterManagerRpcType::RPC_NEW_NODE_HANDSHAKE),
          hs_req,
          hs_resp,
          ctx,
          hs_done_cb_ok);

      hs_done_latch.wait();

      for (uint32_t n = 0; n < kHBNumPerServer; ++n) {
        DataServerHeartBeatRequestPB hb_req;
        auto hb_resp = new DataServerHeartBeatResponsePB();
        auto node_field = hb_req.mutable_node();
        node_field->set_ip(server_addr_str);
        node_field->set_port(server_port);
        auto hb_done_cb_ok = [&](const google::protobuf::Message* rsp,
                                        const std::shared_ptr<sicl::rpc::RpcContext> ctx) {
          EXPECT_EQ(ctx->ErrorCode(), sicl::transport::Result::SICL_SUCCESS);
          auto response = dynamic_cast<const DataServerHeartBeatResponsePB*>(rsp);
          EXPECT_EQ(response->ret_code(), CommonErr::OK);
          // clean up response object
          delete hb_resp;
          hb_resp = nullptr;
          hb_done_latch.count_down();
        };
        // send by ip:port
        // 127.0.0.1:30001
        if (ctype == caseType::OnRecvNodeHeartbeat) {
          MLOG_INFO("Enter into caseType::OnRecvNodeHeartbeat");
          if ((thread_num == 3 || thread_num == 5) && (n == 1 || n == 3)) {
            // mock some hb requests missing
          } else {
            sirpc_client->SendRequest(
                "127.0.0.1",
                FLAGS_cm_rpc_inter_port,
                static_cast<sicl::rpc::ReqType>(simm::cm::ClusterManagerRpcType::RPC_NODE_HEARTBEAT),
                hb_req,
                hb_resp,
                ctx,
                hb_done_cb_ok);
          }
        } else if (ctype == caseType::bgHBScanLoop) {
          MLOG_INFO("Enter into caseType::bgHBScanLoop");
          if ((thread_num == 0 || thread_num == 2 || thread_num == 4 || thread_num == 6 || thread_num == 8) && 
              (n == 4 || n == 5 || n == 6 || n == 7)) {
            // mock some hb requests missing
          } else {
            sirpc_client->SendRequest(
                "127.0.0.1",
                FLAGS_cm_rpc_inter_port,
                static_cast<sicl::rpc::ReqType>(simm::cm::ClusterManagerRpcType::RPC_NODE_HEARTBEAT),
                hb_req,
                hb_resp,
                ctx,
                hb_done_cb_ok);
          }
        }
        // FIXME(ytji): use dataserver hb interval flag instead of hard codes
        std::this_thread::sleep_for(std::chrono::seconds(1));
      }
      MLOG_INFO("client_sub_thread-{} exits!", thread_num);
    };

    for (uint32_t i = 0; i < kDataserverNum; ++i) {
      executor.add([i, &client_sub_thread]() {
        client_sub_thread(i);
      });
      MLOG_INFO("client_sub_thread-{} added!", i);
    }

    // wait for all sub-threads finish hb send/recv test actions
    hb_done_latch.wait();
    // all tests done, signal server thread to stop
    stopServer.store(true);

    MLOG_INFO("Client threads exit!");
  };

  executor1.add(server_thread);
  MLOG_INFO("server_thread is added....");
  executor1.add([&executor1, &client_thread]() { client_thread(executor1); });
  MLOG_INFO("client_thread is added....");

  // wait for server thread to finish
  // co_await serverDone;
  serverDone.wait();

  MLOG_INFO("serverDone.wait finished....");

  executor1.join();

  MLOG_INFO("start case 1 checks....");
  // test checks
  {
    auto locked_map = cm_hb_monitor_ptr_->ds_hb_records_.rlock();
    for (uint32_t i = 0; i < kDataserverNum; ++i) {
        // FIXME(ytji) : change the port hard code with flag value later
        std::string server_addr = ip_addr_prefix + std::to_string(i) + ":" + std::to_string(server_port);
        EXPECT_EQ(locked_map->at(server_addr).size(), FLAGS_cm_heartbeat_records_perserver);
        auto it = locked_map->at(server_addr).begin();
        auto it_nxt = it + 1;
        if (it_nxt != locked_map->at(server_addr).end()) {
          if (i == 3 || i == 5) {
            // FIXME(ytji): how to check ts?
            //EXPECT_EQ(it_nxt->monotonic_tp_ - it->monotonic_tp_, std::chrono::seconds(2));
          } else {
            // FIXME(ytji): how to check ts?
            //EXPECT_EQ(it_nxt->monotonic_tp_ - it->monotonic_tp_, std::chrono::seconds(1));
          }
        }
    }
  }

  // ------------------------------------------------------------------------
  // (2) Test ClusterManagerHBMonitor::BgHBScanLoop() interface
  // ------------------------------------------------------------------------
  MLOG_INFO("Case to test Test ClusterManagerHBMonitor::BgHBScanLoop()");
  // clean up actions
  this->cm_node_manager_ptr_->node_info_map_.clear();
  this->cm_node_manager_ptr_->node_status_map_.clear();
  {
    auto uomap_locked = this->cm_hb_monitor_ptr_->ds_hb_records_.wlock();
    uomap_locked->clear();
  }

  serverStarted.store(false);
  stopServer.store(false);

  folly::CPUThreadPoolExecutor executor2(kDataserverNum+2);
  serverDone.reset();
  // mock 5 servers 4 HB requests respectively
  // server-1 : req1, req2, req3, req4; req5 ~ req8 missed
  // server-3 : req1, req2, req3, req4; req5 ~ req8 missed
  // server-5 : req1, req2, req3, req4; req5 ~ req8 missed
  // server-7 : req1, req2, req3, req4; req5 ~ req8 missed
  // server-9 : req1, req2, req3, req4; req5 ~ req8 missed
  latch_num = kDataserverNum * kHBNumPerServer - 20;
  ctype = caseType::bgHBScanLoop;

  uint32_t old_flag_val_3 = FLAGS_cm_heartbeat_timeout_inSecs;
  uint32_t old_flag_val_4 = FLAGS_cm_heartbeat_bg_scan_interval_inSecs;
  FLAGS_cm_heartbeat_records_perserver = 5; // max 5 HB record per dataserver
  FLAGS_cm_heartbeat_timeout_inSecs = 2; // 2 secs timeout
  FLAGS_cm_heartbeat_bg_scan_interval_inSecs = 1; // 1s bg scan interval

  // mark hb monitor statis is runing
  cm_hb_monitor_ptr_->stop_flag_.store(false);

  executor2.add(server_thread);
  executor2.add([&executor2, &client_thread]() { client_thread(executor2); });

  // wait for server thread to finish
  // co_await serverDone;
  serverDone.wait();

  executor2.join();

  MLOG_INFO("start case 2 checks....");
  // test checks
  {
    auto locked_map = cm_hb_monitor_ptr_->ds_hb_records_.rlock();
    // Caution: folly::ConcurrentMap size() interface has such character:
    // size() is approximate and may not reflect concurrent updates or recent clear() operations.
    EXPECT_EQ(locked_map->size(), kDataserverNum);
    for (uint32_t i = 0; i < kDataserverNum; ++i) {
        // FIXME(ytji) : change the port hard code with flag value later
        std::string server_addr = ip_addr_prefix + std::to_string(i) + ":" + std::to_string(server_port);
        auto it = locked_map->find(server_addr);
        EXPECT_TRUE(it != locked_map->end());
        if (i == 0 || i == 2 || i == 4 || i == 6 || i == 8) {
          EXPECT_EQ(it->second.size(), 4);
        } else {
          EXPECT_EQ(it->second.size(), FLAGS_cm_heartbeat_records_perserver);
        }
        auto it_nm = cm_node_manager_ptr_->node_status_map_.find(server_addr);
        //EXPECT_TRUE(it_nm != cm_node_manager_ptr_->node_status_map_.end());
        if (it_nm != cm_node_manager_ptr_->node_status_map_.end()) {
          if (i == 0 || i == 2 || i == 4 || i == 6 || i == 8) {
            EXPECT_EQ(it_nm->second, NodeStatus::DEAD);
          } else {
            // FIXME(ytji): all dataservers are marked as DEAD
            //EXPECT_EQ(it_nm->second, NodeStatus::RUNNING);
          }
        }
    }
  }

  // revert flag values
  FLAGS_cm_cluster_init_grace_period_inSecs = old_flag_val_1;
  FLAGS_cm_heartbeat_records_perserver = old_flag_val_2;
  FLAGS_cm_heartbeat_timeout_inSecs = old_flag_val_3;
  FLAGS_cm_heartbeat_bg_scan_interval_inSecs = old_flag_val_4;
}

}  // namespace cm
}  // namespace simm

// should use main function in below way to accept gflag options changes
// from UT test binary command line
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
