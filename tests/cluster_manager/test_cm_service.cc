#include <chrono>
#include <latch>
#include <memory>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <folly/Random.h>
#include <folly/ScopeGuard.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/synchronization/Baton.h>

#include "cluster_manager/cm_rpc_handler.h"
#include "cluster_manager/cm_service.h"
#include "common/base/common_types.h"
#include "common/errcode/errcode_def.h"
#include "common/logging/logging.h"
#include "proto/cm_clnt_rpcs.pb.h"
#include "proto/ds_cm_rpcs.pb.h"

DECLARE_LOG_MODULE("cluster_manager");

DECLARE_uint32(shard_total_num);
DECLARE_int32(cm_rpc_inter_port);
DECLARE_uint32(cm_cluster_init_grace_period_inSecs);
DECLARE_string(cm_log_file);
DECLARE_uint32(dataserver_min_num);
DECLARE_string(cm_primary_node_ip);

namespace simm {
namespace cm {

using CmSrvPtr = std::unique_ptr<simm::cm::ClusterManagerService>;

class ClusterManagerServiceTest : public ::testing::Test {
 protected:
  void SetUp() override {
    #ifdef NDEBUG
      simm::logging::LogConfig cm_log_config = simm::logging::LogConfig{FLAGS_cm_log_file, "INFO"};
    #else
      simm::logging::LogConfig cm_log_config = simm::logging::LogConfig{FLAGS_cm_log_file, "DEBUG"};
    #endif
      simm::logging::LoggerManager::Instance().UpdateConfig("cluster_manager", cm_log_config);

    cm_service_ptr_ = std::make_unique<simm::cm::ClusterManagerService>();
  }

  void TearDown() override { cm_service_ptr_.reset(); }

  CmSrvPtr cm_service_ptr_{nullptr};
};

TEST_F(ClusterManagerServiceTest, TestStartAndStop) {
  auto old_flag_val = FLAGS_cm_cluster_init_grace_period_inSecs;
  FLAGS_cm_cluster_init_grace_period_inSecs = 1;

  auto ret = cm_service_ptr_->Init();
  EXPECT_EQ(ret, CommonErr::OK);

  // FIXME(ytji) : for no dataserver sends hand-shake RPC to cm, so it will start failed
  // maybe should add some background threads to send hand-shake RPCs
  ret = cm_service_ptr_->Start();
  EXPECT_EQ(ret, CmErr::InitDataserverNodesTooFew);
  EXPECT_TRUE(!cm_service_ptr_->IsRunning());
  //EXPECT_EQ(ret, CommonErr::OK);
  //EXPECT_TRUE(cm_service_ptr_->IsRunning());

  // FIXME(ytji) : for cm isn't started, so stop action also will fail
  ret = cm_service_ptr_->Stop();
  EXPECT_EQ(ret, CmErr::ClusterManagerAlreadyStopped);
  // EXPECT_EQ(ret, CommonErr::OK);

  FLAGS_cm_cluster_init_grace_period_inSecs = old_flag_val;
}

TEST_F(ClusterManagerServiceTest, TestQueryRoutingTableInfoRPCs) {
  std::atomic<bool> serverStarted{false};
  std::atomic<bool> stopServer{false};
  // light-weight cv
  // post()  - signaled state
  // reset() - set to unsignaled state again
  folly::Baton<> serverDone;
  folly::CPUThreadPoolExecutor executor(2);
  std::latch done_latch(4);

  constexpr uint32_t kDataserverNum = 8;

  auto server_thread = [&]() {
    auto ret = cm_service_ptr_->Init();
    EXPECT_EQ(ret, CommonErr::OK);
    //FIXME(ytji) : for no dataserver sends hand-shake RPC to cm, so it will start failed
    //ret = cm_service_ptr_->Start();
    //EXPECT_EQ(ret, CommonErr::OK);
    //EXPECT_TRUE(cm_service_ptr_->IsRunning());
    ret = cm_service_ptr_->StartRPCServices();
    EXPECT_EQ(ret, CommonErr::OK);

    // by-pass grace period phase for no dataserver send RPCs and join into the cluster
    simm::common::ModuleServiceState::GetInstance().MarkServiceReady();

    // mock 8 dataservers registered in cluster init grace period
    // 16384 shards loaded evenly on 8 dataservers
    // x % 8 == 0 - 192.168.1.1:40000
    // x % 8 == 1 - 192.168.1.2:40001
    // x % 8 == 2 - 192.168.1.3:40002
    // x % 8 == 3 - 192.168.1.4:40003
    // x % 8 == 4 - 192.168.1.5:40004
    // x % 8 == 5 - 192.168.1.6:40005
    // x % 8 == 6 - 192.168.1.7:40006
    // x % 8 == 7 - 192.168.1.8:40007
    std::vector<std::shared_ptr<simm::common::NodeAddress>> all_servers;
    for (uint32_t i = 0; i < kDataserverNum; ++i) {
      auto server = std::make_shared<simm::common::NodeAddress>(("192.168.1." + std::to_string(i + 1)), (40000 + i));
      all_servers.push_back(server);
    }
    ret = cm_service_ptr_->shard_manager_->InitShardRoutingTable(all_servers);
    EXPECT_EQ(ret, CommonErr::OK);

    serverStarted.store(true);

    while (!stopServer.load()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    //FIXME(ytji) : for no dataserver sends hand-shake RPC to cm, it doesn't start, so
    // stop action will always fail
    //ret = cm_service_ptr_->Stop();
    //EXPECT_EQ(ret, CommonErr::OK);
    ret = cm_service_ptr_->StopRPCServices();
    EXPECT_EQ(ret, CommonErr::OK);

    serverStarted.store(false);
    serverDone.post();  // noitfy cv
  };

  auto client_thread = [&]() {
    while (!serverStarted.load()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // create sirpc object as rpc client
    sicl::rpc::SiRPC* sirpc_client;
    sicl::rpc::SiRPC::newInstance(sirpc_client, false);

    sicl::rpc::RpcContext* ctx_p = nullptr;
    sicl::rpc::RpcContext::newInstance(ctx_p);
    std::shared_ptr<sicl::rpc::RpcContext> ctx = std::shared_ptr<sicl::rpc::RpcContext>(ctx_p);
    ctx->set_timeout(sicl::transport::TimerTick::TIMER_1S);

    // test single shard id routing table query
    QueryShardRoutingTableSingleRequestPB single_query_req;
    auto single_query_res_found = new QueryShardRoutingTableSingleResponsePB();
    single_query_req.set_shard_id(0);  // query shard 0
    auto done_cb_ok = [&single_query_res_found, &done_latch](const google::protobuf::Message* rsp,
                                                             const std::shared_ptr<sicl::rpc::RpcContext> ctx) {
      ASSERT_TRUE(ctx->ErrorCode() == sicl::transport::Result::SICL_SUCCESS);
      auto response = dynamic_cast<const QueryShardRoutingTableSingleResponsePB*>(rsp);
      EXPECT_EQ(response->ret_code(), CommonErr::OK);
      EXPECT_EQ(response->shard_info_size(), 1);
      EXPECT_EQ(response->shard_info(0).shard_ids_size(), 1);
      EXPECT_EQ(response->shard_info(0).shard_ids(0), 0);
      EXPECT_EQ(response->shard_info(0).data_server_address().ip(), "192.168.1.1");
      EXPECT_EQ(response->shard_info(0).data_server_address().port(), 40000);
      // clean up response object
      delete single_query_res_found;
      single_query_res_found = nullptr;
      done_latch.count_down();
    };
    // send by ip:port
    // 127.0.0.1:30001
    sirpc_client->SendRequest(
        "127.0.0.1",
        FLAGS_cm_rpc_inter_port,
        static_cast<sicl::rpc::ReqType>(simm::cm::ClusterManagerRpcType::RPC_ROUTING_TABLE_QUERY_SINGLE),
        single_query_req,
        single_query_res_found,
        ctx,
        done_cb_ok);

    single_query_req.set_shard_id(FLAGS_shard_total_num);
    auto single_query_res_nfound = new QueryShardRoutingTableSingleResponsePB();
    auto done_cb_not_found = [&single_query_res_nfound, &done_latch](const google::protobuf::Message* rsp,
                                                                     const std::shared_ptr<sicl::rpc::RpcContext> ctx) {
      ASSERT_TRUE(ctx->ErrorCode() == sicl::transport::Result::SICL_SUCCESS);
      auto response = dynamic_cast<const QueryShardRoutingTableSingleResponsePB*>(rsp);
      EXPECT_EQ(response->ret_code(), CommonErr::CmTargetShardIdNotFound);
      EXPECT_TRUE(response->shard_info().empty());
      // clean up response object
      delete single_query_res_nfound;
      single_query_res_nfound = nullptr;
      done_latch.count_down();
    };
    sirpc_client->SendRequest(
        "127.0.0.1",
        FLAGS_cm_rpc_inter_port,
        static_cast<sicl::rpc::ReqType>(simm::cm::ClusterManagerRpcType::RPC_ROUTING_TABLE_QUERY_SINGLE),
        single_query_req,
        single_query_res_nfound,
        ctx,
        done_cb_not_found);

    std::vector<shard_id_t> target_shards_vec = {0,
                                                 1024 /*ds1*/,
                                                 1 /*ds2*/,
                                                 2048 + 2,
                                                 2 /*ds3*/,
                                                 3072 + 3 /*ds4*/,
                                                 4096 + 4 /*ds5*/,
                                                 5120 + 5 /*ds6*/,
                                                 6144 + 6 /*ds7*/,
                                                 7168 + 7 /*ds8*/,
                                                 30000 /*invalid*/};
    QueryShardRoutingTableBatchRequestPB batch_query_req;
    auto batch_query_res = new QueryShardRoutingTableBatchResponsePB();
    batch_query_req.mutable_shard_ids()->Add(target_shards_vec.begin(), target_shards_vec.end());
    auto done_cb_batch = [&batch_query_res, &done_latch](const google::protobuf::Message* rsp,
                                                         const std::shared_ptr<sicl::rpc::RpcContext> ctx) {
      ASSERT_TRUE(ctx->ErrorCode() == sicl::transport::Result::SICL_SUCCESS);
      auto response = dynamic_cast<const QueryShardRoutingTableBatchResponsePB*>(rsp);
      EXPECT_EQ(response->ret_code(), CommonErr::OK);
      EXPECT_EQ(response->shard_info_size(), 8);  // all 8 dataservers foud
      for (uint32_t i = 0; i < kDataserverNum; ++i) {
        if (response->shard_info(i).data_server_address().ip() == "192.168.1.1") {
          EXPECT_EQ(response->shard_info(i).data_server_address().port(), 40000);
          EXPECT_EQ(response->shard_info(i).shard_ids_size(), 2);
          EXPECT_EQ(response->shard_info(i).shard_ids(0), 1024);
          EXPECT_EQ(response->shard_info(i).shard_ids(1), 0);
        } else if (response->shard_info(i).data_server_address().ip() == "192.168.1.2") {
          EXPECT_EQ(response->shard_info(i).data_server_address().port(), 40001);
          EXPECT_EQ(response->shard_info(i).shard_ids_size(), 1);
          EXPECT_EQ(response->shard_info(i).shard_ids(0), 1);
        } else if (response->shard_info(i).data_server_address().ip() == "192.168.1.3") {
          EXPECT_EQ(response->shard_info(i).data_server_address().port(), 40002);
          EXPECT_EQ(response->shard_info(i).shard_ids_size(), 2);
          EXPECT_EQ(response->shard_info(i).shard_ids(0), 2);
          EXPECT_EQ(response->shard_info(i).shard_ids(1), 2048 + 2);
        } else if (response->shard_info(i).data_server_address().ip() == "192.168.1.4") {
          EXPECT_EQ(response->shard_info(i).data_server_address().port(), 40003);
          EXPECT_EQ(response->shard_info(i).shard_ids_size(), 1);
          EXPECT_EQ(response->shard_info(i).shard_ids(0), 3072 + 3);
        } else if (response->shard_info(i).data_server_address().ip() == "192.168.1.5") {
          EXPECT_EQ(response->shard_info(i).data_server_address().port(), 40004);
          EXPECT_EQ(response->shard_info(i).shard_ids_size(), 1);
          EXPECT_EQ(response->shard_info(i).shard_ids(0), 4096 + 4);
        } else if (response->shard_info(i).data_server_address().ip() == "192.168.1.6") {
          EXPECT_EQ(response->shard_info(i).data_server_address().port(), 40005);
          EXPECT_EQ(response->shard_info(i).shard_ids_size(), 1);
          EXPECT_EQ(response->shard_info(i).shard_ids(0), 5120 + 5);
        } else if (response->shard_info(i).data_server_address().ip() == "192.168.1.7") {
          EXPECT_EQ(response->shard_info(i).data_server_address().port(), 40006);
          EXPECT_EQ(response->shard_info(i).shard_ids_size(), 1);
          EXPECT_EQ(response->shard_info(i).shard_ids(0), 6144 + 6);
        } else if (response->shard_info(i).data_server_address().ip() == "192.168.1.8") {
          EXPECT_EQ(response->shard_info(i).data_server_address().port(), 40007);
          EXPECT_EQ(response->shard_info(i).shard_ids_size(), 1);
          EXPECT_EQ(response->shard_info(i).shard_ids(0), 7168 + 7);
        } else {
          ASSERT_TRUE(false) << "Unexpected DataServer IP: " << response->shard_info(i).data_server_address().ip();
        }
      }

      delete batch_query_res;
      batch_query_res = nullptr;
      done_latch.count_down();
    };
    sirpc_client->SendRequest(
        "127.0.0.1",
        FLAGS_cm_rpc_inter_port,
        static_cast<sicl::rpc::ReqType>(simm::cm::ClusterManagerRpcType::RPC_ROUTING_TABLE_QUERY_BATCH),
        batch_query_req,
        batch_query_res,
        ctx,
        done_cb_batch);

    QueryShardRoutingTableAllRequestPB all_query_req;
    auto all_query_res = new QueryShardRoutingTableAllResponsePB();
    auto done_cb_all = [&all_query_res, &done_latch](const google::protobuf::Message* rsp,
                                                     const std::shared_ptr<sicl::rpc::RpcContext> ctx) {
      EXPECT_TRUE(ctx->ErrorCode() == sicl::transport::Result::SICL_SUCCESS);
      auto response = dynamic_cast<const QueryShardRoutingTableAllResponsePB*>(rsp);
      EXPECT_EQ(response->ret_code(), CommonErr::OK);
      // FIXME(ytji) : fix below checks
      // EXPECT_EQ(response->shard_info_size(), 8); // all 8 dataservers foud
      // for (uint32_t i = 0, shard_id = 100; i < kDataserverNum; ++i, shard_id
      // += 2048) {
      //  EXPECT_EQ(response->shard_info(i).shard_ids_size(), 2048);
      //  EXPECT_EQ(response->shard_info(i).data_server_address().ip(),
      //  "192.168.1." + std::to_string(i + 1));
      //  EXPECT_EQ(response->shard_info(i).data_server_address().port(), 40000
      //  + i);
      //}
      // clean up respnse object
      delete all_query_res;
      all_query_res = nullptr;
      done_latch.count_down();
    };
    sirpc_client->SendRequest(
        "127.0.0.1",
        FLAGS_cm_rpc_inter_port,
        static_cast<sicl::rpc::ReqType>(simm::cm::ClusterManagerRpcType::RPC_ROUTING_TABLE_QUERY_ALL),
        all_query_req,
        all_query_res,
        ctx,
        done_cb_all);

    // wait for all in-flight RPC requests finish
    done_latch.wait();

    // all tests done, signal server thread to stop
    stopServer.store(true);
  };

  executor.add(server_thread);
  executor.add(client_thread);

  // wait for server thread to finish
  // co_await serverDone;
  serverDone.wait();

  executor.join();
}

TEST_F(ClusterManagerServiceTest, TestNewNodeHandShakeRPC) {
  std::atomic<bool> serverStarted{false};
  std::atomic<bool> stopServer{false};
  const uint32_t kDataserverNum = 40;
  std::string ip_addr_prefix{"192.168.0."};
  int32_t server_port = 40000;
  folly::CPUThreadPoolExecutor executor(32);
  folly::Baton<> serverDone;

  auto server_thread = [&]() {
    // mock to record cm main process start timestamp
    auto old_flag_val = FLAGS_cm_cluster_init_grace_period_inSecs;
    FLAGS_cm_cluster_init_grace_period_inSecs = 5;
    simm::common::ModuleServiceState::GetInstance().Reset(FLAGS_cm_cluster_init_grace_period_inSecs);
    auto shard_manager_ptr = std::make_shared<simm::cm::ClusterManagerShardManager>();
    auto node_manager_ptr = std::make_shared<simm::cm::ClusterManagerNodeManager>();
    auto hb_monitor_ptr = std::make_shared<simm::cm::ClusterManagerHBMonitor>(node_manager_ptr, shard_manager_ptr);
    auto cm_service_ptr = std::make_unique<simm::cm::ClusterManagerService>(
      shard_manager_ptr, node_manager_ptr, hb_monitor_ptr);
    auto ret = cm_service_ptr->Init();
    EXPECT_EQ(ret, CommonErr::OK);
    ret = cm_service_ptr->Start();
    EXPECT_EQ(ret, CommonErr::OK);
    EXPECT_TRUE(cm_service_ptr->IsRunning());

    serverStarted.store(true);
    MLOG_INFO("Server thread started successfully!");

    while (!stopServer.load()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    // test checks
    shard_id_t steps = static_cast<shard_id_t>(FLAGS_shard_total_num / kDataserverNum);
    shard_id_t rests = static_cast<shard_id_t>(FLAGS_shard_total_num - steps * (kDataserverNum - 1));
    auto all_servers_vec = node_manager_ptr->GetAllNodeAddress();
    EXPECT_EQ(all_servers_vec.size(), kDataserverNum);
    std::unordered_set<std::string> ds_set;
    for (const auto & ptr : all_servers_vec) {
      ds_set.insert(ptr->toString());
    }
    EXPECT_EQ(ds_set.size(), kDataserverNum);
    // key : ds addr str ; value : shard number it holds
    std::unordered_map<std::string, uint32_t> converted_routing_map;
    for (uint32_t i = 0; i < kDataserverNum; i++) {
      std::string server_addr_str = ip_addr_prefix + std::to_string(i);
      std::string server_addr_port_str = server_addr_str + std::string(":") + std::to_string(server_port);
      auto find_ds_addr_it = ds_set.find(server_addr_port_str);
      EXPECT_TRUE(find_ds_addr_it != ds_set.end());
      simm::cm::NodeStatus st = node_manager_ptr->QueryNodeStatus(server_addr_port_str);
      EXPECT_EQ(st, NodeStatus::RUNNING);
    }
    auto query_res = shard_manager_ptr->QueryAllShardRoutingInfos();
    for (const auto & entry : query_res) {
      std::string ds_addr_str = entry.second->toString();
      auto it = ds_set.find(ds_addr_str);
      EXPECT_TRUE(it != ds_set.end());
      converted_routing_map[ds_addr_str]++;
    }
    for (const auto & entry : converted_routing_map) {
      EXPECT_TRUE(entry.second == steps || entry.second == rests);
      MLOG_INFO("DS({}), holds ({}) shards", entry.first, entry.second);
    }

    MLOG_INFO("Server thread start to stop!");
    ret = cm_service_ptr->Stop();
    EXPECT_EQ(ret, CommonErr::OK);
    MLOG_INFO("Server thread stopped successfully!");

    serverStarted.store(false);
    serverDone.post();  // noitfy cv

    MLOG_INFO("Server thread exit!");

    FLAGS_cm_cluster_init_grace_period_inSecs = old_flag_val;
  };

  auto client_thread = [&]() {
    //while (!serverStarted.load()) {
    // just hang 1 second to wait for RPC service ready of cm service
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    //}

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
      auto node_field = hs_req.mutable_node();
      node_field->set_ip(server_addr_str);
      node_field->set_port(server_port);
      auto hs_done_cb_ok = [&](const google::protobuf::Message* rsp,
                                         const std::shared_ptr<sicl::rpc::RpcContext> ctx) {
        EXPECT_EQ(ctx->ErrorCode(), sicl::transport::Result::SICL_SUCCESS);
        auto response = dynamic_cast<const NewNodeHandShakeResponsePB*>(rsp);
        EXPECT_EQ(response->ret_code(), CommonErr::OK);
        // clean up response object
        // delete hs_resp;
        // hs_resp = nullptr;
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

      MLOG_INFO("client_sub_thread-{} exits!", thread_num);
    };

    for (uint32_t i = 0; i < kDataserverNum; ++i) {
      executor.add([i, &client_sub_thread]() {
        client_sub_thread(i);
      });
      MLOG_INFO("client_sub_thread-{} added!", i);
    }

    // wait for all sub-threads finish hb send/recv test actions
    hs_done_latch.wait();
    MLOG_INFO("All handle shake request finished");

    // all tests done, signal server thread to stop
    stopServer.store(true);

    MLOG_INFO("Client threads exit!");
  };

  executor.add(server_thread);
  executor.add(client_thread);

  serverDone.wait();

  MLOG_INFO("serverDone.wait finished....");

  executor.join();
}

TEST_F(ClusterManagerServiceTest, TestNodeRejoinRPC) {
  auto old_flag_val = FLAGS_cm_cluster_init_grace_period_inSecs;
  FLAGS_cm_cluster_init_grace_period_inSecs = 10;

  FLAGS_dataserver_min_num = 0;

  folly::CPUThreadPoolExecutor executor(32);

  auto data_server_thread = [&](size_t id, bool withInitialShards = true) {
    sicl::rpc::SiRPC *sirpc_client;
    sicl::rpc::SiRPC::newInstance(sirpc_client, false);
    sicl::rpc::RpcContext *ctx_p = nullptr;
    sicl::rpc::RpcContext::newInstance(ctx_p);
    std::shared_ptr<sicl::rpc::RpcContext> ctx = std::shared_ptr<sicl::rpc::RpcContext>(ctx_p);

    NewNodeHandShakeRequestPB request;
    auto response = new NewNodeHandShakeResponsePB();
    auto node_field = request.mutable_node();
    node_field->set_ip("127.0.0.1");
    node_field->set_port(40000 + id);
    if (withInitialShards) {
      request.add_shard_ids(id);
    };

    auto done = [&](const google::protobuf::Message *rsp, const std::shared_ptr<sicl::rpc::RpcContext> ctx) {
      EXPECT_EQ(ctx->ErrorCode(), sicl::transport::Result::SICL_SUCCESS);
      auto response = dynamic_cast<const NewNodeHandShakeResponsePB *>(rsp);
      EXPECT_EQ(response->ret_code(), CommonErr::OK);
    };

    sirpc_client->SendRequest("127.0.0.1",
                              FLAGS_cm_rpc_inter_port,
                              static_cast<sicl::rpc::ReqType>(simm::cm::ClusterManagerRpcType::RPC_NEW_NODE_HANDSHAKE),
                              request,
                              response,
                              ctx,
                              done);
  };

  simm::common::ModuleServiceState::GetInstance().Reset(FLAGS_cm_cluster_init_grace_period_inSecs);
  executor.add([&]() {
    EXPECT_EQ(cm_service_ptr_->Start(), CommonErr::OK);
    MLOG_INFO("Cluster Manager has started");
  });

  std::this_thread::sleep_for(std::chrono::seconds(3));

  for (size_t i = 1; i <= 3; ++i) {
    executor.add([i, data_server_thread]() { data_server_thread(i); });
    MLOG_INFO("Server {} added", i);
  }

  executor.add([data_server_thread]() { data_server_thread(4, false); });
  MLOG_INFO("Server {} added", 4);

  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_cm_cluster_init_grace_period_inSecs));

  auto routing_info = cm_service_ptr_->shard_manager_->QueryAllShardRoutingInfos();

  for (size_t i = 1; i <= 4; ++i) {
    EXPECT_EQ(routing_info.at(i)->node_ip_, "127.0.0.1");
    EXPECT_EQ(routing_info.at(i)->node_port_, 40000 + i);
  }

  EXPECT_EQ(cm_service_ptr_->Stop(), CommonErr::OK);
  MLOG_INFO("Cluster Manager has stopped");

  FLAGS_cm_cluster_init_grace_period_inSecs = old_flag_val;
}

}  // namespace cm
}  // namespace simm

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
