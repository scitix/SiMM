#include <array>
#include <iostream>
#include <memory>
#include <ranges>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <folly/Random.h>
#include <folly/ScopeGuard.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/experimental/coro/BlockingWait.h>
#include <folly/experimental/coro/Collect.h>
#include <folly/experimental/coro/Task.h>
//#include <folly/init/Init.h>

#include "cluster_manager/cm_shard_manager.h"
#include "common/base/common_types.h"
#include "common/errcode/errcode_def.h"
#include "common/logging/logging.h"

DECLARE_LOG_MODULE("cluster_manager");

DECLARE_uint32(shard_total_num);
DECLARE_uint32(dataserver_min_num);

namespace simm {
namespace cm {

using CmSmPtr = std::unique_ptr<simm::cm::ClusterManagerShardManager>;

class ClusterManagerShardManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {
#ifdef NDEBUG
    simm::logging::LogConfig cm_log_config = simm::logging::LogConfig{"/tmp/simm_cm.log", "INFO"};
#else
    simm::logging::LogConfig cm_log_config = simm::logging::LogConfig{"/tmp/simm_cm.log", "DEBUG"};
#endif
    simm::logging::LoggerManager::Instance().UpdateConfig("cluster_manager", cm_log_config);

    cm_shard_manager_ptr_ = std::make_unique<simm::cm::ClusterManagerShardManager>();
  }

  void TearDown() override {
    cm_shard_manager_ptr_.reset();
  }

  CmSmPtr cm_shard_manager_ptr_{nullptr};
};

TEST_F(ClusterManagerShardManagerTest, TestGetTotalShardNum) {
  EXPECT_EQ(cm_shard_manager_ptr_->GetTotalShardNum(), FLAGS_shard_total_num);
}

TEST_F(ClusterManagerShardManagerTest, TestTriggerShardsMigration) {
  std::vector<shard_id_t> target_shards;
  std::vector<simm::common::NodeAddress *> target_servers;
  error_code_t ret = cm_shard_manager_ptr_->TriggerShardsMigration(target_shards, target_servers);
  EXPECT_EQ(ret, CommonErr::NotImplemented);

  // cleanup
  cm_shard_manager_ptr_->CleanRoutingTable();
}

TEST_F(ClusterManagerShardManagerTest, TestOnRecvShardMigrationResponse) {
  error_code_t ret = cm_shard_manager_ptr_->OnRecvShardMigrationResponse();
  EXPECT_EQ(ret, CommonErr::NotImplemented);

  // cleanup
  cm_shard_manager_ptr_->CleanRoutingTable();
}

TEST_F(ClusterManagerShardManagerTest, TestInitShardRoutingTable) {
  // init cluster with different dataserver numbers
  std::array<uint32_t, 8> kDataserverNumArray = {2, 8, 10, 50, 200, 500, 1000, 5000};
  // set different sub step length to check rounting table entry results
  std::array<uint32_t, 4> kCheckSubStepArray = {13, 71, 100, 255};
  for (auto &kCheckSubStep : kCheckSubStepArray) {
    for (auto &kDataserverNum : kDataserverNumArray) {
      cm_shard_manager_ptr_->CleanRoutingTable();
      uint32_t totalShardNum = cm_shard_manager_ptr_->GetTotalShardNum();
      EXPECT_EQ(totalShardNum, FLAGS_shard_total_num);
      uint32_t step = totalShardNum / kDataserverNum;
      // for every dataserver, case samples some routing entries with sub step
      uint32_t sub_step = step / kCheckSubStep;
      // avoid dead loop in below while block
      sub_step = sub_step == 0 ? step - 1 : sub_step;
      shard_id_t cur_shard_id = 0;

      std::vector<std::shared_ptr<simm::common::NodeAddress>> all_servers;
      for (uint32_t i = 0; i < kDataserverNum; ++i) {
        auto server = std::make_shared<simm::common::NodeAddress>(("192.168.1." + std::to_string(i + 1)), (30000 + i));
        all_servers.push_back(server);
      }

      error_code_t ret = cm_shard_manager_ptr_->InitShardRoutingTable(all_servers);
      if (kDataserverNum == 2) {
        EXPECT_EQ(ret, CmErr::InitDataserverNodesTooFew);
        goto next_loop;
      } else if (kDataserverNum == 5000) {
        EXPECT_EQ(ret, CmErr::InitDataserverNodesTooMany);
        goto next_loop;
      } else {
        EXPECT_EQ(ret, CommonErr::OK);
      }

      while (cur_shard_id < totalShardNum) {
        auto dataserver_index = cur_shard_id % kDataserverNum;
        auto routing_info = cm_shard_manager_ptr_->QueryShardRoutingInfo(cur_shard_id);
        EXPECT_EQ(routing_info.size(), 1);
        EXPECT_EQ(routing_info.at(cur_shard_id)->node_ip_, ("192.168.1." + std::to_string(dataserver_index + 1)));
        EXPECT_EQ(routing_info.at(cur_shard_id)->node_port_, (30000 + dataserver_index));
        cur_shard_id += sub_step;
      }

    next_loop:
      for (auto &server : all_servers) {
        server.reset();
      }
      all_servers.clear();
    }
  }

  // cleanup
  cm_shard_manager_ptr_->CleanRoutingTable();
}

folly::coro::Task<void> concurrentReadTask(const CmSmPtr &testMgrPtr) {
  constexpr uint32_t kRandQueryNum = 1000;
  for (uint32_t i = 0; i < kRandQueryNum; ++i) {
    uint32_t rand_shard_id = folly::Random::rand32(0, 16384);  // [min, max)
    auto resmap = testMgrPtr->QueryShardRoutingInfo(rand_shard_id);
    // hack here, 8 for 8 dataservers is hardcoded
    std::string expected_ip = "192.168.1." + std::to_string(rand_shard_id % 8 + 1);
    int expected_port = 30000 + (rand_shard_id % 8);
    EXPECT_EQ(resmap.size(), 1);
    EXPECT_EQ(resmap.at(rand_shard_id)->node_ip_, expected_ip);
    EXPECT_EQ(resmap.at(rand_shard_id)->node_port_, expected_port);
  }
  co_return;
}

folly::coro::Task<void> concurrentWriteTask(const CmSmPtr &testMgrPtr) {
  constexpr uint32_t kRandModifyNum = 5000;
  for (uint32_t i = 0; i < kRandModifyNum; ++i) {
    uint32_t rand_shard_id = folly::Random::rand32(0, 16384);  // [min, max)
    auto new_server =
        std::make_shared<simm::common::NodeAddress>("192.168.1." + std::to_string(folly::Random::rand32(0, 255)),
                                                    static_cast<int32_t>(folly::Random::rand32(30000, 40000)));
    auto ret = testMgrPtr->ModifyRoutingTable(rand_shard_id, new_server);
    EXPECT_EQ(ret, CommonErr::OK);
    // TODO(ytji) : maybe add some check for routing table after modify
  }
  co_return;
}

TEST_F(ClusterManagerShardManagerTest, TestQueryShardRoutingTable) {
  constexpr uint32_t kDataserverNum = 8;
  std::vector<std::shared_ptr<simm::common::NodeAddress>> all_servers;
  for (uint32_t i = 0; i < kDataserverNum; ++i) {
    auto server = std::make_shared<simm::common::NodeAddress>(("192.168.1." + std::to_string(i + 1)), (30000 + i));
    all_servers.push_back(server);
  }

  // 16384 shards loaded evenly on 8 dataservers
  // x % 8 == 0 - 192.168.1.1:40000
  // x % 8 == 1 - 192.168.1.2:40001
  // x % 8 == 2 - 192.168.1.3:40002
  // x % 8 == 3 - 192.168.1.4:40003
  // x % 8 == 4 - 192.168.1.5:40004
  // x % 8 == 5 - 192.168.1.6:40005
  // x % 8 == 6 - 192.168.1.7:40006
  // x % 8 == 7 - 192.168.1.8:40007
  error_code_t ret = cm_shard_manager_ptr_->InitShardRoutingTable(all_servers);
  EXPECT_EQ(ret, CommonErr::OK);

  // test single shard query
  shard_id_t target_shard = 0;
  simm::cm::QueryResultMap resmap = cm_shard_manager_ptr_->QueryShardRoutingInfo(target_shard);
  EXPECT_EQ(resmap.size(), 1);
  EXPECT_EQ(resmap.at(target_shard)->node_ip_, "192.168.1.1");
  EXPECT_EQ(resmap.at(target_shard)->node_port_, 30000);
  target_shard = 6235;
  resmap = cm_shard_manager_ptr_->QueryShardRoutingInfo(target_shard);
  EXPECT_EQ(resmap.size(), 1);
  EXPECT_EQ(resmap.at(target_shard)->node_ip_, "192.168.1.4");
  EXPECT_EQ(resmap.at(target_shard)->node_port_, 30003);
  target_shard = 16383;
  resmap = cm_shard_manager_ptr_->QueryShardRoutingInfo(target_shard);
  EXPECT_EQ(resmap.size(), 1);
  EXPECT_EQ(resmap.at(target_shard)->node_ip_, "192.168.1.8");
  EXPECT_EQ(resmap.at(target_shard)->node_port_, 30007);
  target_shard = 16384;
  resmap = cm_shard_manager_ptr_->QueryShardRoutingInfo(target_shard);
  EXPECT_EQ(resmap.size(), 0);

  // test batch shards query
  std::vector<shard_id_t> target_shards_vec = {0, 4089, 10253, 25555};
  resmap = cm_shard_manager_ptr_->BatchQueryShardRoutingInfos(target_shards_vec);
  // 25555 is invalid shard id, so it will be ignored
  // TODO(ytji): for now, it will return partial map, maybe change in future
  EXPECT_EQ(resmap.size(), 3);
  EXPECT_EQ(resmap.at(target_shards_vec.at(0))->node_ip_, "192.168.1.1");
  EXPECT_EQ(resmap.at(target_shards_vec.at(0))->node_port_, 30000);
  EXPECT_EQ(resmap.at(target_shards_vec.at(1))->node_ip_, "192.168.1.2");
  EXPECT_EQ(resmap.at(target_shards_vec.at(1))->node_port_, 30001);
  EXPECT_EQ(resmap.at(target_shards_vec.at(2))->node_ip_, "192.168.1.6");
  EXPECT_EQ(resmap.at(target_shards_vec.at(2))->node_port_, 30005);

  // test all shards query
  resmap = cm_shard_manager_ptr_->QueryAllShardRoutingInfos();
  EXPECT_EQ(resmap.size(), FLAGS_shard_total_num);
  EXPECT_EQ(resmap.size(), cm_shard_manager_ptr_->GetTotalShardNum());
  EXPECT_EQ(resmap.at(8196)->node_ip_, "192.168.1.5");
  EXPECT_EQ(resmap.at(8196)->node_port_, 30004);
  EXPECT_EQ(resmap.at(15999)->node_ip_, "192.168.1.8");
  EXPECT_EQ(resmap.at(15999)->node_port_, 30007);

  // test concurrent shards query with folly coro tasks
  folly::CPUThreadPoolExecutor threadPool{4, std::make_shared<folly::NamedThreadFactory>("TestQueryThreadPool")};
  folly::coro::blockingWait([&]() -> folly::coro::Task<void> {
    std::vector<folly::coro::TaskWithExecutor<void>> tasks;
    constexpr uint32_t kConcurrentTaskNum = 5;
    for (uint32_t i = 0; i < kConcurrentTaskNum; ++i) {
      tasks.push_back(co_withExecutor(&threadPool, concurrentReadTask(cm_shard_manager_ptr_)));
    }
    // co_await will trigger all tasks to run concurrently on specified
    // executor(threadpool)
    co_await folly::coro::collectAllWindowed(std::move(tasks), 5 /*parallel*/);
    // TODO(ytji): Why collectAll failed but collectAllWindowed succeed?
    // /home/tyji/code_repos/simm/third_party/folly/folly/coro/ViaIfAsync.h:519:16:
    // error: no type named ‘type’ in ‘struct std::enable_if<false, int>’ 519 |
    // int> = 0,
    //     |                ^
    // co_await folly::coro::collectAll(std::move(tasks));
  }());

  // cleanup
  cm_shard_manager_ptr_->CleanRoutingTable();
}

TEST_F(ClusterManagerShardManagerTest, TestModifyShardRoutingTable) {
  constexpr uint32_t kDataserverNum = 8;
  std::vector<std::shared_ptr<simm::common::NodeAddress>> all_servers;
  for (uint32_t i = 0; i < kDataserverNum; ++i) {
    auto server = std::make_shared<simm::common::NodeAddress>(("192.168.1." + std::to_string(i + 1)), (30000 + i));
    all_servers.push_back(server);
  }

  // 16384 shards loaded evenly on 8 dataservers
  // x % 8 == 0 - 192.168.1.1:40000
  // x % 8 == 1 - 192.168.1.2:40001
  // x % 8 == 2 - 192.168.1.3:40002
  // x % 8 == 3 - 192.168.1.4:40003
  // x % 8 == 4 - 192.168.1.5:40004
  // x % 8 == 5 - 192.168.1.6:40005
  // x % 8 == 6 - 192.168.1.7:40006
  // x % 8 == 7 - 192.168.1.8:40007
  error_code_t ret = cm_shard_manager_ptr_->InitShardRoutingTable(all_servers);
  EXPECT_EQ(ret, CommonErr::OK);

  // test single shard modify
  shard_id_t target_shard = 0;
  simm::cm::QueryResultMap resmap = cm_shard_manager_ptr_->QueryShardRoutingInfo(target_shard);
  EXPECT_EQ(resmap.size(), 1);
  EXPECT_EQ(resmap.at(target_shard)->node_ip_, "192.168.1.1");
  EXPECT_EQ(resmap.at(target_shard)->node_port_, 30000);
  auto new_server = std::make_shared<simm::common::NodeAddress>("192.168.1.3", 30002);
  ret = cm_shard_manager_ptr_->ModifyRoutingTable(target_shard, new_server);
  EXPECT_EQ(ret, CommonErr::OK);
  resmap = cm_shard_manager_ptr_->QueryShardRoutingInfo(target_shard);
  EXPECT_EQ(resmap.size(), 1);
  EXPECT_EQ(resmap.at(target_shard)->node_ip_, "192.168.1.3");
  EXPECT_EQ(resmap.at(target_shard)->node_port_, 30002);
  new_server->node_ip_ = "192.168.1.1";
  new_server->node_port_ = 30000;
  // revert modify
  ret = cm_shard_manager_ptr_->ModifyRoutingTable(target_shard, new_server);
  EXPECT_EQ(ret, CommonErr::OK);
  target_shard = 25555;  // non-existing shard id
  ret = cm_shard_manager_ptr_->ModifyRoutingTable(target_shard, new_server);
  EXPECT_EQ(ret, CommonErr::InvalidArgument);

  // test batch shards modify
  std::vector<shard_id_t> target_shards_vec = {1, 4094, 10248, 25555};
  std::vector<std::shared_ptr<simm::common::NodeAddress>> target_servers_vec;
  for (uint32_t i = 0; i < target_shards_vec.size(); ++i) {
    new_server->node_ip_ = "192.168.1.9";
    new_server->node_port_ = 30008;
    target_servers_vec.push_back(new_server);
  }
  ret = cm_shard_manager_ptr_->BatchModifyRoutingTable(target_shards_vec, target_servers_vec);
  EXPECT_EQ(ret, CommonErr::OK);
  resmap = cm_shard_manager_ptr_->BatchQueryShardRoutingInfos(target_shards_vec);
  EXPECT_EQ(resmap.size(), 3);
  EXPECT_EQ(resmap.at(target_shards_vec.at(0))->node_ip_, "192.168.1.9");
  EXPECT_EQ(resmap.at(target_shards_vec.at(0))->node_port_, 30008);
  EXPECT_EQ(resmap.at(target_shards_vec.at(1))->node_ip_, "192.168.1.9");
  EXPECT_EQ(resmap.at(target_shards_vec.at(1))->node_port_, 30008);
  EXPECT_EQ(resmap.at(target_shards_vec.at(2))->node_ip_, "192.168.1.9");
  EXPECT_EQ(resmap.at(target_shards_vec.at(2))->node_port_, 30008);

  // test concurrent shards modify with folly coro tasks
  folly::CPUThreadPoolExecutor threadPool{4, std::make_shared<folly::NamedThreadFactory>("TestModifyThreadPool")};
  folly::coro::blockingWait([&]() -> folly::coro::Task<void> {
    std::vector<folly::coro::TaskWithExecutor<void>> tasks;
    constexpr uint32_t kConcurrentTaskNum = 5;
    for (uint32_t i = 0; i < kConcurrentTaskNum; ++i) {
      tasks.push_back(co_withExecutor(&threadPool, concurrentWriteTask(cm_shard_manager_ptr_)));
    }
    // co_await will trigger all tasks to run concurrently on specified
    // executor(threadpool)
    co_await folly::coro::collectAllWindowed(std::move(tasks), 5 /*parallel*/);
    // TODO(ytji): Why collectAll failed but collectAllWindowed succeed?
    // /home/tyji/code_repos/simm/third_party/folly/folly/coro/ViaIfAsync.h:519:16:
    // error: no type named ‘type’ in ‘struct std::enable_if<false, int>’ 519 |
    // int> = 0,
    //     |                ^
    // co_await folly::coro::collectAll(std::move(tasks));
  }());

  // cleanup
  cm_shard_manager_ptr_->CleanRoutingTable();
}

TEST_F(ClusterManagerShardManagerTest, TestFailureBelowMinMarksDeadNodeShardsUnavailable) {
  auto old_min_ds = FLAGS_dataserver_min_num;
  auto restore_flag = folly::makeGuard([&]() { FLAGS_dataserver_min_num = old_min_ds; });
  FLAGS_dataserver_min_num = 3;

  std::vector<std::shared_ptr<simm::common::NodeAddress>> all_servers;
  for (uint32_t i = 0; i < 3; ++i) {
    all_servers.push_back(std::make_shared<simm::common::NodeAddress>("192.168.2." + std::to_string(i + 1), 32000 + i));
  }

  ASSERT_EQ(cm_shard_manager_ptr_->InitShardRoutingTable(all_servers), CommonErr::OK);
  const auto failed_node = all_servers[0]->toString();

  std::vector<std::shared_ptr<simm::common::NodeAddress>> alive_servers{all_servers[1], all_servers[2]};
  EXPECT_EQ(cm_shard_manager_ptr_->RebalanceShardsAfterNodeFailure({failed_node}, alive_servers),
            CmErr::InsufficientDataservers);

  auto routing_info = cm_shard_manager_ptr_->QueryAllShardRoutingInfos();
  uint32_t unavailable_shards = 0;
  for (const auto &[shard_id, node_addr] : routing_info) {
    if (!node_addr) {
      ++unavailable_shards;
      continue;
    }
    EXPECT_NE(node_addr->toString(), failed_node);
  }
  EXPECT_GT(unavailable_shards, 0U);
}

TEST_F(ClusterManagerShardManagerTest, TestFailureWithNoAliveServersMarksAllFailedNodeShardsUnavailable) {
  std::vector<std::shared_ptr<simm::common::NodeAddress>> all_servers;
  for (uint32_t i = 0; i < 3; ++i) {
    all_servers.push_back(std::make_shared<simm::common::NodeAddress>("192.168.3." + std::to_string(i + 1), 33000 + i));
  }

  ASSERT_EQ(cm_shard_manager_ptr_->InitShardRoutingTable(all_servers), CommonErr::OK);

  std::vector<std::string> failed_nodes;
  for (const auto &server : all_servers) {
    failed_nodes.push_back(server->toString());
  }

  EXPECT_EQ(cm_shard_manager_ptr_->RebalanceShardsAfterNodeFailure(failed_nodes, {}), CmErr::NoAvailableDataservers);

  auto routing_info = cm_shard_manager_ptr_->QueryAllShardRoutingInfos();
  ASSERT_EQ(routing_info.size(), FLAGS_shard_total_num);
  for (const auto &[shard_id, node_addr] : routing_info) {
    EXPECT_EQ(node_addr, nullptr) << "Shard " << shard_id << " should be unavailable when no dataservers are alive";
  }
}

// copy from folly example test case
TEST_F(ClusterManagerShardManagerTest, TestVectorOfTaskWithExecutorUsage) {
  folly::CPUThreadPoolExecutor threadPool{4, std::make_shared<folly::NamedThreadFactory>("TestThreadPool")};

  folly::coro::blockingWait([&]() -> folly::coro::Task<void> {
    std::vector<folly::coro::TaskWithExecutor<int>> tasks;
    for (int i = 0; i < 4; ++i) {
      tasks.push_back(co_withExecutor(&threadPool, [](int idx) -> folly::coro::Task<int> { co_return idx + 1; }(i)));
    }

    auto results = co_await folly::coro::collectAllWindowed(std::move(tasks), 2);
    EXPECT_TRUE(results.size() == 4);
    EXPECT_TRUE(results[0] == 1);
    EXPECT_TRUE(results[1] == 2);
    EXPECT_TRUE(results[2] == 3);
    EXPECT_TRUE(results[3] == 4);
  }());
}

}  // namespace cm
}  // namespace simm

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
