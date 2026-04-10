#include <chrono>
#include <memory>
#include <thread>
#include <utility>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <folly/ScopeGuard.h>

#include "cluster_manager/cm_rpc_handler.h"
#include "cluster_manager/cm_node_manager.h"
#include "common/logging/logging.h"
#include "proto/ds_cm_rpcs.pb.h"
#include "rpc/connection.h"
#include "rpc/rpc.h"

DECLARE_LOG_MODULE("cluster_manager_test");

DECLARE_uint32(dataserver_resource_interval_inSecs);
DECLARE_string(cm_log_file);
DECLARE_int32(mgt_service_port);

namespace simm {
namespace cm {

class ClusterManagerNodeManagerTestPeer {
 public:
  static void SetResourceQueryHook(
      ClusterManagerNodeManager &node_manager,
      std::function<std::shared_ptr<simm::common::NodeResource>(const std::string &)> hook) {
    node_manager.test_resource_query_hook_ = std::move(hook);
  }
};

namespace {

template <typename Predicate>
bool WaitUntil(Predicate pred,
               std::chrono::milliseconds timeout,
               std::chrono::milliseconds poll_interval = std::chrono::milliseconds(50)) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (pred()) {
      return true;
    }
    std::this_thread::sleep_for(poll_interval);
  }
  return pred();
}

}  // namespace

class DelayedResourceQueryHandler : public sicl::rpc::HandlerBase {
 public:
  DelayedResourceQueryHandler(sicl::rpc::SiRPC *service, std::chrono::milliseconds delay)
      : sicl::rpc::HandlerBase(service, new DataServerResourceRequestPB()), delay_(delay) {}

  void Work(const std::shared_ptr<sicl::rpc::RpcContext> ctx,
            const std::shared_ptr<sicl::rpc::Connection> conn,
            const google::protobuf::Message *request) const override {
    (void)request;
    auto rsp = std::make_shared<DataServerResourceResponsePB>();
    rsp->set_mem_total_bytes(4096);
    rsp->set_mem_allocated_bytes(3584);
    rsp->set_mem_free_bytes(1024);
    rsp->set_mem_used_bytes(3072);
    rsp->set_last_report_timestamp_us(123456);
    auto *shard = rsp->add_shard_mem_infos();
    shard->set_shard_id(7);
    shard->set_shard_mem_used_bytes(2048);
    std::this_thread::sleep_for(delay_);
    conn->SendResponse(*rsp, ctx, [rsp](std::shared_ptr<sicl::rpc::RpcContext>) {});
  }

 private:
  std::chrono::milliseconds delay_;
};

class ClusterManagerNodeManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    simm::logging::LogConfig cm_log_config = simm::logging::LogConfig{FLAGS_cm_log_file, "DEBUG"};
    simm::logging::LoggerManager::Instance().UpdateConfig("cluster_manager", cm_log_config);
    old_interval_ = FLAGS_dataserver_resource_interval_inSecs;
    FLAGS_dataserver_resource_interval_inSecs = 1;
  }

  void TearDown() override { FLAGS_dataserver_resource_interval_inSecs = old_interval_; }

 private:
  uint32_t old_interval_{0};
};

TEST_F(ClusterManagerNodeManagerTest, TestResourcePollingRefreshesNodeAndShardStats) {
  auto node_manager = std::make_shared<ClusterManagerNodeManager>();
  std::atomic<int> generation{0};
  ClusterManagerNodeManagerTestPeer::SetResourceQueryHook(*node_manager, [&generation](const std::string &addr_str) {
    EXPECT_EQ(addr_str, "127.0.0.1:44110");
    if (generation.load() == 0) {
      return std::make_shared<simm::common::NodeResource>(
          1024, 896, 768, 256, 100, std::vector<simm::common::NodeResource::ShardMemResource>{{1, 128}, {8, 640}});
    }
    return std::make_shared<simm::common::NodeResource>(
        2048, 1024, 512, 1536, 200, std::vector<simm::common::NodeResource::ShardMemResource>{{3, 512}});
  });

  ASSERT_EQ(node_manager->AddNode("127.0.0.1:44110"), CommonErr::OK);
  node_manager->Init();
  auto cleanup = folly::makeGuard([&]() { node_manager->Stop(); });

  ASSERT_TRUE(WaitUntil(
      [&]() {
        auto resource = node_manager->GetNodeResource("127.0.0.1:44110");
        return resource != nullptr && resource->mem_used_bytes_ == 768 && resource->shard_mem_infos_.size() == 2;
      },
      std::chrono::seconds(3)));

  auto resource = node_manager->GetNodeResource("127.0.0.1:44110");
  ASSERT_NE(resource, nullptr);
  EXPECT_EQ(resource->mem_total_bytes_, 1024);
  EXPECT_EQ(resource->mem_allocated_bytes_, 896);
  EXPECT_EQ(resource->mem_free_bytes_, 256);
  EXPECT_EQ(resource->mem_used_bytes_, 768);
  EXPECT_EQ(resource->last_report_timestamp_us_, 100);
  ASSERT_EQ(resource->shard_mem_infos_.size(), 2);
  EXPECT_EQ(resource->shard_mem_infos_[0].shard_id_, 1);
  EXPECT_EQ(resource->shard_mem_infos_[0].mem_used_bytes_, 128);
  EXPECT_EQ(resource->shard_mem_infos_[1].shard_id_, 8);
  EXPECT_EQ(resource->shard_mem_infos_[1].mem_used_bytes_, 640);

  generation.store(1);
  ASSERT_TRUE(WaitUntil(
      [&]() {
        auto updated = node_manager->GetNodeResource("127.0.0.1:44110");
        return updated != nullptr && updated->mem_used_bytes_ == 512 && updated->last_report_timestamp_us_ == 200 &&
               updated->shard_mem_infos_.size() == 1 && updated->shard_mem_infos_[0].shard_id_ == 3;
      },
      std::chrono::seconds(3)));
}

TEST_F(ClusterManagerNodeManagerTest, TestRefreshNodeResourceWaitsForResponseCompletion) {
  auto old_mgt_port = FLAGS_mgt_service_port;
  auto restore_port = folly::makeGuard([&]() { FLAGS_mgt_service_port = old_mgt_port; });
  FLAGS_mgt_service_port = 44111;

  sicl::rpc::SiRPC *server_raw = nullptr;
  ASSERT_EQ(sicl::rpc::SiRPC::newInstance(server_raw, true), sicl::transport::Result::SICL_SUCCESS);
  std::unique_ptr<sicl::rpc::SiRPC> server(server_raw);
  ASSERT_TRUE(server->RegisterHandler(
      static_cast<sicl::rpc::ReqType>(ClusterManagerRpcType::RPC_DATASERVER_RESOURCE_QUERY),
      new DelayedResourceQueryHandler(server.get(), std::chrono::milliseconds(200))));
  ASSERT_EQ(server->Start(FLAGS_mgt_service_port), 0);

  std::jthread server_thread([&]() { server->RunUntilAskedToQuit(); });
  auto server_cleanup = folly::makeGuard([&]() { server->Stop(); });

  auto node_manager = std::make_shared<ClusterManagerNodeManager>();
  auto resource = node_manager->RefreshNodeResource("127.0.0.1:44111");

  ASSERT_NE(resource, nullptr);
  EXPECT_EQ(resource->mem_total_bytes_, 4096);
  EXPECT_EQ(resource->mem_allocated_bytes_, 3584);
  EXPECT_EQ(resource->mem_free_bytes_, 1024);
  EXPECT_EQ(resource->mem_used_bytes_, 3072);
  EXPECT_EQ(resource->last_report_timestamp_us_, 123456);
  ASSERT_EQ(resource->shard_mem_infos_.size(), 1);
  EXPECT_EQ(resource->shard_mem_infos_[0].shard_id_, 7);
  EXPECT_EQ(resource->shard_mem_infos_[0].mem_used_bytes_, 2048);
}

TEST_F(ClusterManagerNodeManagerTest, TestRefreshNodeResourceUsesManagementPortForIoAddress) {
  auto old_mgt_port = FLAGS_mgt_service_port;
  auto restore_ports = folly::makeGuard([&]() { FLAGS_mgt_service_port = old_mgt_port; });
  FLAGS_mgt_service_port = 44121;

  sicl::rpc::SiRPC *server_raw = nullptr;
  ASSERT_EQ(sicl::rpc::SiRPC::newInstance(server_raw, true), sicl::transport::Result::SICL_SUCCESS);
  std::unique_ptr<sicl::rpc::SiRPC> server(server_raw);
  ASSERT_TRUE(server->RegisterHandler(
      static_cast<sicl::rpc::ReqType>(ClusterManagerRpcType::RPC_DATASERVER_RESOURCE_QUERY),
      new DelayedResourceQueryHandler(server.get(), std::chrono::milliseconds(10))));
  ASSERT_EQ(server->Start(FLAGS_mgt_service_port), 0);

  std::jthread server_thread([&]() { server->RunUntilAskedToQuit(); });
  auto server_cleanup = folly::makeGuard([&]() { server->Stop(); });

  auto node_manager = std::make_shared<ClusterManagerNodeManager>();
  auto resource = node_manager->RefreshNodeResource("127.0.0.1:44120");

  ASSERT_NE(resource, nullptr);
  EXPECT_EQ(resource->mem_total_bytes_, 4096);
  EXPECT_EQ(resource->mem_allocated_bytes_, 3584);
  EXPECT_EQ(resource->mem_used_bytes_, 3072);
  EXPECT_EQ(resource->last_report_timestamp_us_, 123456);
}

TEST_F(ClusterManagerNodeManagerTest, TestAddNodeIsIdempotentForExistingAddress) {
  auto node_manager = std::make_shared<ClusterManagerNodeManager>();
  ASSERT_EQ(node_manager->AddNode("127.0.0.1:44112"), CommonErr::OK);
  ASSERT_EQ(node_manager->AddNode("127.0.0.1:44112"), CommonErr::OK);

  auto status_map = node_manager->GetAllNodeStatus();
  ASSERT_EQ(status_map.size(), 1);
  EXPECT_EQ(status_map.at("127.0.0.1:44112"), NodeStatus::RUNNING);
}

}  // namespace cm
}  // namespace simm
