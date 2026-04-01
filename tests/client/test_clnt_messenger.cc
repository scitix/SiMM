#include <atomic>
#include <thread>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "client/clnt_messenger.h"
#include "rpc/connection.h"
#include "rpc/rpc.h"
#include "rpc/rpc_context.h"

namespace simm {
namespace clnt {

namespace {

class FakeConnection : public sicl::rpc::Connection {
 public:
  explicit FakeConnection(std::string name) : name_(std::move(name)) {}

  sicl::transport::Result send(const google::protobuf::Message &,
                               sicl::transport::MsgTag,
                               sicl::SendCallback,
                               sicl::transport::Channel *) const override {
    return sicl::transport::SICL_SUCCESS;
  }

  sicl::transport::Result read(const void *,
                               const size_t,
                               const uint64_t,
                               const std::vector<uint32_t> &,
                               sicl::ReadCallback,
                               sicl::RequestParam) const override {
    return sicl::transport::SICL_SUCCESS;
  }

  sicl::transport::Result write(const void *,
                                const size_t,
                                const uint64_t,
                                const std::vector<uint32_t> &,
                                sicl::WriteCallback,
                                sicl::RequestParam) const override {
    return sicl::transport::SICL_SUCCESS;
  }

  sicl::transport::Result write_imm(const uint32_t, sicl::WriteCallback) override {
    return sicl::transport::SICL_SUCCESS;
  }

  bool addChannel(sicl::transport::Channel *) override { return true; }
  sicl::transport::Channel *getAChannel() const override { return nullptr; }
  size_t getChannelCount() const override { return 1; }
  sicl::transport::Result remove(const sicl::transport::Channel *) override { return sicl::transport::SICL_SUCCESS; }
  bool hasChannel(const sicl::transport::Channel *) const override { return false; }
  std::string toString() const override { return name_; }
  void SendResponse(const google::protobuf::Message &,
                    std::shared_ptr<sicl::rpc::RpcContext>,
                    sicl::rpc::RpcResponseDoneFn) const override {}
  uuids::uuid getGroupID() const override { return {}; }
  std::pair<const std::string, const int> getIPPort() const override { return {"127.0.0.1", 12345}; }
  sicl::transport::Result recv_large(sicl::rpc::SiRPC &,
                                     void *,
                                     size_t,
                                     std::function<void(void *, size_t)>) const override {
    return sicl::transport::SICL_SUCCESS;
  }

 private:
  std::string name_;
};

std::shared_ptr<sicl::rpc::RpcContext> MakeFailedRpcContext(int error_code) {
  sicl::rpc::RpcContext *ctx_raw = nullptr;
  sicl::rpc::RpcContext::newInstance(ctx_raw);
  auto ctx = std::shared_ptr<sicl::rpc::RpcContext>(ctx_raw);
  ctx->SetError(error_code, "mock failure");
  return ctx;
}

std::shared_ptr<QueryShardRoutingTableAllResponsePB> BuildRoutingResponse(
    const std::vector<std::pair<std::string, std::vector<uint16_t>>> &routing_entries) {
  auto resp = std::make_shared<QueryShardRoutingTableAllResponsePB>();
  resp->set_ret_code(CommonErr::OK);
  for (const auto &[addr, shard_ids] : routing_entries) {
    auto node_addr = simm::common::NodeAddress::ParseFromString(addr);
    EXPECT_TRUE(node_addr.has_value());
    if (!node_addr.has_value()) {
      continue;
    }
    auto *entry = resp->add_shard_info();
    entry->mutable_data_server_address()->set_ip(node_addr->node_ip_);
    entry->mutable_data_server_address()->set_port(node_addr->node_port_);
    for (auto shard_id : shard_ids) {
      entry->add_shard_ids(shard_id);
    }
  }
  return resp;
}

}  // namespace

class ClientMessengerTestPeer {
 public:
  static void ResetState() {
    auto &messenger = ClientMessenger::Instance();
    messenger.shard_table_.clear();
    messenger.ds_conn_ctxs_.clear();
    messenger.cm_addr_.clear();
    messenger.initialized_ = false;
    messenger.test_get_cm_address_hook_ = nullptr;
    messenger.test_route_query_hook_ = nullptr;
    messenger.test_build_connection_hook_ = nullptr;
  }

  static void InstallDsContext(const std::string &addr,
                               bool active,
                               size_t gen_num,
                               const std::vector<uint16_t> &shard_ids,
                               std::shared_ptr<sicl::rpc::Connection> connection = nullptr) {
    auto &messenger = ClientMessenger::Instance();
    auto ds_ctx = messenger.GetOrCreateConnectionContext(addr);
    ds_ctx->StoreConnection(std::move(connection));
    ds_ctx->active.store(active);
    ds_ctx->gen_num.store(gen_num);
    for (auto shard_id : shard_ids) {
      messenger.shard_table_.insert_or_assign(shard_id, ds_ctx);
    }
  }

  static bool HasDsContext(const std::string &addr) {
    auto &messenger = ClientMessenger::Instance();
    return messenger.ds_conn_ctxs_.find(addr) != messenger.ds_conn_ctxs_.end();
  }

  static bool IsDsActive(const std::string &addr) {
    auto &messenger = ClientMessenger::Instance();
    auto it = messenger.ds_conn_ctxs_.find(addr);
    return it != messenger.ds_conn_ctxs_.end() && it->second->active.load();
  }

  static std::shared_ptr<sicl::rpc::Connection> GetConnection(const std::string &addr) {
    auto &messenger = ClientMessenger::Instance();
    auto it = messenger.ds_conn_ctxs_.find(addr);
    return it == messenger.ds_conn_ctxs_.end() ? nullptr : it->second->LoadConnection();
  }

  static void SetConnection(const std::string &addr, std::shared_ptr<sicl::rpc::Connection> connection) {
    ClientMessenger::Instance().GetOrCreateConnectionContext(addr)->StoreConnection(std::move(connection));
  }

  static void MarkConnectionActive(const std::string &addr, bool active, size_t gen_num) {
    auto ds_ctx = ClientMessenger::Instance().GetOrCreateConnectionContext(addr);
    ds_ctx->active.store(active);
    ds_ctx->gen_num.store(gen_num);
  }

  static std::string ShardOwner(uint16_t shard_id) {
    auto &messenger = ClientMessenger::Instance();
    auto it = messenger.shard_table_.find(shard_id);
    return it == messenger.shard_table_.end() ? "" : it->second->ip_port;
  }

  static void PruneConnections(const std::vector<std::string> &live_servers) {
    ClientMessenger::Instance().PruneStaleConnectionContexts(
        std::unordered_set<std::string>(live_servers.begin(), live_servers.end()));
  }

  static void HandleAsyncFailure(std::shared_ptr<sicl::rpc::RpcContext> rpc_ctx,
                                 const std::string &request_addr,
                                 uint16_t shard_id,
                                 size_t old_conn_gen_num) {
    auto &messenger = ClientMessenger::Instance();
    auto it = messenger.ds_conn_ctxs_.find(request_addr);
    if (it != messenger.ds_conn_ctxs_.end()) {
      messenger.HandleAsyncRequestFailure(std::move(rpc_ctx), it->second, shard_id, old_conn_gen_num);
    }
  }

  static void SetGetCmAddressHook(std::function<std::string()> hook) {
    ClientMessenger::Instance().test_get_cm_address_hook_ = std::move(hook);
  }

  static void SetRouteQueryHook(
      std::function<std::pair<error_code_t, std::shared_ptr<QueryShardRoutingTableAllResponsePB>>(const std::string &)>
          hook) {
    ClientMessenger::Instance().test_route_query_hook_ = std::move(hook);
  }

  static void SetBuildConnectionHook(std::function<error_code_t(const std::string &)> hook) {
    ClientMessenger::Instance().test_build_connection_hook_ = std::move(hook);
  }

  static error_code_t ReInit() { return ClientMessenger::Instance().ReInit(); }

  static bool IsInitialized() { return ClientMessenger::Instance().initialized_; }

  static std::string CmAddr() { return ClientMessenger::Instance().cm_addr_; }
};

class ClientMessengerUnitTest : public ::testing::Test {
 protected:
  void SetUp() override { ClientMessengerTestPeer::ResetState(); }

  void TearDown() override { ClientMessengerTestPeer::ResetState(); }
};

TEST_F(ClientMessengerUnitTest, PruneStaleConnectionsRemovesDeadDataservers) {
  auto conn_a = std::make_shared<FakeConnection>("a");
  auto conn_b = std::make_shared<FakeConnection>("b");
  auto conn_c = std::make_shared<FakeConnection>("c");

  ClientMessengerTestPeer::InstallDsContext("10.0.0.1:1001", true, 1, {0}, conn_a);
  ClientMessengerTestPeer::InstallDsContext("10.0.0.2:1002", true, 1, {1}, conn_b);
  ClientMessengerTestPeer::InstallDsContext("10.0.0.3:1003", true, 1, {2}, conn_c);

  ClientMessengerTestPeer::PruneConnections({"10.0.0.2:1002", "10.0.0.3:1003"});

  EXPECT_FALSE(ClientMessengerTestPeer::HasDsContext("10.0.0.1:1001"));
  EXPECT_TRUE(ClientMessengerTestPeer::HasDsContext("10.0.0.2:1002"));
  EXPECT_TRUE(ClientMessengerTestPeer::HasDsContext("10.0.0.3:1003"));
}

TEST_F(ClientMessengerUnitTest, AsyncFailureUsesRequestTimeDataserverContext) {
  auto old_conn = std::make_shared<FakeConnection>("old");
  auto new_conn = std::make_shared<FakeConnection>("new");

  ClientMessengerTestPeer::InstallDsContext("10.0.0.1:1001", true, 7, {}, old_conn);
  ClientMessengerTestPeer::InstallDsContext("10.0.0.2:1002", true, 7, {0}, new_conn);

  auto rpc_ctx = MakeFailedRpcContext(sicl::transport::SICL_ERR_INVALID_STATE);
  ClientMessengerTestPeer::HandleAsyncFailure(rpc_ctx, "10.0.0.1:1001", 0, 7);

  EXPECT_FALSE(ClientMessengerTestPeer::IsDsActive("10.0.0.1:1001"));
  EXPECT_TRUE(ClientMessengerTestPeer::IsDsActive("10.0.0.2:1002"));
  EXPECT_EQ(ClientMessengerTestPeer::ShardOwner(0), "10.0.0.2:1002");
}

TEST_F(ClientMessengerUnitTest, ConnectionAccessorsStayConsistentDuringConcurrentSwap) {
  auto conn_a = std::make_shared<FakeConnection>("a");
  auto conn_b = std::make_shared<FakeConnection>("b");
  ClientMessengerTestPeer::InstallDsContext("10.0.0.1:1001", true, 1, {0}, conn_a);

  std::atomic<bool> stop{false};
  std::thread writer([&]() {
    for (int i = 0; i < 20000; ++i) {
      ClientMessengerTestPeer::SetConnection("10.0.0.1:1001", (i % 2 == 0) ? conn_a : conn_b);
    }
    stop.store(true);
  });

  std::thread reader([&]() {
    while (!stop.load()) {
      auto current = ClientMessengerTestPeer::GetConnection("10.0.0.1:1001");
      ASSERT_TRUE(current == conn_a || current == conn_b);
    }
  });

  writer.join();
  reader.join();

  auto final_conn = ClientMessengerTestPeer::GetConnection("10.0.0.1:1001");
  EXPECT_TRUE(final_conn == conn_a || final_conn == conn_b);
}

TEST_F(ClientMessengerUnitTest, ReInitRefreshesRoutingBuildsConnectionsAndPrunesStaleServers) {
  auto healthy_conn = std::make_shared<FakeConnection>("healthy");
  auto inactive_conn = std::make_shared<FakeConnection>("inactive");
  auto stale_conn = std::make_shared<FakeConnection>("stale");

  ClientMessengerTestPeer::InstallDsContext("10.0.0.1:1001", true, 10, {0}, healthy_conn);
  ClientMessengerTestPeer::InstallDsContext("10.0.0.2:1002", false, 4, {1}, inactive_conn);
  ClientMessengerTestPeer::InstallDsContext("10.0.0.9:1999", true, 2, {9}, stale_conn);

  std::vector<std::string> built_servers_order;
  std::unordered_set<std::string> built_servers;
  ClientMessengerTestPeer::SetGetCmAddressHook([]() { return std::string("10.0.0.100:9000"); });
  ClientMessengerTestPeer::SetRouteQueryHook([](const std::string &) {
    return std::make_pair(
        CommonErr::OK, BuildRoutingResponse({{"10.0.0.1:1001", {0}}, {"10.0.0.2:1002", {1}}, {"10.0.0.3:1003", {2}}}));
  });
  ClientMessengerTestPeer::SetBuildConnectionHook([&](const std::string &addr) {
    built_servers.insert(addr);
    built_servers_order.push_back(addr);
    auto conn = std::make_shared<FakeConnection>(addr);
    ClientMessengerTestPeer::SetConnection(addr, conn);
    ClientMessengerTestPeer::MarkConnectionActive(addr, true, 100 + built_servers_order.size());
    return CommonErr::OK;
  });

  EXPECT_EQ(ClientMessengerTestPeer::ReInit(), CommonErr::OK);

  EXPECT_TRUE(ClientMessengerTestPeer::IsInitialized());
  EXPECT_EQ(ClientMessengerTestPeer::CmAddr(), "10.0.0.100:9000");
  EXPECT_FALSE(ClientMessengerTestPeer::HasDsContext("10.0.0.9:1999"));
  EXPECT_TRUE(ClientMessengerTestPeer::HasDsContext("10.0.0.1:1001"));
  EXPECT_TRUE(ClientMessengerTestPeer::HasDsContext("10.0.0.2:1002"));
  EXPECT_TRUE(ClientMessengerTestPeer::HasDsContext("10.0.0.3:1003"));
  EXPECT_TRUE(ClientMessengerTestPeer::IsDsActive("10.0.0.1:1001"));
  EXPECT_TRUE(ClientMessengerTestPeer::IsDsActive("10.0.0.2:1002"));
  EXPECT_TRUE(ClientMessengerTestPeer::IsDsActive("10.0.0.3:1003"));
  EXPECT_EQ(ClientMessengerTestPeer::GetConnection("10.0.0.1:1001"), healthy_conn);
  EXPECT_EQ(ClientMessengerTestPeer::ShardOwner(0), "10.0.0.1:1001");
  EXPECT_EQ(ClientMessengerTestPeer::ShardOwner(1), "10.0.0.2:1002");
  EXPECT_EQ(ClientMessengerTestPeer::ShardOwner(2), "10.0.0.3:1003");
  EXPECT_EQ(built_servers.size(), 2);
  EXPECT_TRUE(built_servers.contains("10.0.0.2:1002"));
  EXPECT_TRUE(built_servers.contains("10.0.0.3:1003"));
  EXPECT_FALSE(built_servers.contains("10.0.0.1:1001"));
}

TEST_F(ClientMessengerUnitTest, ReInitFailsWhenRouteQueryFailsAndClearsCmAddress) {
  ClientMessengerTestPeer::SetGetCmAddressHook([]() { return std::string("10.0.0.100:9000"); });
  ClientMessengerTestPeer::SetRouteQueryHook([](const std::string &) {
    return std::make_pair(ClntErr::GetRoutingTableFailed, std::shared_ptr<QueryShardRoutingTableAllResponsePB>{});
  });

  EXPECT_EQ(ClientMessengerTestPeer::ReInit(), CommonErr::InvalidState);
  EXPECT_FALSE(ClientMessengerTestPeer::IsInitialized());
  EXPECT_EQ(ClientMessengerTestPeer::CmAddr(), "");
}

TEST_F(ClientMessengerUnitTest, ReInitRemovesStaleConnectionFromLocalContextMap) {
  ClientMessengerTestPeer::InstallDsContext("10.0.0.1:1001", true, 1, {0}, std::make_shared<FakeConnection>("old"));
  ClientMessengerTestPeer::InstallDsContext("10.0.0.9:1999", true, 2, {9}, std::make_shared<FakeConnection>("stale"));

  ClientMessengerTestPeer::SetGetCmAddressHook([]() { return std::string("10.0.0.100:9000"); });
  ClientMessengerTestPeer::SetRouteQueryHook([](const std::string &) {
    return std::make_pair(CommonErr::OK, BuildRoutingResponse({{"10.0.0.1:1001", {0, 1}}}));
  });
  ClientMessengerTestPeer::SetBuildConnectionHook([](const std::string &) { return CommonErr::OK; });

  EXPECT_EQ(ClientMessengerTestPeer::ReInit(), CommonErr::OK);
  EXPECT_TRUE(ClientMessengerTestPeer::HasDsContext("10.0.0.1:1001"));
  EXPECT_FALSE(ClientMessengerTestPeer::HasDsContext("10.0.0.9:1999"));
  EXPECT_EQ(ClientMessengerTestPeer::ShardOwner(0), "10.0.0.1:1001");
  EXPECT_EQ(ClientMessengerTestPeer::ShardOwner(1), "10.0.0.1:1001");
}

}  // namespace clnt
}  // namespace simm

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
