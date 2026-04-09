#include <atomic>
#include <thread>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "client/clnt_messenger.h"
#include "data_server/kv_rpc_handler.h"
#include "proto/ds_clnt_rpcs.pb.h"
#include "rpc/connection.h"
#include "rpc/rpc.h"
#include "rpc/rpc_context.h"

DECLARE_bool(clnt_syncreq_enable_retry);
DECLARE_uint32(clnt_syncreq_retry_count);
DECLARE_bool(clnt_use_k8s);
DECLARE_string(cm_primary_node_ip);
DECLARE_int32(cm_rpc_inter_port);
DECLARE_uint32(shard_total_num);
DECLARE_uint32(clnt_deferred_reshard_wait_inSecs);
DECLARE_uint32(clnt_cm_addr_check_interval_inSecs);

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

class FakeSiRPC : public sicl::rpc::SiRPC {
 public:
  std::shared_ptr<sicl::rpc::Connection> connect(const std::string &, const int) override {
    connect_calls_.fetch_add(1);
    if (connect_delay_.count() > 0) {
      std::this_thread::sleep_for(connect_delay_);
    }
    if (connect_handler_) {
      return connect_handler_();
    }
    return std::make_shared<FakeConnection>("fake-connect");
  }

  std::shared_ptr<sicl::rpc::Connection> connect(const std::string &, const int, const int, const int) override {
    return nullptr;
  }

  size_t addConnect(std::shared_ptr<sicl::rpc::Connection>, size_t count) override { return count; }

  void SendRequest(const std::shared_ptr<sicl::rpc::Connection>,
                   const sicl::rpc::ReqType,
                   const google::protobuf::Message &,
                   google::protobuf::Message *rsp,
                   const std::shared_ptr<sicl::rpc::RpcContext> ctx,
                   const sicl::rpc::RpcDoneFn done) override {
    async_send_calls_.fetch_add(1);
    if (async_send_handler_) {
      async_send_handler_(ctx);
    } else {
      ctx->SetError(sicl::transport::SICL_SUCCESS, "");
    }
    done(rsp, ctx);
  }

  void SendRequest(const std::shared_ptr<sicl::rpc::Connection>,
                   const sicl::rpc::ReqType,
                   const google::protobuf::Message &,
                   google::protobuf::Message *,
                   const std::shared_ptr<sicl::rpc::RpcContext> ctx) override {
    sync_send_calls_.fetch_add(1);
    if (sync_send_handler_) {
      sync_send_handler_(ctx);
    } else {
      ctx->SetError(sicl::transport::SICL_SUCCESS, "");
    }
  }

  void SendRequest(const std::string &,
                   const int,
                   const sicl::rpc::ReqType,
                   const google::protobuf::Message &,
                   google::protobuf::Message *,
                   const std::shared_ptr<sicl::rpc::RpcContext>,
                   const sicl::rpc::RpcDoneFn) override {}

  int Stop() override { return 0; }
  int Start(const int) override { return 0; }
  void RunUntilAskedToQuit() override {}
  bool RegisterHandler(const sicl::rpc::ReqType, const sicl::rpc::HandlerBase *) override { return true; }
  void HandleRequest(const std::shared_ptr<sicl::rpc::RpcContext>,
                     const std::shared_ptr<sicl::rpc::Connection>,
                     const sicl::rpc::ReqType,
                     const void *,
                     const size_t) override {}
  std::vector<sicl::transport::IbvDevice *> GetAllDevices() const override { return {}; }
  sicl::transport::Mempool *GetMempool() const override { return nullptr; }

  void SetConnectHandler(std::function<std::shared_ptr<sicl::rpc::Connection>()> handler) {
    connect_handler_ = std::move(handler);
  }

  void SetSyncSendHandler(std::function<void(const std::shared_ptr<sicl::rpc::RpcContext> &)> handler) {
    sync_send_handler_ = std::move(handler);
  }

  void SetAsyncSendHandler(std::function<void(const std::shared_ptr<sicl::rpc::RpcContext> &)> handler) {
    async_send_handler_ = std::move(handler);
  }

  void SetConnectDelay(std::chrono::milliseconds delay) { connect_delay_ = delay; }

  int ConnectCalls() const { return connect_calls_.load(); }
  int SyncSendCalls() const { return sync_send_calls_.load(); }
  int AsyncSendCalls() const { return async_send_calls_.load(); }

 private:
  std::atomic<int> connect_calls_{0};
  std::atomic<int> sync_send_calls_{0};
  std::atomic<int> async_send_calls_{0};
  std::chrono::milliseconds connect_delay_{0};
  std::function<std::shared_ptr<sicl::rpc::Connection>()> connect_handler_{};
  std::function<void(const std::shared_ptr<sicl::rpc::RpcContext> &)> sync_send_handler_{};
  std::function<void(const std::shared_ptr<sicl::rpc::RpcContext> &)> async_send_handler_{};
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
    // clear dead-since state between tests
    std::lock_guard lg(messenger.ds_dead_since_mtx_);
    messenger.ds_dead_since_.clear();
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

  static std::string GetCmAddress() { return ClientMessenger::Instance().get_cm_address(); }

  static sicl::rpc::SiRPC *SwapRpcClient(sicl::rpc::SiRPC *replacement) {
    auto &messenger = ClientMessenger::Instance();
    auto *original = messenger.rpc_client_;
    messenger.rpc_client_ = replacement;
    return original;
  }

  static error_code_t CallSyncLookup(uint16_t shard_id) {
    auto ctx = std::make_shared<simm::common::SimmContext>();
    sicl::rpc::RpcContext *ctx_p = nullptr;
    sicl::rpc::RpcContext::newInstance(ctx_p);
    auto rpc_ctx = std::shared_ptr<sicl::rpc::RpcContext>(ctx_p);
    ctx->set_rpc_ctx(rpc_ctx);
    rpc_ctx->set_timeout(sicl::transport::TimerTick::TIMER_1S);

    KVLookupRequestPB req;
    req.set_shard_id(shard_id);
    req.set_key("mock-key");
    auto resp = std::make_shared<KVLookupResponsePB>();
    return ClientMessenger::Instance().call_sync<KVLookupRequestPB, KVLookupResponsePB>(
        shard_id, static_cast<sicl::rpc::ReqType>(simm::ds::KVServerRpcType::RPC_CLIENT_KV_LOOKUP), req, resp, ctx);
  }

  static error_code_t BuildConnectionWait(const std::string &addr) {
    return ClientMessenger::Instance().build_connection(addr, ClientMessenger::BuildConnWaitMode::kWaitForInflight);
  }

  static error_code_t BuildConnectionNoWait(const std::string &addr) {
    return ClientMessenger::Instance().build_connection(addr, ClientMessenger::BuildConnWaitMode::kNoWait);
  }

  // Inject a dead-since timestamp for a DS, simulating it having been seen as dead at a given
  // time.  Used to control the deferred-reshard wait window in failover thread tests.
  static void InjectDeadSince(const std::string &addr,
                               std::chrono::steady_clock::time_point tp) {
    auto &messenger = ClientMessenger::Instance();
    std::lock_guard lg(messenger.ds_dead_since_mtx_);
    messenger.ds_dead_since_[addr] = tp;
  }

  // Clear the dead-since record for a DS.
  static void ClearDeadSince(const std::string &addr) {
    auto &messenger = ClientMessenger::Instance();
    std::lock_guard lg(messenger.ds_dead_since_mtx_);
    messenger.ds_dead_since_.erase(addr);
  }

  // Check if a dead-since record exists for a DS.
  static bool HasDeadSince(const std::string &addr) {
    auto &messenger = ClientMessenger::Instance();
    std::lock_guard lg(messenger.ds_dead_since_mtx_);
    return messenger.ds_dead_since_.count(addr) > 0;
  }

  // Wake up the failover thread immediately (simulate a scan cycle without waiting for the
  // clnt_cm_addr_check_interval_inSecs sleep).
  static void WakeFailoverThread() {
    auto &messenger = ClientMessenger::Instance();
    messenger.failover_condv_.notify_all();
  }

  // Run one failover scan iteration synchronously (the DS-inactive branch only).
  // cm_addr_ is set to match the CM hook so the CM-change branch is skipped.
  // Returns true if ReInit() was triggered.
  static bool RunOneFailoverCycle() {
    auto &messenger = ClientMessenger::Instance();
    messenger.cm_addr_ = "10.0.0.100:9000";

    bool should_reinit = false;
    for (auto [addr, ds_ctx] : messenger.ds_conn_ctxs_) {
      if (!ds_ctx->active.load()) {
        {
          std::lock_guard lg(messenger.ds_dead_since_mtx_);
          if (!messenger.ds_dead_since_.count(addr)) {
            messenger.ds_dead_since_[addr] = std::chrono::steady_clock::now();
          }
        }

        if (CommonErr::OK == messenger.build_connection(addr)) {
          std::lock_guard lg(messenger.ds_dead_since_mtx_);
          messenger.ds_dead_since_.erase(addr);
          continue;
        }

        std::chrono::duration<double> dur;
        {
          std::lock_guard lg(messenger.ds_dead_since_mtx_);
          dur = std::chrono::steady_clock::now() - messenger.ds_dead_since_[addr];
        }

        if (dur > std::chrono::seconds(FLAGS_clnt_deferred_reshard_wait_inSecs)) {
          {
            std::lock_guard lg(messenger.ds_dead_since_mtx_);
            messenger.ds_dead_since_.erase(addr);
          }
          should_reinit = true;
        }
      }
    }

    if (should_reinit) {
      messenger.ReInit();
    }
    return should_reinit;
  }
};

class ClientMessengerUnitTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ClientMessengerTestPeer::ResetState();
    original_rpc_client_ = ClientMessengerTestPeer::SwapRpcClient(nullptr);
  }

  void TearDown() override {
    if (injected_rpc_client_ != nullptr) {
      auto *to_delete = ClientMessengerTestPeer::SwapRpcClient(original_rpc_client_);
      if (to_delete != nullptr && to_delete != original_rpc_client_) {
        delete to_delete;
      }
      injected_rpc_client_ = nullptr;
    } else {
      ClientMessengerTestPeer::SwapRpcClient(original_rpc_client_);
    }
    ClientMessengerTestPeer::ResetState();
  }

  FakeSiRPC *InstallFakeRpcClient() {
    auto *fake = new FakeSiRPC();
    ClientMessengerTestPeer::SwapRpcClient(fake);
    injected_rpc_client_ = fake;
    return fake;
  }

 private:
  sicl::rpc::SiRPC *original_rpc_client_{nullptr};
  sicl::rpc::SiRPC *injected_rpc_client_{nullptr};
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

TEST_F(ClientMessengerUnitTest, SyncRetryFlagControlsRetryCountForTimeoutErrors) {
  auto *fake_rpc = InstallFakeRpcClient();
  ClientMessengerTestPeer::InstallDsContext("10.0.0.1:1001", true, 1, {0}, std::make_shared<FakeConnection>("ready"));

  const auto old_enable_retry = FLAGS_clnt_syncreq_enable_retry;
  const auto old_retry_count = FLAGS_clnt_syncreq_retry_count;

  fake_rpc->SetSyncSendHandler([](const std::shared_ptr<sicl::rpc::RpcContext> &ctx) {
    ctx->SetError(sicl::transport::SICL_ERR_TIMEOUT, "timeout");
  });

  FLAGS_clnt_syncreq_enable_retry = false;
  FLAGS_clnt_syncreq_retry_count = 2;
  EXPECT_EQ(ClientMessengerTestPeer::CallSyncLookup(0), ClntErr::ClntSendRPCFailed);
  EXPECT_EQ(fake_rpc->SyncSendCalls(), 1);

  FLAGS_clnt_syncreq_enable_retry = true;
  FLAGS_clnt_syncreq_retry_count = 2;
  EXPECT_EQ(ClientMessengerTestPeer::CallSyncLookup(0), ClntErr::ClntSendRPCFailed);
  EXPECT_EQ(fake_rpc->SyncSendCalls(), 4);

  FLAGS_clnt_syncreq_enable_retry = old_enable_retry;
  FLAGS_clnt_syncreq_retry_count = old_retry_count;
}

TEST_F(ClientMessengerUnitTest, ConcurrentForegroundBuildConnectionFailsFastFollowers) {
  auto *fake_rpc = InstallFakeRpcClient();
  fake_rpc->SetConnectDelay(std::chrono::milliseconds(200));
  fake_rpc->SetConnectHandler([]() { return std::make_shared<FakeConnection>("connected"); });
  ClientMessengerTestPeer::InstallDsContext("10.0.0.8:1008", false, 0, {0}, nullptr);

  std::atomic<int> ready{0};
  std::atomic<bool> go{false};
  std::atomic<int> ok_count{0};
  std::atomic<int> fail_count{0};
  std::vector<std::thread> threads;
  for (int i = 0; i < 8; ++i) {
    threads.emplace_back([&]() {
      ready.fetch_add(1);
      while (!go.load()) {
        std::this_thread::yield();
      }
      auto ret = ClientMessengerTestPeer::BuildConnectionNoWait("10.0.0.8:1008");
      if (ret == CommonErr::OK) {
        ok_count.fetch_add(1);
      } else {
        EXPECT_EQ(ret, ClntErr::BuildConnectionFailed);
        fail_count.fetch_add(1);
      }
    });
  }

  while (ready.load() != 8) {
    std::this_thread::yield();
  }
  go.store(true);

  for (auto &t : threads) {
    t.join();
  }

  EXPECT_EQ(fake_rpc->ConnectCalls(), 1);
  EXPECT_GE(ok_count.load(), 1);
  EXPECT_GE(fail_count.load(), 1);
  EXPECT_TRUE(ClientMessengerTestPeer::IsDsActive("10.0.0.8:1008"));
}

TEST_F(ClientMessengerUnitTest, ConcurrentBackgroundBuildConnectionWaitsAndSharesSingleConnect) {
  auto *fake_rpc = InstallFakeRpcClient();
  fake_rpc->SetConnectDelay(std::chrono::milliseconds(100));
  fake_rpc->SetConnectHandler([]() { return std::make_shared<FakeConnection>("connected"); });
  ClientMessengerTestPeer::InstallDsContext("10.0.0.9:1009", false, 0, {0}, nullptr);

  std::atomic<int> ready{0};
  std::atomic<bool> go{false};
  std::atomic<int> ok_count{0};
  std::vector<std::thread> threads;
  for (int i = 0; i < 6; ++i) {
    threads.emplace_back([&]() {
      ready.fetch_add(1);
      while (!go.load()) {
        std::this_thread::yield();
      }
      auto ret = ClientMessengerTestPeer::BuildConnectionWait("10.0.0.9:1009");
      EXPECT_EQ(ret, CommonErr::OK);
      if (ret == CommonErr::OK) {
        ok_count.fetch_add(1);
      }
    });
  }

  while (ready.load() != 6) {
    std::this_thread::yield();
  }
  go.store(true);

  for (auto &t : threads) {
    t.join();
  }

  EXPECT_EQ(fake_rpc->ConnectCalls(), 1);
  EXPECT_EQ(ok_count.load(), 6);
  EXPECT_TRUE(ClientMessengerTestPeer::IsDsActive("10.0.0.9:1009"));
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

TEST_F(ClientMessengerUnitTest, GetCmAddressUsesFlagToSkipK8SLookup) {
  const auto old_use_k8s = FLAGS_clnt_use_k8s;
  const auto old_cm_ip = FLAGS_cm_primary_node_ip;
  const auto old_cm_port = FLAGS_cm_rpc_inter_port;

  FLAGS_clnt_use_k8s = false;
  FLAGS_cm_primary_node_ip = "10.8.0.1";
  FLAGS_cm_rpc_inter_port = 30001;

  EXPECT_EQ(ClientMessengerTestPeer::GetCmAddress(), "10.8.0.1:30001");

  FLAGS_clnt_use_k8s = old_use_k8s;
  FLAGS_cm_primary_node_ip = old_cm_ip;
  FLAGS_cm_rpc_inter_port = old_cm_port;
}

// ─────────────────────────────────────────────────────────────────────────────
// Deferred reshard window tests: verify the failover thread's DS-dead tracking
// and ReInit-trigger logic for the "DS IP changed after restart" scenario.
//
// The failover thread logic has two key branches we test here:
//   1. DS inactive + reconnect fails + window NOT expired → should_reinit = false
//   2. DS inactive + reconnect fails + window expired    → should_reinit = true → ReInit()
//   3. DS inactive + reconnect succeeds                  → dead-since cleared, no ReInit
//
// We test these by driving the failover decision logic directly (via RunOneFailoverCycle)
// rather than relying on timing of the background thread.
// ─────────────────────────────────────────────────────────────────────────────

// Expose a test-only hook to run one failover scan cycle synchronously.
// This mirrors the body of the failover thread loop, minus the sleep.
// We add this to ClientMessengerTestPeer rather than production code.

// Helper: manually execute one failover scan iteration (the DS-scan branch only,
// CM-address-change check is skipped by keeping cm_addr_ matching the hook).
static bool RunOneFailoverCycle(FakeSiRPC * /*unused*/) {
  return ClientMessengerTestPeer::RunOneFailoverCycle();
}

// Test: DS goes inactive, reconnect fails, window NOT expired → ReInit not triggered.
// After backdating dead-since past the window, next cycle triggers ReInit and picks
// up the new IP from CM.
TEST_F(ClientMessengerUnitTest, DeferredWindowExpiryTriggersReInitWithNewIP) {
  const std::string old_addr = "10.0.0.1:1001";
  const std::string new_addr = "10.0.0.99:1001";
  const uint32_t window_secs = 2;

  const auto saved_window = FLAGS_clnt_deferred_reshard_wait_inSecs;
  FLAGS_clnt_deferred_reshard_wait_inSecs = window_secs;

  auto *fake_rpc = InstallFakeRpcClient();

  // DS starts active on old_addr, owns shard 0
  ClientMessengerTestPeer::InstallDsContext(old_addr, true, 1, {0},
                                            std::make_shared<FakeConnection>("old"));
  // DS goes inactive (pod crash)
  ClientMessengerTestPeer::MarkConnectionActive(old_addr, false, 2);

  // CM hook must be set before RunOneFailoverCycle (ReInit calls get_cm_address via hook)
  ClientMessengerTestPeer::SetGetCmAddressHook([]() { return std::string("10.0.0.100:9000"); });

  // build_connection for old_addr always fails — old IP unreachable.
  // build_connection for new_addr succeeds — DS restarted on new IP.
  // Both go through test_build_connection_hook_ which takes priority over rpc_client_->connect.
  fake_rpc->SetConnectHandler([]() -> std::shared_ptr<sicl::rpc::Connection> { return nullptr; });

  std::atomic<int> reinit_count{0};
  // CM now knows the new IP (DS has already re-registered with new IP)
  ClientMessengerTestPeer::SetRouteQueryHook([&](const std::string &) {
    reinit_count.fetch_add(1);
    return std::make_pair(CommonErr::OK, BuildRoutingResponse({{new_addr, {0}}}));
  });
  // Use build_connection hook so both old and new addr go through the same path
  ClientMessengerTestPeer::SetBuildConnectionHook([&](const std::string &addr) {
    if (addr == new_addr) {
      ClientMessengerTestPeer::MarkConnectionActive(addr, true, 10);
      return CommonErr::OK;
    }
    // old_addr (and any other addr) — still unreachable
    return ClntErr::BuildConnectionFailed;
  });

  // ── Cycle 1: dead-since just now → still within window ──
  ClientMessengerTestPeer::InjectDeadSince(old_addr, std::chrono::steady_clock::now());
  bool did_reinit = RunOneFailoverCycle(fake_rpc);

  EXPECT_FALSE(did_reinit) << "ReInit must not fire within deferred window";
  EXPECT_EQ(reinit_count.load(), 0) << "route_query hook must not be called within window";
  EXPECT_EQ(ClientMessengerTestPeer::ShardOwner(0), old_addr)
      << "Shard 0 must still point to old addr while within window";
  EXPECT_TRUE(ClientMessengerTestPeer::HasDeadSince(old_addr))
      << "dead-since entry must persist while in window";

  // ── Cycle 2: backdate dead-since past window → ReInit fires ──
  ClientMessengerTestPeer::InjectDeadSince(
      old_addr,
      std::chrono::steady_clock::now() - std::chrono::seconds(window_secs + 1));
  did_reinit = RunOneFailoverCycle(fake_rpc);

  EXPECT_TRUE(did_reinit) << "ReInit must fire after window expires";
  EXPECT_EQ(reinit_count.load(), 1) << "route_query hook must be called exactly once";
  EXPECT_FALSE(ClientMessengerTestPeer::HasDeadSince(old_addr))
      << "dead-since entry must be cleared after ReInit";
  EXPECT_EQ(ClientMessengerTestPeer::ShardOwner(0), new_addr)
      << "Shard 0 must be rerouted to new DS IP after ReInit";

  FLAGS_clnt_deferred_reshard_wait_inSecs = saved_window;
}

// Test: DS goes inactive but recovers on the SAME IP within the deferred window.
// build_connection succeeds → dead-since cleared, ReInit never triggered.
TEST_F(ClientMessengerUnitTest, DeferredWindowReconnectSameIPClearsDeadSince) {
  const std::string addr = "10.0.0.1:1001";
  const uint32_t window_secs = 10;

  const auto saved_window = FLAGS_clnt_deferred_reshard_wait_inSecs;
  FLAGS_clnt_deferred_reshard_wait_inSecs = window_secs;

  auto *fake_rpc = InstallFakeRpcClient();

  ClientMessengerTestPeer::InstallDsContext(addr, false, 1, {0}, nullptr);

  std::atomic<int> reinit_count{0};
  ClientMessengerTestPeer::SetRouteQueryHook([&](const std::string &) {
    reinit_count.fetch_add(1);
    return std::make_pair(CommonErr::OK, BuildRoutingResponse({{addr, {0}}}));
  });

  // DS recovered on same IP — reconnect succeeds
  fake_rpc->SetConnectHandler([&]() {
    return std::make_shared<FakeConnection>("recovered");
  });

  // Inject a fresh dead-since (well within the window)
  ClientMessengerTestPeer::InjectDeadSince(addr, std::chrono::steady_clock::now());

  bool did_reinit = RunOneFailoverCycle(fake_rpc);

  EXPECT_FALSE(did_reinit) << "ReInit must not fire when DS recovers on same IP";
  EXPECT_EQ(reinit_count.load(), 0) << "route_query hook must not be called on same-IP recovery";
  EXPECT_FALSE(ClientMessengerTestPeer::HasDeadSince(addr))
      << "dead-since must be cleared after successful reconnect";
  EXPECT_TRUE(ClientMessengerTestPeer::IsDsActive(addr))
      << "DS must be active after successful reconnect";

  FLAGS_clnt_deferred_reshard_wait_inSecs = saved_window;
}

}  // namespace clnt
}  // namespace simm

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
