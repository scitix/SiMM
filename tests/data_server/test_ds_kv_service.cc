#include <gtest/gtest.h>
#include <rpc/rpc.h>


#include "folly/Random.h"
#include "folly/executors/IOThreadPoolExecutor.h"
#include "folly/futures/Future.h"
#include "folly/synchronization/Baton.h"

#include "transport/types.h"

#include "common/logging/logging.h"
#include "common/base/hash.h"
#include "common/hashkit/murmurhash.h"
#include "proto/ds_clnt_rpcs.pb.h"
#include "rpc/rpc_context.h"
#include "data_server/kv_rpc_handler.h"
#include "data_server/kv_rpc_service.h"

DECLARE_LOG_MODULE("data_server");
DECLARE_uint64(memory_limit_bytes);
DECLARE_uint32(ds_free_memory_usable_ratio);

namespace simm {
namespace ds {

size_t get_random_size(size_t min = 1, size_t max = 1UL << 22) {
  return folly::Random::rand32(min, max + 1);
}

void get_random_string(size_t len, std::string *val) {
  static constexpr char charset[] =
      "abcdefghijklmnopqrstuvwxyz"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "0123456789"
      "~!@#$%^&*()_+`-=,./<>?;':[]{}";
  std::string &res = *val;
  res.reserve(len);
  for (size_t i = 0; i < len; i++) {
    size_t idx = folly::Random::rand32(0, sizeof(charset) - 1);
    res.push_back(charset[idx]);
  }
}

class KVServiceTest : public ::testing::Test {
 protected:
  void SetUp() override {
    rpcServicePtr = std::make_unique<KVRpcService>();
    FLAGS_memory_limit_bytes = 1ULL << 31;
    FLAGS_ds_free_memory_usable_ratio = 100;
    auto ret = rpcServicePtr->Init();
    ASSERT_EQ(ret, CommonErr::OK);
  }

  void TearDown() override { rpcServicePtr.reset(); }

  std::unique_ptr<KVRpcService> rpcServicePtr;
};

TEST_F(KVServiceTest, TestClientHandlers) {
  size_t server_thread_num = 1;
  size_t client_thread_num = 3;
  auto serverPool = std::make_unique<folly::IOThreadPoolExecutor>(server_thread_num);
  auto clientPool = std::make_unique<folly::IOThreadPoolExecutor>(client_thread_num);
  folly::Baton<> serverReady, serverExit;

  folly::via(serverPool->getEventBase(), [&] {
    auto ret = rpcServicePtr->Start();
    EXPECT_EQ(ret, CommonErr::OK);
    serverReady.post();
    serverExit.wait();
  });

  serverReady.wait();
  size_t client_count = 2;
  std::atomic<size_t> done_count{0};
  std::vector<std::string> keys(client_count);
  std::vector<std::string> vals(client_count);
  std::vector<sicl::transport::MemDesc *> descrs(client_count);
  for (size_t i = 0; i < client_count; i++) {
    folly::via(clientPool->getEventBase(), [&, i] {
      sicl::rpc::SiRPC *sirpc;
      sicl::rpc::SiRPC::newInstance(sirpc, false);
      sicl::rpc::RpcContext *sictx;
      sicl::rpc::RpcContext::newInstance(sictx);
      auto ctx = std::shared_ptr<sicl::rpc::RpcContext>(sictx);
      ctx->set_timeout(sicl::transport::TimerTick::TIMER_1S);

      // construct connection
      auto conn = sirpc->connect("127.0.0.1", FLAGS_io_service_port);
      EXPECT_EQ(bool(conn), true);

      // construct key and value
      auto &key = keys[i];
      key = "test_key_from_client_" + std::to_string(i + 1);
      uint16_t shard_id = hashkit::HashkitBase::Instance().generate_16bit_hash_value(key.c_str(), key.length());
      shard_id %= FLAGS_shard_total_num;
      sicl::transport::Mempool *simemp = sirpc->GetMempool();
      size_t value_len = get_random_size();
      MLOG_INFO("shard[{}] key: {} get_random_size for value length {}", shard_id, key, value_len);
      sicl::transport::MemDesc *descr = descrs[i];
      auto res = simemp->alloc(descr, value_len);
      EXPECT_EQ(res, sicl::transport::Result::SICL_SUCCESS);
      auto &value = vals[i];
      get_random_string(value_len, &value);
      memcpy(descr->getAddr(), value.c_str(), value_len);

      // construct put request
      auto put_req = std::make_shared<KVPutRequestPB>();
      auto put_rsp = std::make_shared<KVPutResponsePB>();
      put_req->set_shard_id(shard_id);
      put_req->set_key(key);
      put_req->set_val_len(value_len);
      put_req->set_buf_addr((uint64_t)(descr->getAddr()));
      put_req->set_buf_ofs(0);
      put_req->set_buf_len(value_len);
      for (auto rkey : descr->getRemoteKeys()) {
        put_req->add_buf_rkey(rkey);
      }
      std::atomic<bool> put_succ{false};

      sicl::rpc::RpcDoneFn put_fn = [&put_succ](const google::protobuf::Message *rsp,
                                                const std::shared_ptr<sicl::rpc::RpcContext> ctx) {
        ASSERT_EQ(ctx->Failed(), false);
        auto put_rsp = dynamic_cast<const KVPutResponsePB *>(rsp);
        EXPECT_EQ(put_rsp->ret_code(), 0);
        put_succ.store(true);
      };
      sirpc->SendRequest(conn,
                         static_cast<sicl::rpc::ReqType>(KVServerRpcType::RPC_CLIENT_KV_PUT),
                         *put_req,
                         put_rsp.get(),
                         ctx,
                         put_fn);

      while (!put_succ.load()) std::this_thread::sleep_for(std::chrono::milliseconds(10));

      // construct lookup request
      auto lookup_req = std::make_shared<KVLookupRequestPB>();
      auto lookup_rsp = std::make_shared<KVLookupResponsePB>();
      lookup_req->set_shard_id(shard_id);
      lookup_req->set_key(key);
      std::atomic<bool> lookup_succ{false};

      sicl::rpc::RpcDoneFn lookup_fn = [&lookup_succ](const google::protobuf::Message *rsp,
                                                      const std::shared_ptr<sicl::rpc::RpcContext> ctx) {
        ASSERT_EQ(ctx->Failed(), false);
        auto lookup_rsp = dynamic_cast<const KVLookupResponsePB *>(rsp);
        EXPECT_EQ(lookup_rsp->ret_code(), 0);
        lookup_succ.store(true);
      };
      sirpc->SendRequest(conn,
                         static_cast<sicl::rpc::ReqType>(KVServerRpcType::RPC_CLIENT_KV_LOOKUP),
                         *lookup_req,
                         lookup_rsp.get(),
                         ctx,
                         lookup_fn);

      while (!lookup_succ.load()) std::this_thread::sleep_for(std::chrono::milliseconds(10));

      // construct get request
      memset(descr->getAddr(), 0, value_len);
      auto get_req = std::make_shared<KVGetRequestPB>();
      auto get_rsp = std::make_shared<KVGetResponsePB>();
      get_req->set_shard_id(shard_id);
      get_req->set_key(key);
      get_req->set_buf_addr((uint64_t)(descr->getAddr()));
      get_req->set_buf_ofs(0);
      get_req->set_buf_len(value_len);
      for (auto rkey : descr->getRemoteKeys()) {
        get_req->add_buf_rkey(rkey);
      }
      std::atomic<bool> get_succ{false};

      sicl::rpc::RpcDoneFn get_fn = [&get_succ](const google::protobuf::Message *rsp,
                                                const std::shared_ptr<sicl::rpc::RpcContext> ctx) {
        ASSERT_EQ(ctx->Failed(), false);
        auto get_rsp = dynamic_cast<const KVGetResponsePB *>(rsp);
        EXPECT_EQ(get_rsp->ret_code(), 0);
        get_succ.store(true);
      };
      sirpc->SendRequest(conn,
                         static_cast<sicl::rpc::ReqType>(KVServerRpcType::RPC_CLIENT_KV_GET),
                         *get_req,
                         get_rsp.get(),
                         ctx,
                         get_fn);

      while (!get_succ.load()) std::this_thread::sleep_for(std::chrono::milliseconds(10));
      int check_ret = memcmp(descr->getAddr(), value.c_str(), value_len);
      EXPECT_EQ(check_ret, 0);

      // construct del request
      auto del_req = std::make_shared<KVDelRequestPB>();
      auto del_rsp = std::make_shared<KVDelResponsePB>();
      del_req->set_shard_id(shard_id);
      del_req->set_key(key);

      sirpc->SendRequest(
          conn, static_cast<sicl::rpc::ReqType>(KVServerRpcType::RPC_CLIENT_KV_DEL), *del_req, del_rsp.get(), ctx);
      ASSERT_EQ(ctx->Failed(), false);
      EXPECT_EQ(del_rsp->ret_code(), 0);

      // notify server exit if all finished
      auto cnt = done_count.fetch_add(1);
      if (cnt + 1 == client_count) {
        serverExit.post();
      }

      simemp->release(descr);
    });
  }

  serverPool->join();
  clientPool->join();
}
}  // namespace ds
}  // namespace simm

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
