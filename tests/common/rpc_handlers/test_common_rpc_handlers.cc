#include <atomic>
#include <string>
#include <string_view>
#include <thread>
#include <chrono>
#include <cfloat>
#include <latch>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "rpc/connection.h"
#include "rpc/rpc.h"
#include "rpc/rpc_context.h"
#include "transport/types.h"

#include "common/logging/logging.h"
#include "common/base/common_types.h"
#include "common/rpc_handlers/common_rpc_handlers.h"
#include "common/base/common_types.h"
#include "common/errcode/errcode_def.h"
#include "proto/common.pb.h"

static constexpr double sPI{3.1415926};
static constexpr bool sNewBool{true};
static constexpr int32_t sNewInt32{-200};
static constexpr int64_t sNewInt64{-88888};
static constexpr uint32_t sNewUint32{9999};
static constexpr uint64_t sNewUint64{202508131026};
static const std::string sNewString{"hello_world"};
static constexpr double sNewDouble{sPI*2};

DEFINE_bool(test_flag_bool, false, "test bool");
DEFINE_int32(test_flag_int32, -100, "test int32");
DEFINE_int64(test_flag_int64, 10000, "test int64");
DEFINE_uint32(test_flag_uint32, 100, "test uint32");
DEFINE_uint64(test_flag_uint64, 1000000, "test uint64");
DEFINE_string(test_flag_string, "ut_test", "test string");
DEFINE_double(test_flag_double, sPI, "test double");

namespace simm {
namespace common {

class CommonRpcHandlersTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // create SiRPC object for server thread usage
    sicl::rpc::SiRPC *rpc_raw_server = nullptr;
    auto res = sicl::rpc::SiRPC::newInstance(rpc_raw_server, true/*is_server*/);
    EXPECT_EQ(res, sicl::transport::SICL_SUCCESS);
    server_rpc_service_.reset(rpc_raw_server);
    res = static_cast<sicl::transport::Result>(server_rpc_service_->Start(server_admin_port_));
    EXPECT_EQ(res, sicl::transport::SICL_SUCCESS);

    // create SiRPC object for client usage
    sicl::rpc::SiRPC *rpc_raw_client = nullptr;
    res = sicl::rpc::SiRPC::newInstance(rpc_raw_client, false/*is_server*/);
    EXPECT_EQ(res, sicl::transport::SICL_SUCCESS);
    client_rpc_service_.reset(rpc_raw_client);

    // start server thread
    server_thread_ = std::jthread(&CommonRpcHandlersTest::ServerAdminServiceThread, this);
  }

  void TearDown() override {
    stop_server_.store(true);
    if (client_rpc_service_) {
      client_rpc_service_.reset();
    }
    if (server_rpc_service_) {
      server_rpc_service_.reset();
    }
  }

  void ServerAdminServiceThread() {
    server_rpc_service_->RegisterHandler(
        static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_GET_GFLAG_REQ),
        new simm::common::GetGFlagHandler(server_rpc_service_.get(), new proto::common::GetGFlagValueRequestPB));
    server_rpc_service_->RegisterHandler(
        static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_SET_GFLAG_REQ),
        new simm::common::SetGFlagHandler(server_rpc_service_.get(), new proto::common::SetGFlagValueRequestPB));
    server_rpc_service_->RegisterHandler(
        static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_LIST_GFLAGS_REQ),
        new simm::common::ListGFlagsHandler(server_rpc_service_.get(), new proto::common::ListAllGFlagsRequestPB));

    while (!stop_server_.load()) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    LOG_INFO("{} exit...", __func__);
  }

  const int32_t server_admin_port_{30001};

  std::atomic<bool> stop_server_{false};

  std::jthread server_thread_{};

  std::unique_ptr<sicl::rpc::SiRPC> client_rpc_service_{nullptr};
  std::unique_ptr<sicl::rpc::SiRPC> server_rpc_service_{nullptr};
};

TEST_F(CommonRpcHandlersTest, TestGetGFlagRpc) {
  std::latch get_done_latch(7);
  proto::common::GetGFlagValueRequestPB req;
  auto resp = new proto::common::GetGFlagValueResponsePB();
  sicl::rpc::RpcContext* ctx_p = nullptr;
  sicl::rpc::RpcContext::newInstance(ctx_p);
  std::shared_ptr<sicl::rpc::RpcContext> ctx = std::shared_ptr<sicl::rpc::RpcContext>(ctx_p);
  ctx->set_timeout(sicl::transport::TimerTick::TIMER_1S);
  auto done_cb_ok = [&resp, &get_done_latch](const google::protobuf::Message* rsp,
                                      const std::shared_ptr<sicl::rpc::RpcContext> ctx) {
    EXPECT_EQ(ctx->ErrorCode(), sicl::transport::Result::SICL_SUCCESS);
    auto *response = dynamic_cast<const proto::common::GetGFlagValueResponsePB*>(rsp);
    EXPECT_EQ(response->ret_code(), CommonErr::OK);
    if (response->flag_info().flag_name() == "test_flag_bool") {
        EXPECT_EQ(response->flag_info().flag_name(), "test_flag_bool");
        EXPECT_EQ(response->flag_info().flag_value(), "false");
        EXPECT_EQ(response->flag_info().flag_default_value(), "false");
        EXPECT_EQ(response->flag_info().flag_type(), "bool");
        EXPECT_EQ(response->flag_info().flag_description(), "test bool");
    } else if (response->flag_info().flag_name() == "test_flag_int32") {
        EXPECT_EQ(response->flag_info().flag_name(), "test_flag_int32");
        EXPECT_EQ(response->flag_info().flag_value(), "-100");
        EXPECT_EQ(response->flag_info().flag_default_value(), "-100");
        EXPECT_EQ(response->flag_info().flag_type(), "int32");
        EXPECT_EQ(response->flag_info().flag_description(), "test int32");
    } else if (response->flag_info().flag_name() == "test_flag_int64") {
        EXPECT_EQ(response->flag_info().flag_name(), "test_flag_int64");
        EXPECT_EQ(response->flag_info().flag_value(), "10000");
        EXPECT_EQ(response->flag_info().flag_default_value(), "10000");
        EXPECT_EQ(response->flag_info().flag_type(), "int64");
        EXPECT_EQ(response->flag_info().flag_description(), "test int64");
    } else if (response->flag_info().flag_name() == "test_flag_uint32") {
        EXPECT_EQ(response->flag_info().flag_name(), "test_flag_uint32");
        EXPECT_EQ(response->flag_info().flag_value(), "100");
        EXPECT_EQ(response->flag_info().flag_default_value(), "100");
        EXPECT_EQ(response->flag_info().flag_type(), "uint32");
        EXPECT_EQ(response->flag_info().flag_description(), "test uint32");
    } else if (response->flag_info().flag_name() == "test_flag_uint64") {
        EXPECT_EQ(response->flag_info().flag_name(), "test_flag_uint64");
        EXPECT_EQ(response->flag_info().flag_value(), "1000000");
        EXPECT_EQ(response->flag_info().flag_default_value(), "1000000");
        EXPECT_EQ(response->flag_info().flag_type(), "uint64");
        EXPECT_EQ(response->flag_info().flag_description(), "test uint64");
    } else if (response->flag_info().flag_name() == "test_flag_string") {
        EXPECT_EQ(response->flag_info().flag_name(), "test_flag_string");
        EXPECT_EQ(response->flag_info().flag_value(), "ut_test");
        EXPECT_EQ(response->flag_info().flag_default_value(), "ut_test");
        EXPECT_EQ(response->flag_info().flag_type(), "string");
        EXPECT_EQ(response->flag_info().flag_description(), "test string");
    } else if (response->flag_info().flag_name() == "test_flag_double") {
        EXPECT_EQ(response->flag_info().flag_name(), "test_flag_double");
        EXPECT_TRUE(std::abs(std::stod(response->flag_info().flag_value()) - sPI) < DBL_EPSILON);
        EXPECT_TRUE(std::abs(std::stod(response->flag_info().flag_default_value()) - sPI) < DBL_EPSILON);
        EXPECT_EQ(response->flag_info().flag_type(), "double");
        EXPECT_EQ(response->flag_info().flag_description(), "test double");
    } else {
        // nothing
    }
    get_done_latch.count_down();
  };

  req.set_flag_name("test_flag_bool");
  client_rpc_service_->SendRequest("127.0.0.1", server_admin_port_,
    static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_GET_GFLAG_REQ),
    req, resp, ctx, done_cb_ok);

  req.set_flag_name("test_flag_int32");
  resp->Clear();
  client_rpc_service_->SendRequest("127.0.0.1", server_admin_port_,
    static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_GET_GFLAG_REQ),
    req, resp, ctx, done_cb_ok);

  req.set_flag_name("test_flag_int64");
  resp->Clear();
  client_rpc_service_->SendRequest("127.0.0.1", server_admin_port_,
    static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_GET_GFLAG_REQ),
    req, resp, ctx, done_cb_ok);

  req.set_flag_name("test_flag_uint32");
  resp->Clear();
  client_rpc_service_->SendRequest("127.0.0.1", server_admin_port_,
    static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_GET_GFLAG_REQ),
    req, resp, ctx, done_cb_ok);

  req.set_flag_name("test_flag_uint64");
  resp->Clear();
  client_rpc_service_->SendRequest("127.0.0.1", server_admin_port_,
    static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_GET_GFLAG_REQ),
    req, resp, ctx, done_cb_ok);

  req.set_flag_name("test_flag_string");
  resp->Clear();
  client_rpc_service_->SendRequest("127.0.0.1", server_admin_port_,
    static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_GET_GFLAG_REQ),
    req, resp, ctx, done_cb_ok);

  req.set_flag_name("test_flag_double");
  resp->Clear();
  client_rpc_service_->SendRequest("127.0.0.1", server_admin_port_,
    static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_GET_GFLAG_REQ),
    req, resp, ctx, done_cb_ok);

  get_done_latch.wait();
}

TEST_F(CommonRpcHandlersTest, TestSetGFlagRpc) {
  std::latch set_done_latch(7);
  std::latch get_done_latch(7);

  proto::common::SetGFlagValueRequestPB req;
  auto resp = new proto::common::SetGFlagValueResponsePB();
  sicl::rpc::RpcContext* ctx_p = nullptr;
  sicl::rpc::RpcContext::newInstance(ctx_p);
  std::shared_ptr<sicl::rpc::RpcContext> ctx = std::shared_ptr<sicl::rpc::RpcContext>(ctx_p);
  ctx->set_timeout(sicl::transport::TimerTick::TIMER_1S);
  
  auto done_cb_set = [&resp, &set_done_latch](const google::protobuf::Message* rsp,
                                       const std::shared_ptr<sicl::rpc::RpcContext> ctx) {
    EXPECT_EQ(ctx->ErrorCode(), sicl::transport::Result::SICL_SUCCESS);
    auto *response = dynamic_cast<const proto::common::SetGFlagValueResponsePB*>(rsp);
    EXPECT_EQ(response->ret_code(), CommonErr::OK);
    // decrease the waiting counter for every set req done
    set_done_latch.count_down();
  };

  req.set_flag_name("test_flag_bool");
  req.set_flag_value("true");
  resp->Clear();
  client_rpc_service_->SendRequest("127.0.0.1", server_admin_port_,
    static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_SET_GFLAG_REQ),
    req, resp, ctx, done_cb_set);

  req.set_flag_name("test_flag_int32");
  req.set_flag_value(std::to_string(sNewInt32));
  resp->Clear();
  client_rpc_service_->SendRequest("127.0.0.1", server_admin_port_,
    static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_SET_GFLAG_REQ),
    req, resp, ctx, done_cb_set);

  req.set_flag_name("test_flag_int64");
  req.set_flag_value(std::to_string(sNewInt64));
  resp->Clear();
  client_rpc_service_->SendRequest("127.0.0.1", server_admin_port_,
    static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_SET_GFLAG_REQ),
    req, resp, ctx, done_cb_set);

  req.set_flag_name("test_flag_uint32");
  req.set_flag_value(std::to_string(sNewUint32));
  resp->Clear();
  client_rpc_service_->SendRequest("127.0.0.1", server_admin_port_,
    static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_SET_GFLAG_REQ),
    req, resp, ctx, done_cb_set);

  req.set_flag_name("test_flag_uint64");
  req.set_flag_value(std::to_string(sNewUint64));
  resp->Clear();
  client_rpc_service_->SendRequest("127.0.0.1", server_admin_port_,
    static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_SET_GFLAG_REQ),
    req, resp, ctx, done_cb_set);

  req.set_flag_name("test_flag_string");
  req.set_flag_value(sNewString);
  resp->Clear();
  client_rpc_service_->SendRequest("127.0.0.1", server_admin_port_,
    static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_SET_GFLAG_REQ),
    req, resp, ctx, done_cb_set);

  req.set_flag_name("test_flag_double");
  req.set_flag_value(std::to_string(sNewDouble));
  resp->Clear();
  client_rpc_service_->SendRequest("127.0.0.1", server_admin_port_,
    static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_SET_GFLAG_REQ),
    req, resp, ctx, done_cb_set);

  set_done_latch.wait();

  proto::common::GetGFlagValueRequestPB req_get;
  auto resp_get = new proto::common::GetGFlagValueResponsePB();
  auto done_cb_get = [&](const google::protobuf::Message* rsp,
                                   const std::shared_ptr<sicl::rpc::RpcContext> ctx) {
    EXPECT_EQ(ctx->ErrorCode(), sicl::transport::Result::SICL_SUCCESS);
    auto *response = dynamic_cast<const proto::common::GetGFlagValueResponsePB*>(rsp);
    EXPECT_EQ(response->ret_code(), CommonErr::OK);
    if (response->flag_info().flag_name() == "test_flag_bool") {
        EXPECT_EQ(response->flag_info().flag_name(), "test_flag_bool");
        EXPECT_EQ(response->flag_info().flag_value(), "true");
        EXPECT_EQ(response->flag_info().flag_default_value(), "false");
        EXPECT_EQ(response->flag_info().flag_type(), "bool");
        EXPECT_EQ(response->flag_info().flag_description(), "test bool");
    } else if (response->flag_info().flag_name() == "test_flag_int32") {
        EXPECT_EQ(response->flag_info().flag_name(), "test_flag_int32");
        EXPECT_EQ(response->flag_info().flag_value(), std::to_string(sNewInt32));
        EXPECT_EQ(response->flag_info().flag_default_value(), "-100");
        EXPECT_EQ(response->flag_info().flag_type(), "int32");
        EXPECT_EQ(response->flag_info().flag_description(), "test int32");
    } else if (response->flag_info().flag_name() == "test_flag_int64") {
        EXPECT_EQ(response->flag_info().flag_name(), "test_flag_int64");
        EXPECT_EQ(response->flag_info().flag_value(), std::to_string(sNewInt64));
        EXPECT_EQ(response->flag_info().flag_default_value(), "10000");
        EXPECT_EQ(response->flag_info().flag_type(), "int64");
        EXPECT_EQ(response->flag_info().flag_description(), "test int64");
    } else if (response->flag_info().flag_name() == "test_flag_uint32") {
        EXPECT_EQ(response->flag_info().flag_name(), "test_flag_uint32");
        EXPECT_EQ(response->flag_info().flag_value(),std::to_string(sNewUint32));
        EXPECT_EQ(response->flag_info().flag_default_value(), "100");
        EXPECT_EQ(response->flag_info().flag_type(), "uint32");
        EXPECT_EQ(response->flag_info().flag_description(), "test uint32");
    } else if (response->flag_info().flag_name() == "test_flag_uint64") {
        EXPECT_EQ(response->flag_info().flag_name(), "test_flag_uint64");
        EXPECT_EQ(response->flag_info().flag_value(), std::to_string(sNewUint64));
        EXPECT_EQ(response->flag_info().flag_default_value(), "1000000");
        EXPECT_EQ(response->flag_info().flag_type(), "uint64");
        EXPECT_EQ(response->flag_info().flag_description(), "test uint64");
    } else if (response->flag_info().flag_name() == "test_flag_string") {
        EXPECT_EQ(response->flag_info().flag_name(), "test_flag_string");
        EXPECT_EQ(response->flag_info().flag_value(), sNewString);
        EXPECT_EQ(response->flag_info().flag_default_value(), "ut_test");
        EXPECT_EQ(response->flag_info().flag_type(), "string");
        EXPECT_EQ(response->flag_info().flag_description(), "test string");
    } else if (response->flag_info().flag_name() == "test_flag_double") {
        EXPECT_EQ(response->flag_info().flag_name(), "test_flag_double");
        //EXPECT_TRUE(std::abs(std::stod(response->flag_info().flag_value()) - 2 * sPI) < DBL_EPSILON);
        EXPECT_TRUE(std::abs(std::stod(response->flag_info().flag_default_value()) - sPI) < DBL_EPSILON);
        EXPECT_EQ(response->flag_info().flag_type(), "double");
        EXPECT_EQ(response->flag_info().flag_description(), "test double");
    } else {
        // nothing
    }
    get_done_latch.count_down();
  };

  req_get.set_flag_name("test_flag_bool");
  client_rpc_service_->SendRequest("127.0.0.1", server_admin_port_,
    static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_GET_GFLAG_REQ),
    req_get, resp_get, ctx, done_cb_get);

  req_get.set_flag_name("test_flag_int32");
  resp_get->Clear();
  client_rpc_service_->SendRequest("127.0.0.1", server_admin_port_,
    static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_GET_GFLAG_REQ),
    req_get, resp_get, ctx, done_cb_get);

  req_get.set_flag_name("test_flag_int64");
  resp_get->Clear();
  client_rpc_service_->SendRequest("127.0.0.1", server_admin_port_,
    static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_GET_GFLAG_REQ),
    req_get, resp_get, ctx, done_cb_get);

  req_get.set_flag_name("test_flag_uint32");
  resp_get->Clear();
  client_rpc_service_->SendRequest("127.0.0.1", server_admin_port_,
    static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_GET_GFLAG_REQ),
    req_get, resp_get, ctx, done_cb_get);

  req_get.set_flag_name("test_flag_uint64");
  resp_get->Clear();
  client_rpc_service_->SendRequest("127.0.0.1", server_admin_port_,
    static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_GET_GFLAG_REQ),
    req_get, resp_get, ctx, done_cb_get);

  req_get.set_flag_name("test_flag_string");
  resp_get->Clear();
  client_rpc_service_->SendRequest("127.0.0.1", server_admin_port_,
    static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_GET_GFLAG_REQ),
    req_get, resp_get, ctx, done_cb_get);

  req_get.set_flag_name("test_flag_double");
  resp_get->Clear();
  client_rpc_service_->SendRequest("127.0.0.1", server_admin_port_,
    static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_GET_GFLAG_REQ),
    req_get, resp_get, ctx, done_cb_get);
  
  get_done_latch.wait();
}

TEST_F(CommonRpcHandlersTest, TestListGFlagsRpc) {
  std::latch done_latch(1);
  std::atomic<int32_t> flags_found{0};

  proto::common::ListAllGFlagsRequestPB req;
  auto resp = new proto::common::ListAllGFlagsResponsePB();
  sicl::rpc::RpcContext* ctx_p = nullptr;
  sicl::rpc::RpcContext::newInstance(ctx_p);
  std::shared_ptr<sicl::rpc::RpcContext> ctx = std::shared_ptr<sicl::rpc::RpcContext>(ctx_p);
  ctx->set_timeout(sicl::transport::TimerTick::TIMER_1S);
  auto done_cb_list = [&](const google::protobuf::Message* rsp,
                                    const std::shared_ptr<sicl::rpc::RpcContext> ctx) {
    EXPECT_EQ(ctx->ErrorCode(), sicl::transport::Result::SICL_SUCCESS);
    auto *response = dynamic_cast<const proto::common::ListAllGFlagsResponsePB*>(rsp);
    EXPECT_EQ(response->ret_code(), CommonErr::OK);
    for (const auto & f : response->flags()) {
      if (f.flag_name() == "test_flag_bool") {
          EXPECT_EQ(f.flag_name(), "test_flag_bool");
          EXPECT_EQ(f.flag_value(), "true");
          EXPECT_EQ(f.flag_default_value(), "false");
          EXPECT_EQ(f.flag_type(), "bool");
          EXPECT_EQ(f.flag_description(), "test bool");
          flags_found++;
      } else if (f.flag_name() == "test_flag_int32") {
          EXPECT_EQ(f.flag_name(), "test_flag_int32");
          EXPECT_EQ(f.flag_value(), std::to_string(sNewInt32));
          EXPECT_EQ(f.flag_default_value(), "-100");
          EXPECT_EQ(f.flag_type(), "int32");
          EXPECT_EQ(f.flag_description(), "test int32");
          flags_found++;
      } else if (f.flag_name() == "test_flag_int64") {
          EXPECT_EQ(f.flag_name(), "test_flag_int64");
          EXPECT_EQ(f.flag_value(), std::to_string(sNewInt64));
          EXPECT_EQ(f.flag_default_value(), "10000");
          EXPECT_EQ(f.flag_type(), "int64");
          EXPECT_EQ(f.flag_description(), "test int64");
          flags_found++;
      } else if (f.flag_name() == "test_flag_uint32") {
          EXPECT_EQ(f.flag_name(), "test_flag_uint32");
          EXPECT_EQ(f.flag_value(),std::to_string(sNewUint32));
          EXPECT_EQ(f.flag_default_value(), "100");
          EXPECT_EQ(f.flag_type(), "uint32");
          EXPECT_EQ(f.flag_description(), "test uint32");
          flags_found++;
      } else if (f.flag_name() == "test_flag_uint64") {
          EXPECT_EQ(f.flag_name(), "test_flag_uint64");
          EXPECT_EQ(f.flag_value(), std::to_string(sNewUint64));
          EXPECT_EQ(f.flag_default_value(), "1000000");
          EXPECT_EQ(f.flag_type(), "uint64");
          EXPECT_EQ(f.flag_description(), "test uint64");
          flags_found++;
      } else if (f.flag_name() == "test_flag_string") {
          EXPECT_EQ(f.flag_name(), "test_flag_string");
          EXPECT_EQ(f.flag_value(), sNewString);
          EXPECT_EQ(f.flag_default_value(), "ut_test");
          EXPECT_EQ(f.flag_type(), "string");
          EXPECT_EQ(f.flag_description(), "test string");
          flags_found++;
      } else if (f.flag_name() == "test_flag_double") {
          EXPECT_EQ(f.flag_name(), "test_flag_double");
          //EXPECT_TRUE(std::abs(std::stod(f.flag_value()) - 2 * sPI) < DBL_EPSILON);
          EXPECT_TRUE(std::abs(std::stod(f.flag_default_value()) - sPI) < DBL_EPSILON);
          EXPECT_EQ(f.flag_type(), "double");
          EXPECT_EQ(f.flag_description(), "test double");
          flags_found++;
      } else {
          // nothing
      }
    }
    done_latch.count_down();
  };

  client_rpc_service_->SendRequest("127.0.0.1", server_admin_port_,
    static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_LIST_GFLAGS_REQ),
    req, resp, ctx, done_cb_list);

  done_latch.wait();

  EXPECT_EQ(flags_found.load(), 7);
}

} // namespace common
} // namespace simm

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
