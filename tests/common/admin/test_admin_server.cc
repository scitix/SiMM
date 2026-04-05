#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <memory>
#include <string>
#include <thread>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "common/admin/admin_msg_types.h"
#include "common/admin/admin_server.h"
#include "common/errcode/errcode_def.h"
#include "proto/common.pb.h"

namespace simm {
namespace common {
namespace {

// ---------------------------------------------------------------------------
// UDS client helper — sends a request frame, receives a response frame.
// Wire format: [uint32_t frame_len (network order)][uint16_t type][payload]
// ---------------------------------------------------------------------------

class UdsClient {
 public:
  bool connect(const std::string& socketPath) {
    fd_ = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd_ < 0) return false;

    sockaddr_un addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    std::strncpy(addr.sun_path, socketPath.c_str(), sizeof(addr.sun_path) - 1);

    socklen_t len = static_cast<socklen_t>(
        offsetof(sockaddr_un, sun_path) + std::strlen(addr.sun_path));

    if (::connect(fd_, reinterpret_cast<sockaddr*>(&addr), len) < 0) {
      ::close(fd_);
      fd_ = -1;
      return false;
    }
    return true;
  }

  ~UdsClient() { close(); }

  void close() {
    if (fd_ >= 0) { ::close(fd_); fd_ = -1; }
  }

  bool sendRequest(AdminMsgType type, const std::string& payload,
                   uint16_t& respType, std::string& respPayload) {
    if (fd_ < 0) return false;

    // Build request frame
    uint32_t frameLen = static_cast<uint32_t>(sizeof(uint16_t) + payload.size());
    uint32_t frameLenNet = htonl(frameLen);
    uint16_t typeNet = htons(static_cast<uint16_t>(type));

    if (!writeAll(&frameLenNet, sizeof(frameLenNet))) return false;
    if (!writeAll(&typeNet, sizeof(typeNet))) return false;
    if (!payload.empty() && !writeAll(payload.data(), payload.size())) return false;

    // Read response frame
    uint32_t respLenNet = 0;
    if (!readAll(&respLenNet, sizeof(respLenNet))) return false;
    uint32_t respLen = ntohl(respLenNet);
    if (respLen < sizeof(uint16_t)) return false;

    uint16_t respTypeNet = 0;
    if (!readAll(&respTypeNet, sizeof(respTypeNet))) return false;
    respType = ntohs(respTypeNet);

    uint32_t payloadLen = respLen - static_cast<uint32_t>(sizeof(uint16_t));
    respPayload.resize(payloadLen);
    if (payloadLen > 0 && !readAll(respPayload.data(), payloadLen)) return false;

    return true;
  }

 private:
  bool writeAll(const void* buf, size_t len) {
    const char* p = static_cast<const char*>(buf);
    size_t remaining = len;
    while (remaining > 0) {
      ssize_t n = ::write(fd_, p, remaining);
      if (n > 0) { remaining -= n; p += n; }
      else if (n == 0) return false;
      else { if (errno == EINTR) continue; return false; }
    }
    return true;
  }

  bool readAll(void* buf, size_t len) {
    char* p = static_cast<char*>(buf);
    size_t remaining = len;
    while (remaining > 0) {
      ssize_t n = ::read(fd_, p, remaining);
      if (n > 0) { remaining -= n; p += n; }
      else if (n == 0) return false;
      else { if (errno == EINTR) continue; return false; }
    }
    return true;
  }

  int fd_{-1};
};

// ---------------------------------------------------------------------------
// Test fixture
// ---------------------------------------------------------------------------

class AdminServerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ::mkdir("/run/simm", 0777);
  }

  void TearDown() override {
    server_.reset();
  }

  void createServer(const std::string& basePath = "/run/simm/simm_test") {
    server_ = std::make_unique<AdminServer>(basePath);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  UdsClient connectClient() {
    UdsClient client;
    EXPECT_TRUE(client.connect(server_->socketPath()));
    return client;
  }

  std::unique_ptr<AdminServer> server_;
};

// ---------------------------------------------------------------------------
// Lifecycle tests
// ---------------------------------------------------------------------------

TEST_F(AdminServerTest, ConstructionCreatesSocketFile) {
  createServer("/run/simm/simm_lifecycle");
  EXPECT_TRUE(server_->isRunning());
  struct stat st;
  EXPECT_EQ(::stat(server_->socketPath().c_str(), &st), 0);
  EXPECT_TRUE(S_ISSOCK(st.st_mode));
}

TEST_F(AdminServerTest, SocketPathFormat) {
  createServer("/run/simm/simm_ds");
  std::string expected = std::string("/run/simm/simm_ds.") +
                         std::to_string(::getpid()) + ".sock";
  EXPECT_EQ(server_->socketPath(), expected);
}

TEST_F(AdminServerTest, DestructorRemovesSocketFile) {
  createServer("/run/simm/simm_cleanup");
  std::string path = server_->socketPath();
  server_.reset();
  struct stat st;
  EXPECT_NE(::stat(path.c_str(), &st), 0);
}

TEST_F(AdminServerTest, MultipleServersWithDifferentBasePaths) {
  auto serverA = std::make_unique<AdminServer>("/run/simm/simm_testa");
  auto serverB = std::make_unique<AdminServer>("/run/simm/simm_testb");
  EXPECT_NE(serverA->socketPath(), serverB->socketPath());

  struct stat st;
  EXPECT_EQ(::stat(serverA->socketPath().c_str(), &st), 0);
  EXPECT_EQ(::stat(serverB->socketPath().c_str(), &st), 0);

  std::string pathA = serverA->socketPath();
  std::string pathB = serverB->socketPath();
  serverA.reset();
  serverB.reset();
  EXPECT_NE(::stat(pathA.c_str(), &st), 0);
  EXPECT_NE(::stat(pathB.c_str(), &st), 0);
}

TEST_F(AdminServerTest, IsRunningReflectsState) {
  createServer("/run/simm/simm_running");
  EXPECT_TRUE(server_->isRunning());
  server_.reset();
  // After destruction, no server to check — just verify no crash
}

// ---------------------------------------------------------------------------
// GFlag handler tests
// ---------------------------------------------------------------------------

TEST_F(AdminServerTest, GFlagListReturnsFlags) {
  createServer("/run/simm/simm_gflag_list");
  auto client = connectClient();

  uint16_t respType = 0;
  std::string respPayload;
  ASSERT_TRUE(client.sendRequest(AdminMsgType::GFLAG_LIST, "", respType, respPayload));
  EXPECT_EQ(respType, static_cast<uint16_t>(AdminMsgType::GFLAG_LIST));

  proto::common::ListAllGFlagsResponsePB resp;
  ASSERT_TRUE(resp.ParseFromString(respPayload));
  EXPECT_EQ(resp.ret_code(), CommonErr::OK);
  EXPECT_GT(resp.flags_size(), 0);
}

TEST_F(AdminServerTest, GFlagGetKnownFlag) {
  createServer("/run/simm/simm_gflag_get");

  proto::common::GetGFlagValueRequestPB req;
  req.set_flag_name("gtest_color");
  std::string reqBuf;
  req.SerializeToString(&reqBuf);

  auto client = connectClient();
  uint16_t respType = 0;
  std::string respPayload;
  ASSERT_TRUE(client.sendRequest(AdminMsgType::GFLAG_GET, reqBuf, respType, respPayload));

  proto::common::GetGFlagValueResponsePB resp;
  ASSERT_TRUE(resp.ParseFromString(respPayload));
  EXPECT_EQ(resp.ret_code(), CommonErr::OK);
  EXPECT_EQ(resp.flag_info().flag_name(), "gtest_color");
}

TEST_F(AdminServerTest, GFlagGetUnknownFlag) {
  createServer("/run/simm/simm_gflag_get_unk");

  proto::common::GetGFlagValueRequestPB req;
  req.set_flag_name("nonexistent_flag_12345");
  std::string reqBuf;
  req.SerializeToString(&reqBuf);

  auto client = connectClient();
  uint16_t respType = 0;
  std::string respPayload;
  ASSERT_TRUE(client.sendRequest(AdminMsgType::GFLAG_GET, reqBuf, respType, respPayload));

  proto::common::GetGFlagValueResponsePB resp;
  ASSERT_TRUE(resp.ParseFromString(respPayload));
  EXPECT_EQ(resp.ret_code(), CommonErr::GFlagNotFound);
}

TEST_F(AdminServerTest, GFlagSetAndVerify) {
  createServer("/run/simm/simm_gflag_set");

  proto::common::SetGFlagValueRequestPB setReq;
  setReq.set_flag_name("gtest_color");
  setReq.set_flag_value("no");
  std::string setBuf;
  setReq.SerializeToString(&setBuf);

  {
    auto client = connectClient();
    uint16_t respType = 0;
    std::string respPayload;
    ASSERT_TRUE(client.sendRequest(AdminMsgType::GFLAG_SET, setBuf, respType, respPayload));

    proto::common::SetGFlagValueResponsePB resp;
    ASSERT_TRUE(resp.ParseFromString(respPayload));
    EXPECT_EQ(resp.ret_code(), CommonErr::OK);
  }

  proto::common::GetGFlagValueRequestPB getReq;
  getReq.set_flag_name("gtest_color");
  std::string getBuf;
  getReq.SerializeToString(&getBuf);

  {
    auto client = connectClient();
    uint16_t respType = 0;
    std::string respPayload;
    ASSERT_TRUE(client.sendRequest(AdminMsgType::GFLAG_GET, getBuf, respType, respPayload));

    proto::common::GetGFlagValueResponsePB resp;
    ASSERT_TRUE(resp.ParseFromString(respPayload));
    EXPECT_EQ(resp.ret_code(), CommonErr::OK);
    EXPECT_EQ(resp.flag_info().flag_value(), "no");
  }
}

TEST_F(AdminServerTest, GFlagSetNonexistentFlag) {
  createServer("/run/simm/simm_gflag_set_unk");

  proto::common::SetGFlagValueRequestPB req;
  req.set_flag_name("nonexistent_flag_12345");
  req.set_flag_value("42");
  std::string reqBuf;
  req.SerializeToString(&reqBuf);

  auto client = connectClient();
  uint16_t respType = 0;
  std::string respPayload;
  ASSERT_TRUE(client.sendRequest(AdminMsgType::GFLAG_SET, reqBuf, respType, respPayload));

  proto::common::SetGFlagValueResponsePB resp;
  ASSERT_TRUE(resp.ParseFromString(respPayload));
  EXPECT_EQ(resp.ret_code(), CommonErr::GFlagNotFound);
}

// ---------------------------------------------------------------------------
// Trace toggle handler test
// ---------------------------------------------------------------------------

TEST_F(AdminServerTest, TraceToggle) {
  createServer("/run/simm/simm_trace");

  proto::common::TraceToggleRequestPB req;
  req.set_enable_trace(true);
  std::string reqBuf;
  req.SerializeToString(&reqBuf);

  auto client = connectClient();
  uint16_t respType = 0;
  std::string respPayload;
  ASSERT_TRUE(client.sendRequest(AdminMsgType::TRACE_TOGGLE, reqBuf, respType, respPayload));
  EXPECT_EQ(respType, static_cast<uint16_t>(AdminMsgType::TRACE_TOGGLE));

  proto::common::TraceToggleResponsePB resp;
  ASSERT_TRUE(resp.ParseFromString(respPayload));
  EXPECT_EQ(resp.ret_code(), CommonErr::OK);
}

// ---------------------------------------------------------------------------
// Custom handler registration tests
// ---------------------------------------------------------------------------

TEST_F(AdminServerTest, RegisterCustomHandler) {
  createServer("/run/simm/simm_custom");

  server_->registerHandler(AdminMsgType::DS_STATUS,
      [](const std::string& payload) -> std::string {
        proto::common::DsStatusResponsePB resp;
        resp.set_ret_code(CommonErr::OK);
        resp.set_is_registered(true);
        resp.set_cm_ready(true);
        resp.set_heartbeat_failure_count(0);
        std::string buf;
        resp.SerializeToString(&buf);
        return buf;
      });

  auto client = connectClient();
  uint16_t respType = 0;
  std::string respPayload;
  ASSERT_TRUE(client.sendRequest(AdminMsgType::DS_STATUS, "", respType, respPayload));
  EXPECT_EQ(respType, static_cast<uint16_t>(AdminMsgType::DS_STATUS));

  proto::common::DsStatusResponsePB resp;
  ASSERT_TRUE(resp.ParseFromString(respPayload));
  EXPECT_EQ(resp.ret_code(), CommonErr::OK);
  EXPECT_TRUE(resp.is_registered());
  EXPECT_TRUE(resp.cm_ready());
  EXPECT_EQ(resp.heartbeat_failure_count(), 0u);
}

TEST_F(AdminServerTest, CustomHandlerReceivesPayload) {
  createServer("/run/simm/simm_payload");

  server_->registerHandler(AdminMsgType::DS_STATUS,
      [](const std::string& payload) -> std::string {
        proto::common::DsStatusResponsePB resp;
        resp.set_ret_code(CommonErr::OK);
        resp.set_heartbeat_failure_count(static_cast<uint32_t>(payload.size()));
        std::string buf;
        resp.SerializeToString(&buf);
        return buf;
      });

  proto::common::DsStatusRequestPB req;
  std::string reqBuf;
  req.SerializeToString(&reqBuf);

  auto client = connectClient();
  uint16_t respType = 0;
  std::string respPayload;
  ASSERT_TRUE(client.sendRequest(AdminMsgType::DS_STATUS, reqBuf, respType, respPayload));

  proto::common::DsStatusResponsePB resp;
  ASSERT_TRUE(resp.ParseFromString(respPayload));
  EXPECT_EQ(resp.ret_code(), CommonErr::OK);
  EXPECT_EQ(resp.heartbeat_failure_count(), 0u);
}

TEST_F(AdminServerTest, OverrideBuiltinHandler) {
  createServer("/run/simm/simm_override");

  server_->registerHandler(AdminMsgType::GFLAG_LIST,
      [](const std::string& /*payload*/) -> std::string {
        proto::common::ListAllGFlagsResponsePB resp;
        resp.set_ret_code(CommonErr::OK);
        std::string buf;
        resp.SerializeToString(&buf);
        return buf;
      });

  auto client = connectClient();
  uint16_t respType = 0;
  std::string respPayload;
  ASSERT_TRUE(client.sendRequest(AdminMsgType::GFLAG_LIST, "", respType, respPayload));

  proto::common::ListAllGFlagsResponsePB resp;
  ASSERT_TRUE(resp.ParseFromString(respPayload));
  EXPECT_EQ(resp.ret_code(), CommonErr::OK);
  EXPECT_EQ(resp.flags_size(), 0);
}

// ---------------------------------------------------------------------------
// Protocol robustness tests
// ---------------------------------------------------------------------------

TEST_F(AdminServerTest, UnregisteredMsgTypeNoResponse) {
  createServer("/run/simm/simm_unknown_type");

  int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
  ASSERT_GE(fd, 0);

  sockaddr_un addr;
  std::memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  std::strncpy(addr.sun_path, server_->socketPath().c_str(), sizeof(addr.sun_path) - 1);
  socklen_t addrLen = static_cast<socklen_t>(
      offsetof(sockaddr_un, sun_path) + std::strlen(addr.sun_path));
  ASSERT_EQ(::connect(fd, reinterpret_cast<sockaddr*>(&addr), addrLen), 0);

  uint32_t frameLen = htonl(sizeof(uint16_t));
  uint16_t msgType = htons(999);
  ::write(fd, &frameLen, sizeof(frameLen));
  ::write(fd, &msgType, sizeof(msgType));

  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  char buf[64];
  ssize_t n = ::read(fd, buf, sizeof(buf));
  EXPECT_LE(n, 0);

  ::close(fd);
}

TEST_F(AdminServerTest, InvalidFrameTooShort) {
  createServer("/run/simm/simm_bad_frame");

  int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
  ASSERT_GE(fd, 0);

  sockaddr_un addr;
  std::memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  std::strncpy(addr.sun_path, server_->socketPath().c_str(), sizeof(addr.sun_path) - 1);
  socklen_t addrLen = static_cast<socklen_t>(
      offsetof(sockaddr_un, sun_path) + std::strlen(addr.sun_path));
  ASSERT_EQ(::connect(fd, reinterpret_cast<sockaddr*>(&addr), addrLen), 0);

  uint32_t frameLen = htonl(1);
  ::write(fd, &frameLen, sizeof(frameLen));
  uint8_t garbage = 0xFF;
  ::write(fd, &garbage, 1);

  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  char buf[64];
  ssize_t n = ::read(fd, buf, sizeof(buf));
  EXPECT_LE(n, 0);

  ::close(fd);
}

TEST_F(AdminServerTest, EmptyPayload) {
  createServer("/run/simm/simm_empty_payload");

  auto client = connectClient();
  uint16_t respType = 0;
  std::string respPayload;
  ASSERT_TRUE(client.sendRequest(AdminMsgType::GFLAG_LIST, "", respType, respPayload));

  proto::common::ListAllGFlagsResponsePB resp;
  ASSERT_TRUE(resp.ParseFromString(respPayload));
  EXPECT_EQ(resp.ret_code(), CommonErr::OK);
}

TEST_F(AdminServerTest, MalformedProtobufPayload) {
  createServer("/run/simm/simm_bad_proto");

  std::string garbage = "\x00\xFF\xFE\xAB\xCD";

  auto client = connectClient();
  uint16_t respType = 0;
  std::string respPayload;
  ASSERT_TRUE(client.sendRequest(AdminMsgType::GFLAG_GET, garbage, respType, respPayload));

  proto::common::GetGFlagValueResponsePB resp;
  ASSERT_TRUE(resp.ParseFromString(respPayload));
  EXPECT_EQ(resp.ret_code(), CommonErr::InvalidArgument);
}

// ---------------------------------------------------------------------------
// Concurrent client test
// ---------------------------------------------------------------------------

TEST_F(AdminServerTest, MultipleSequentialClients) {
  createServer("/run/simm/simm_multi_client");

  for (int i = 0; i < 5; ++i) {
    auto client = connectClient();
    uint16_t respType = 0;
    std::string respPayload;
    ASSERT_TRUE(client.sendRequest(AdminMsgType::GFLAG_LIST, "", respType, respPayload));

    proto::common::ListAllGFlagsResponsePB resp;
    ASSERT_TRUE(resp.ParseFromString(respPayload));
    EXPECT_EQ(resp.ret_code(), CommonErr::OK);
  }
}

TEST_F(AdminServerTest, ConcurrentClients) {
  createServer("/run/simm/simm_concurrent");

  constexpr int kNumClients = 10;
  std::atomic<int> successCount{0};
  std::vector<std::thread> threads;

  for (int i = 0; i < kNumClients; ++i) {
    threads.emplace_back([&, i]() {
      std::this_thread::sleep_for(std::chrono::milliseconds(i * 10));

      UdsClient client;
      if (!client.connect(server_->socketPath())) return;

      uint16_t respType = 0;
      std::string respPayload;
      if (!client.sendRequest(AdminMsgType::GFLAG_LIST, "", respType, respPayload)) return;

      proto::common::ListAllGFlagsResponsePB resp;
      if (resp.ParseFromString(respPayload) && resp.ret_code() == CommonErr::OK) {
        successCount.fetch_add(1);
      }
    });
  }

  for (auto& t : threads) t.join();

  EXPECT_EQ(successCount.load(), kNumClients);
}

// ---------------------------------------------------------------------------
// Shutdown tests
// ---------------------------------------------------------------------------

TEST_F(AdminServerTest, ShutdownWhileClientConnected) {
  createServer("/run/simm/simm_shutdown_client");
  std::string path = server_->socketPath();

  UdsClient client;
  ASSERT_TRUE(client.connect(path));

  server_.reset();

  struct stat st;
  EXPECT_NE(::stat(path.c_str(), &st), 0);
}

TEST_F(AdminServerTest, DoubleDestructionSafe) {
  createServer("/run/simm/simm_double_destroy");
  server_.reset();
  server_.reset();
}

TEST_F(AdminServerTest, StaleSocketFileOverwritten) {
  std::string stalePath = std::string("/run/simm/simm_stale.") +
                          std::to_string(::getpid()) + ".sock";
  {
    int fd = ::creat(stalePath.c_str(), 0666);
    if (fd >= 0) ::close(fd);
  }

  auto server = std::make_unique<AdminServer>("/run/simm/simm_stale");
  EXPECT_TRUE(server->isRunning());
  struct stat st;
  EXPECT_EQ(::stat(server->socketPath().c_str(), &st), 0);
  EXPECT_TRUE(S_ISSOCK(st.st_mode));

  server.reset();
  ::unlink(stalePath.c_str());
}

}  // namespace
}  // namespace common
}  // namespace simm
