#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "common/admin/admin_msg_types.h"
#include "common/admin/admin_server.h"
#include "common/errcode/errcode_def.h"
#include "proto/cm_clnt_rpcs.pb.h"
#include "proto/common.pb.h"

// Test-only gflag for GFlag handler tests
DEFINE_string(test_admin_flag, "default_value", "test flag for AdminServer UT");

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

  void createServer(const std::string& basePath = "/run/simm/admin_test") {
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
  createServer("/run/simm/admin_lifecycle");
  EXPECT_TRUE(server_->isRunning());
  struct stat st;
  EXPECT_EQ(::stat(server_->socketPath().c_str(), &st), 0);
  EXPECT_TRUE(S_ISSOCK(st.st_mode));
}

TEST_F(AdminServerTest, SocketPathFormat) {
  createServer("/run/simm/admin_ds");
  std::string expected = std::string("/run/simm/admin_ds.") +
                         std::to_string(::getpid()) + ".sock";
  EXPECT_EQ(server_->socketPath(), expected);
}

TEST_F(AdminServerTest, DestructorRemovesSocketFile) {
  createServer("/run/simm/admin_cleanup");
  std::string path = server_->socketPath();
  server_.reset();
  struct stat st;
  EXPECT_NE(::stat(path.c_str(), &st), 0);
}

TEST_F(AdminServerTest, MultipleServersWithDifferentBasePaths) {
  auto serverA = std::make_unique<AdminServer>("/run/simm/admin_testa");
  auto serverB = std::make_unique<AdminServer>("/run/simm/admin_testb");
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
  createServer("/run/simm/admin_running");
  EXPECT_TRUE(server_->isRunning());
  server_.reset();
  // After destruction, no server to check — just verify no crash
}

// ---------------------------------------------------------------------------
// GFlag handler tests
// ---------------------------------------------------------------------------

TEST_F(AdminServerTest, GFlagListReturnsFlags) {
  createServer("/run/simm/admin_gflag_list");
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
  createServer("/run/simm/admin_gflag_get");

  proto::common::GetGFlagValueRequestPB req;
  req.set_flag_name("test_admin_flag");
  std::string reqBuf;
  req.SerializeToString(&reqBuf);

  auto client = connectClient();
  uint16_t respType = 0;
  std::string respPayload;
  ASSERT_TRUE(client.sendRequest(AdminMsgType::GFLAG_GET, reqBuf, respType, respPayload));

  proto::common::GetGFlagValueResponsePB resp;
  ASSERT_TRUE(resp.ParseFromString(respPayload));
  EXPECT_EQ(resp.ret_code(), CommonErr::OK);
  EXPECT_EQ(resp.flag_info().flag_name(), "test_admin_flag");
}

TEST_F(AdminServerTest, GFlagGetUnknownFlag) {
  createServer("/run/simm/admin_gflag_get_unk");

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
  createServer("/run/simm/admin_gflag_set");

  proto::common::SetGFlagValueRequestPB setReq;
  setReq.set_flag_name("test_admin_flag");
  setReq.set_flag_value("new_value");
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
  getReq.set_flag_name("test_admin_flag");
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
    EXPECT_EQ(resp.flag_info().flag_value(), "new_value");
  }
}

TEST_F(AdminServerTest, GFlagSetNonexistentFlag) {
  createServer("/run/simm/admin_gflag_set_unk");

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
  createServer("/run/simm/admin_trace");

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
  createServer("/run/simm/admin_custom");

  server_->registerHandler(AdminMsgType::DS_STATUS,
      [](const std::string& payload) -> std::string {
        proto::common::AdmDsStatusResponsePB resp;
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

  proto::common::AdmDsStatusResponsePB resp;
  ASSERT_TRUE(resp.ParseFromString(respPayload));
  EXPECT_EQ(resp.ret_code(), CommonErr::OK);
  EXPECT_TRUE(resp.is_registered());
  EXPECT_TRUE(resp.cm_ready());
  EXPECT_EQ(resp.heartbeat_failure_count(), 0u);
}

TEST_F(AdminServerTest, CustomHandlerReceivesPayload) {
  createServer("/run/simm/admin_payload");

  server_->registerHandler(AdminMsgType::DS_STATUS,
      [](const std::string& payload) -> std::string {
        proto::common::AdmDsStatusResponsePB resp;
        resp.set_ret_code(CommonErr::OK);
        resp.set_heartbeat_failure_count(static_cast<uint32_t>(payload.size()));
        std::string buf;
        resp.SerializeToString(&buf);
        return buf;
      });

  proto::common::AdmDsStatusRequestPB req;
  std::string reqBuf;
  req.SerializeToString(&reqBuf);

  auto client = connectClient();
  uint16_t respType = 0;
  std::string respPayload;
  ASSERT_TRUE(client.sendRequest(AdminMsgType::DS_STATUS, reqBuf, respType, respPayload));

  proto::common::AdmDsStatusResponsePB resp;
  ASSERT_TRUE(resp.ParseFromString(respPayload));
  EXPECT_EQ(resp.ret_code(), CommonErr::OK);
  EXPECT_EQ(resp.heartbeat_failure_count(), 0u);
}

TEST_F(AdminServerTest, OverrideBuiltinHandler) {
  createServer("/run/simm/admin_override");

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
  createServer("/run/simm/admin_unknown_type");

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
  createServer("/run/simm/admin_bad_frame");

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
  createServer("/run/simm/admin_empty_payload");

  auto client = connectClient();
  uint16_t respType = 0;
  std::string respPayload;
  ASSERT_TRUE(client.sendRequest(AdminMsgType::GFLAG_LIST, "", respType, respPayload));

  proto::common::ListAllGFlagsResponsePB resp;
  ASSERT_TRUE(resp.ParseFromString(respPayload));
  EXPECT_EQ(resp.ret_code(), CommonErr::OK);
}

TEST_F(AdminServerTest, MalformedProtobufPayload) {
  createServer("/run/simm/admin_bad_proto");

  std::string garbage = "\x00\xFF\xFE\xAB\xCD";

  auto client = connectClient();
  uint16_t respType = 0;
  std::string respPayload;
  ASSERT_TRUE(client.sendRequest(AdminMsgType::GFLAG_GET, garbage, respType, respPayload));

  proto::common::GetGFlagValueResponsePB resp;
  ASSERT_TRUE(resp.ParseFromString(respPayload));
  // Garbled payload may still parse as protobuf with a junk flag name,
  // resulting in GFlagNotFound rather than InvalidArgument.
  EXPECT_NE(resp.ret_code(), CommonErr::OK);
}

// ---------------------------------------------------------------------------
// Concurrent client test
// ---------------------------------------------------------------------------

TEST_F(AdminServerTest, MultipleSequentialClients) {
  createServer("/run/simm/admin_multi_client");

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
  createServer("/run/simm/admin_concurrent");

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
  createServer("/run/simm/admin_shutdown_client");
  std::string path = server_->socketPath();

  UdsClient client;
  ASSERT_TRUE(client.connect(path));

  server_.reset();

  struct stat st;
  EXPECT_NE(::stat(path.c_str(), &st), 0);
}

TEST_F(AdminServerTest, DoubleDestructionSafe) {
  createServer("/run/simm/admin_double_destroy");
  server_.reset();
  server_.reset();
}

TEST_F(AdminServerTest, StaleSocketFileOverwritten) {
  std::string stalePath = std::string("/run/simm/admin_stale.") +
                          std::to_string(::getpid()) + ".sock";
  {
    int fd = ::creat(stalePath.c_str(), 0666);
    if (fd >= 0) ::close(fd);
  }

  auto server = std::make_unique<AdminServer>("/run/simm/admin_stale");
  EXPECT_TRUE(server->isRunning());
  struct stat st;
  EXPECT_EQ(::stat(server->socketPath().c_str(), &st), 0);
  EXPECT_TRUE(S_ISSOCK(st.st_mode));

  server.reset();
  ::unlink(stalePath.c_str());
}

// ---------------------------------------------------------------------------
// End-to-end tests: AdminServer + simmctl binary via subprocess
// ---------------------------------------------------------------------------

// Helper: locate simmctl binary relative to the test binary.
// Test binary: build/{mode}/bin/unit_tests/test_admin_server
// simmctl:     build/{mode}/bin/tools/simmctl
static std::string findSimmctlBinary() {
  // Try via /proc/self/exe
  char selfPath[4096];
  ssize_t len = ::readlink("/proc/self/exe", selfPath, sizeof(selfPath) - 1);
  if (len > 0) {
    selfPath[len] = '\0';
    std::string self(selfPath);
    // Go up from unit_tests/ to bin/, then into tools/
    auto pos = self.rfind('/');
    if (pos != std::string::npos) {
      std::string binDir = self.substr(0, pos);  // .../bin/unit_tests
      pos = binDir.rfind('/');
      if (pos != std::string::npos) {
        binDir = binDir.substr(0, pos);  // .../bin
        return binDir + "/tools/simmctl";
      }
    }
  }
  // Fallback: assume it's in PATH
  return "simmctl";
}

// Run simmctl as subprocess and capture stdout/stderr/exit code.
struct SimmctlResult {
  int exitCode;
  std::string stdoutStr;
  std::string stderrStr;
};

static SimmctlResult runSimmctl(const std::vector<std::string>& args) {
  std::string simmctl = findSimmctlBinary();
  std::string cmd = simmctl;
  for (const auto& arg : args) {
    cmd += " " + arg;
  }
  cmd += " 2>/tmp/simmctl_test_stderr";

  FILE* fp = popen(cmd.c_str(), "r");
  SimmctlResult result;
  result.exitCode = -1;
  if (!fp) {
    return result;
  }

  char buf[4096];
  while (fgets(buf, sizeof(buf), fp)) {
    result.stdoutStr += buf;
  }
  int status = pclose(fp);
  result.exitCode = WIFEXITED(status) ? WEXITSTATUS(status) : -1;

  // Read stderr
  FILE* errFp = fopen("/tmp/simmctl_test_stderr", "r");
  if (errFp) {
    while (fgets(buf, sizeof(buf), errFp)) {
      result.stderrStr += buf;
    }
    fclose(errFp);
    ::unlink("/tmp/simmctl_test_stderr");
  }

  return result;
}

class SimmctlE2ETest : public ::testing::Test {
 protected:
  void SetUp() override {
    ::mkdir("/run/simm", 0777);
    pidStr_ = std::to_string(::getpid());
  }

  void TearDown() override {
    server_.reset();
  }

  void createServer(const std::string& basePath) {
    server_ = std::make_unique<AdminServer>(basePath);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  std::unique_ptr<AdminServer> server_;
  std::string pidStr_;
};

TEST_F(SimmctlE2ETest, DsStatusViaPid) {
  createServer("/run/simm/admin_ds");

  // Register mock DS_STATUS handler
  server_->registerHandler(AdminMsgType::DS_STATUS,
      [](const std::string& /*payload*/) -> std::string {
        proto::common::AdmDsStatusResponsePB resp;
        resp.set_ret_code(CommonErr::OK);
        resp.set_is_registered(true);
        resp.set_cm_ready(true);
        resp.set_heartbeat_failure_count(0);
        std::string buf;
        resp.SerializeToString(&buf);
        return buf;
      });

  auto result = runSimmctl({"ds", "status", "--pid", pidStr_});
  EXPECT_EQ(result.exitCode, 0) << "stderr: " << result.stderrStr;
  EXPECT_NE(result.stdoutStr.find("is_registered"), std::string::npos);
  EXPECT_NE(result.stdoutStr.find("true"), std::string::npos);
  EXPECT_NE(result.stdoutStr.find("cm_ready"), std::string::npos);
}

TEST_F(SimmctlE2ETest, CmStatusViaPid) {
  createServer("/run/simm/admin_cm");

  server_->registerHandler(AdminMsgType::CM_STATUS,
      [](const std::string& /*payload*/) -> std::string {
        proto::common::AdmCmStatusResponsePB resp;
        resp.set_ret_code(CommonErr::OK);
        resp.set_is_running(true);
        resp.set_service_ready(true);
        resp.set_alive_node_count(3);
        resp.set_dead_node_count(0);
        resp.set_total_shard_count(64);
        std::string buf;
        resp.SerializeToString(&buf);
        return buf;
      });

  auto result = runSimmctl({"cm", "status", "--pid", pidStr_});
  EXPECT_EQ(result.exitCode, 0) << "stderr: " << result.stderrStr;
  EXPECT_NE(result.stdoutStr.find("is_running"), std::string::npos);
  EXPECT_NE(result.stdoutStr.find("true"), std::string::npos);
  EXPECT_NE(result.stdoutStr.find("alive_node_count"), std::string::npos);
  EXPECT_NE(result.stdoutStr.find("3"), std::string::npos);
}

TEST_F(SimmctlE2ETest, NodeListViaPid) {
  createServer("/run/simm/admin_cm");

  server_->registerHandler(AdminMsgType::NODE_LIST,
      [](const std::string& /*payload*/) -> std::string {
        ListNodesResponsePB resp;
        resp.set_ret_code(CommonErr::OK);

        auto* n1 = resp.add_nodes();
        n1->mutable_node_address()->set_ip("10.0.0.2");
        n1->mutable_node_address()->set_port(40000);
        n1->set_node_status(1);  // RUNNING

        auto* n2 = resp.add_nodes();
        n2->mutable_node_address()->set_ip("10.0.0.3");
        n2->mutable_node_address()->set_port(40000);
        n2->set_node_status(0);  // DEAD

        std::string buf;
        resp.SerializeToString(&buf);
        return buf;
      });

  auto result = runSimmctl({"node", "list", "--pid", pidStr_});
  EXPECT_EQ(result.exitCode, 0) << "stderr: " << result.stderrStr;
  EXPECT_NE(result.stdoutStr.find("10.0.0.2:40000"), std::string::npos);
  EXPECT_NE(result.stdoutStr.find("RUNNING"), std::string::npos);
  EXPECT_NE(result.stdoutStr.find("10.0.0.3:40000"), std::string::npos);
  EXPECT_NE(result.stdoutStr.find("DEAD"), std::string::npos);
}

TEST_F(SimmctlE2ETest, ShardListViaPid) {
  createServer("/run/simm/admin_cm");

  server_->registerHandler(AdminMsgType::SHARD_LIST,
      [](const std::string& /*payload*/) -> std::string {
        QueryShardRoutingTableAllResponsePB resp;
        resp.set_ret_code(CommonErr::OK);

        auto* entry = resp.add_shard_info();
        entry->mutable_data_server_address()->set_ip("10.0.0.2");
        entry->mutable_data_server_address()->set_port(40000);
        entry->add_shard_ids(0);
        entry->add_shard_ids(1);
        entry->add_shard_ids(2);

        std::string buf;
        resp.SerializeToString(&buf);
        return buf;
      });

  auto result = runSimmctl({"shard", "list", "--pid", pidStr_});
  EXPECT_EQ(result.exitCode, 0) << "stderr: " << result.stderrStr;
  EXPECT_NE(result.stdoutStr.find("10.0.0.2:40000"), std::string::npos);
  EXPECT_NE(result.stdoutStr.find("3"), std::string::npos);  // shard count
}

TEST_F(SimmctlE2ETest, GFlagGetViaPid) {
  // gflag with --pid uses simm_trace.<pid>.sock path
  createServer("/run/simm/simm_trace");

  // Built-in gflag handler should work — test_admin_flag is defined in this binary.
  // Note: a prior test (GFlagSetAndVerify) may have changed the value,
  // so just verify the output structure, not the specific value.
  auto result = runSimmctl({"gflag", "get", "test_admin_flag", "--pid", pidStr_});
  EXPECT_EQ(result.exitCode, 0) << "stderr: " << result.stderrStr;
  EXPECT_NE(result.stdoutStr.find("test_admin_flag"), std::string::npos);
  EXPECT_NE(result.stdoutStr.find("VALUE"), std::string::npos);
}

TEST_F(SimmctlE2ETest, PidAndProcMutuallyExclusive) {
  auto result = runSimmctl({"cm", "status", "--pid", "12345",
                            "--proc", "cluster_manager"});
  EXPECT_NE(result.exitCode, 0);
  EXPECT_NE(result.stderrStr.find("mutually exclusive"), std::string::npos);
}

TEST_F(SimmctlE2ETest, ProcNotFound) {
  // Use a process name that definitely doesn't exist
  auto result = runSimmctl({"cm", "status", "--proc", "cluster_manager"});
  // This will either fail because no cluster_manager is running,
  // or succeed if one happens to be running. We only check the not-found case
  // by using an unlikely process name via direct pgrep test.
  // For a deterministic test, just verify --proc with invalid name fails.
  auto result2 = runSimmctl({"cm", "status", "--proc", "nonexistent_proc_xyz"});
  EXPECT_NE(result2.exitCode, 0);
}

TEST_F(SimmctlE2ETest, ProcInvalidName) {
  auto result = runSimmctl({"cm", "status", "--proc", "invalid_name"});
  EXPECT_NE(result.exitCode, 0);
  EXPECT_NE(result.stderrStr.find("cluster_manager"), std::string::npos);
}

}  // namespace
}  // namespace common
}  // namespace simm
