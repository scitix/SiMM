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
  // Connect to a UDS socket. Returns false on failure.
  bool Connect(const std::string& socket_path) {
    fd_ = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd_ < 0) return false;

    sockaddr_un addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    std::strncpy(addr.sun_path, socket_path.c_str(), sizeof(addr.sun_path) - 1);

    socklen_t len = static_cast<socklen_t>(
        offsetof(sockaddr_un, sun_path) + std::strlen(addr.sun_path));

    if (::connect(fd_, reinterpret_cast<sockaddr*>(&addr), len) < 0) {
      ::close(fd_);
      fd_ = -1;
      return false;
    }
    return true;
  }

  ~UdsClient() { Close(); }

  void Close() {
    if (fd_ >= 0) { ::close(fd_); fd_ = -1; }
  }

  // Send request and receive response.
  // Returns false on I/O error; on success, populates resp_type and resp_payload.
  bool SendRequest(AdminMsgType type, const std::string& payload,
                   uint16_t& resp_type, std::string& resp_payload) {
    if (fd_ < 0) return false;

    // Build request frame
    uint32_t frame_len = static_cast<uint32_t>(sizeof(uint16_t) + payload.size());
    uint32_t frame_len_net = htonl(frame_len);
    uint16_t type_net = htons(static_cast<uint16_t>(type));

    if (!WriteAll(&frame_len_net, sizeof(frame_len_net))) return false;
    if (!WriteAll(&type_net, sizeof(type_net))) return false;
    if (!payload.empty() && !WriteAll(payload.data(), payload.size())) return false;

    // Read response frame
    uint32_t resp_len_net = 0;
    if (!ReadAll(&resp_len_net, sizeof(resp_len_net))) return false;
    uint32_t resp_len = ntohl(resp_len_net);
    if (resp_len < sizeof(uint16_t)) return false;

    uint16_t resp_type_net = 0;
    if (!ReadAll(&resp_type_net, sizeof(resp_type_net))) return false;
    resp_type = ntohs(resp_type_net);

    uint32_t payload_len = resp_len - static_cast<uint32_t>(sizeof(uint16_t));
    resp_payload.resize(payload_len);
    if (payload_len > 0 && !ReadAll(resp_payload.data(), payload_len)) return false;

    return true;
  }

 private:
  bool WriteAll(const void* buf, size_t len) {
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

  bool ReadAll(void* buf, size_t len) {
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
    // Ensure /run/simm exists
    ::mkdir("/run/simm", 0777);
  }

  void TearDown() override {
    server_.reset();
  }

  void CreateServer(const std::string& role = "test") {
    server_ = std::make_unique<AdminServer>(role);
    // Give the serve thread a moment to start polling
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  UdsClient ConnectClient() {
    UdsClient client;
    EXPECT_TRUE(client.Connect(server_->SocketPath()));
    return client;
  }

  std::unique_ptr<AdminServer> server_;
};

// ---------------------------------------------------------------------------
// Lifecycle tests
// ---------------------------------------------------------------------------

TEST_F(AdminServerTest, ConstructionCreatesSocketFile) {
  CreateServer("test_lifecycle");
  struct stat st;
  EXPECT_EQ(::stat(server_->SocketPath().c_str(), &st), 0);
  EXPECT_TRUE(S_ISSOCK(st.st_mode));
}

TEST_F(AdminServerTest, SocketPathFormat) {
  CreateServer("ds");
  std::string expected = std::string("/run/simm/simm_ds.") +
                         std::to_string(::getpid()) + ".sock";
  EXPECT_EQ(server_->SocketPath(), expected);
}

TEST_F(AdminServerTest, DestructorRemovesSocketFile) {
  CreateServer("test_cleanup");
  std::string path = server_->SocketPath();
  server_.reset();  // trigger destructor
  struct stat st;
  EXPECT_NE(::stat(path.c_str(), &st), 0);  // file should be gone
}

TEST_F(AdminServerTest, MultipleServersWithDifferentRoles) {
  auto server_a = std::make_unique<AdminServer>("testa");
  auto server_b = std::make_unique<AdminServer>("testb");
  EXPECT_NE(server_a->SocketPath(), server_b->SocketPath());

  struct stat st;
  EXPECT_EQ(::stat(server_a->SocketPath().c_str(), &st), 0);
  EXPECT_EQ(::stat(server_b->SocketPath().c_str(), &st), 0);

  std::string path_a = server_a->SocketPath();
  std::string path_b = server_b->SocketPath();
  server_a.reset();
  server_b.reset();
  EXPECT_NE(::stat(path_a.c_str(), &st), 0);
  EXPECT_NE(::stat(path_b.c_str(), &st), 0);
}

// ---------------------------------------------------------------------------
// GFlag handler tests
// ---------------------------------------------------------------------------

TEST_F(AdminServerTest, GFlagListReturnsFlags) {
  CreateServer("test_gflag_list");
  auto client = ConnectClient();

  uint16_t resp_type = 0;
  std::string resp_payload;
  ASSERT_TRUE(client.SendRequest(AdminMsgType::GFLAG_LIST, "", resp_type, resp_payload));
  EXPECT_EQ(resp_type, static_cast<uint16_t>(AdminMsgType::GFLAG_LIST));

  proto::common::ListAllGFlagsResponsePB resp;
  ASSERT_TRUE(resp.ParseFromString(resp_payload));
  EXPECT_EQ(resp.ret_code(), CommonErr::OK);
  EXPECT_GT(resp.flags_size(), 0);  // gflags always has some flags registered
}

TEST_F(AdminServerTest, GFlagGetKnownFlag) {
  CreateServer("test_gflag_get");

  // Use a well-known gtest flag that always exists
  proto::common::GetGFlagValueRequestPB req;
  req.set_flag_name("gtest_color");
  std::string req_buf;
  req.SerializeToString(&req_buf);

  auto client = ConnectClient();
  uint16_t resp_type = 0;
  std::string resp_payload;
  ASSERT_TRUE(client.SendRequest(AdminMsgType::GFLAG_GET, req_buf, resp_type, resp_payload));

  proto::common::GetGFlagValueResponsePB resp;
  ASSERT_TRUE(resp.ParseFromString(resp_payload));
  EXPECT_EQ(resp.ret_code(), CommonErr::OK);
  EXPECT_EQ(resp.flag_info().flag_name(), "gtest_color");
}

TEST_F(AdminServerTest, GFlagGetUnknownFlag) {
  CreateServer("test_gflag_get_unk");

  proto::common::GetGFlagValueRequestPB req;
  req.set_flag_name("nonexistent_flag_12345");
  std::string req_buf;
  req.SerializeToString(&req_buf);

  auto client = ConnectClient();
  uint16_t resp_type = 0;
  std::string resp_payload;
  ASSERT_TRUE(client.SendRequest(AdminMsgType::GFLAG_GET, req_buf, resp_type, resp_payload));

  proto::common::GetGFlagValueResponsePB resp;
  ASSERT_TRUE(resp.ParseFromString(resp_payload));
  EXPECT_EQ(resp.ret_code(), CommonErr::GFlagNotFound);
}

TEST_F(AdminServerTest, GFlagSetAndVerify) {
  CreateServer("test_gflag_set");

  // Set gtest_color to "no"
  proto::common::SetGFlagValueRequestPB set_req;
  set_req.set_flag_name("gtest_color");
  set_req.set_flag_value("no");
  std::string set_buf;
  set_req.SerializeToString(&set_buf);

  {
    auto client = ConnectClient();
    uint16_t resp_type = 0;
    std::string resp_payload;
    ASSERT_TRUE(client.SendRequest(AdminMsgType::GFLAG_SET, set_buf, resp_type, resp_payload));

    proto::common::SetGFlagValueResponsePB resp;
    ASSERT_TRUE(resp.ParseFromString(resp_payload));
    EXPECT_EQ(resp.ret_code(), CommonErr::OK);
  }

  // Verify the change via GFLAG_GET
  proto::common::GetGFlagValueRequestPB get_req;
  get_req.set_flag_name("gtest_color");
  std::string get_buf;
  get_req.SerializeToString(&get_buf);

  {
    auto client = ConnectClient();
    uint16_t resp_type = 0;
    std::string resp_payload;
    ASSERT_TRUE(client.SendRequest(AdminMsgType::GFLAG_GET, get_buf, resp_type, resp_payload));

    proto::common::GetGFlagValueResponsePB resp;
    ASSERT_TRUE(resp.ParseFromString(resp_payload));
    EXPECT_EQ(resp.ret_code(), CommonErr::OK);
    EXPECT_EQ(resp.flag_info().flag_value(), "no");
  }
}

TEST_F(AdminServerTest, GFlagSetNonexistentFlag) {
  CreateServer("test_gflag_set_unk");

  proto::common::SetGFlagValueRequestPB req;
  req.set_flag_name("nonexistent_flag_12345");
  req.set_flag_value("42");
  std::string req_buf;
  req.SerializeToString(&req_buf);

  auto client = ConnectClient();
  uint16_t resp_type = 0;
  std::string resp_payload;
  ASSERT_TRUE(client.SendRequest(AdminMsgType::GFLAG_SET, req_buf, resp_type, resp_payload));

  proto::common::SetGFlagValueResponsePB resp;
  ASSERT_TRUE(resp.ParseFromString(resp_payload));
  EXPECT_EQ(resp.ret_code(), CommonErr::GFlagNotFound);
}

// ---------------------------------------------------------------------------
// Trace toggle handler test
// ---------------------------------------------------------------------------

TEST_F(AdminServerTest, TraceToggle) {
  CreateServer("test_trace");

  proto::common::TraceToggleRequestPB req;
  req.set_enable_trace(true);
  std::string req_buf;
  req.SerializeToString(&req_buf);

  auto client = ConnectClient();
  uint16_t resp_type = 0;
  std::string resp_payload;
  ASSERT_TRUE(client.SendRequest(AdminMsgType::TRACE_TOGGLE, req_buf, resp_type, resp_payload));
  EXPECT_EQ(resp_type, static_cast<uint16_t>(AdminMsgType::TRACE_TOGGLE));

  proto::common::TraceToggleResponsePB resp;
  ASSERT_TRUE(resp.ParseFromString(resp_payload));
  EXPECT_EQ(resp.ret_code(), CommonErr::OK);
}

// ---------------------------------------------------------------------------
// Custom handler registration tests
// ---------------------------------------------------------------------------

TEST_F(AdminServerTest, RegisterCustomHandler) {
  CreateServer("test_custom");

  // Register a custom DS_STATUS handler
  server_->RegisterHandler(AdminMsgType::DS_STATUS,
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

  auto client = ConnectClient();
  uint16_t resp_type = 0;
  std::string resp_payload;
  ASSERT_TRUE(client.SendRequest(AdminMsgType::DS_STATUS, "", resp_type, resp_payload));
  EXPECT_EQ(resp_type, static_cast<uint16_t>(AdminMsgType::DS_STATUS));

  proto::common::DsStatusResponsePB resp;
  ASSERT_TRUE(resp.ParseFromString(resp_payload));
  EXPECT_EQ(resp.ret_code(), CommonErr::OK);
  EXPECT_TRUE(resp.is_registered());
  EXPECT_TRUE(resp.cm_ready());
  EXPECT_EQ(resp.heartbeat_failure_count(), 0u);
}

TEST_F(AdminServerTest, CustomHandlerReceivesPayload) {
  CreateServer("test_payload");

  // Register a handler that echoes payload length in a DsStatusResponse
  server_->RegisterHandler(AdminMsgType::DS_STATUS,
      [](const std::string& payload) -> std::string {
        proto::common::DsStatusResponsePB resp;
        resp.set_ret_code(CommonErr::OK);
        resp.set_heartbeat_failure_count(static_cast<uint32_t>(payload.size()));
        std::string buf;
        resp.SerializeToString(&buf);
        return buf;
      });

  proto::common::DsStatusRequestPB req;
  std::string req_buf;
  req.SerializeToString(&req_buf);

  auto client = ConnectClient();
  uint16_t resp_type = 0;
  std::string resp_payload;
  ASSERT_TRUE(client.SendRequest(AdminMsgType::DS_STATUS, req_buf, resp_type, resp_payload));

  proto::common::DsStatusResponsePB resp;
  ASSERT_TRUE(resp.ParseFromString(resp_payload));
  EXPECT_EQ(resp.ret_code(), CommonErr::OK);
  // DsStatusRequestPB serializes to 0 bytes (empty message)
  EXPECT_EQ(resp.heartbeat_failure_count(), 0u);
}

TEST_F(AdminServerTest, OverrideBuiltinHandler) {
  CreateServer("test_override");

  // Override the built-in GFLAG_LIST handler with a custom one
  server_->RegisterHandler(AdminMsgType::GFLAG_LIST,
      [](const std::string& /*payload*/) -> std::string {
        proto::common::ListAllGFlagsResponsePB resp;
        resp.set_ret_code(CommonErr::OK);
        // Return an empty flag list instead of real flags
        std::string buf;
        resp.SerializeToString(&buf);
        return buf;
      });

  auto client = ConnectClient();
  uint16_t resp_type = 0;
  std::string resp_payload;
  ASSERT_TRUE(client.SendRequest(AdminMsgType::GFLAG_LIST, "", resp_type, resp_payload));

  proto::common::ListAllGFlagsResponsePB resp;
  ASSERT_TRUE(resp.ParseFromString(resp_payload));
  EXPECT_EQ(resp.ret_code(), CommonErr::OK);
  EXPECT_EQ(resp.flags_size(), 0);  // custom handler returns empty list
}

// ---------------------------------------------------------------------------
// Protocol robustness tests
// ---------------------------------------------------------------------------

TEST_F(AdminServerTest, UnregisteredMsgTypeNoResponse) {
  CreateServer("test_unknown_type");

  // Send a message type that has no handler (raw value 999)
  UdsClient client;
  ASSERT_TRUE(client.Connect(server_->SocketPath()));

  int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
  ASSERT_GE(fd, 0);

  sockaddr_un addr;
  std::memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  std::strncpy(addr.sun_path, server_->SocketPath().c_str(), sizeof(addr.sun_path) - 1);
  socklen_t addr_len = static_cast<socklen_t>(
      offsetof(sockaddr_un, sun_path) + std::strlen(addr.sun_path));
  ASSERT_EQ(::connect(fd, reinterpret_cast<sockaddr*>(&addr), addr_len), 0);

  // Send frame with unregistered type 999
  uint32_t frame_len = htonl(sizeof(uint16_t));
  uint16_t msg_type = htons(999);
  ::write(fd, &frame_len, sizeof(frame_len));
  ::write(fd, &msg_type, sizeof(msg_type));

  // Server should close the connection (no response for unregistered type).
  // Try to read — should get EOF.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  char buf[64];
  ssize_t n = ::read(fd, buf, sizeof(buf));
  EXPECT_LE(n, 0);  // EOF or error

  ::close(fd);
}

TEST_F(AdminServerTest, InvalidFrameTooShort) {
  CreateServer("test_bad_frame");

  int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
  ASSERT_GE(fd, 0);

  sockaddr_un addr;
  std::memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  std::strncpy(addr.sun_path, server_->SocketPath().c_str(), sizeof(addr.sun_path) - 1);
  socklen_t addr_len = static_cast<socklen_t>(
      offsetof(sockaddr_un, sun_path) + std::strlen(addr.sun_path));
  ASSERT_EQ(::connect(fd, reinterpret_cast<sockaddr*>(&addr), addr_len), 0);

  // Send frame_len = 1 (< sizeof(uint16_t)), which is invalid
  uint32_t frame_len = htonl(1);
  ::write(fd, &frame_len, sizeof(frame_len));
  uint8_t garbage = 0xFF;
  ::write(fd, &garbage, 1);

  // Server should reject and close connection
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  char buf[64];
  ssize_t n = ::read(fd, buf, sizeof(buf));
  EXPECT_LE(n, 0);

  ::close(fd);
}

TEST_F(AdminServerTest, EmptyPayload) {
  CreateServer("test_empty_payload");

  // GFLAG_LIST expects no payload — should work fine with empty payload
  auto client = ConnectClient();
  uint16_t resp_type = 0;
  std::string resp_payload;
  ASSERT_TRUE(client.SendRequest(AdminMsgType::GFLAG_LIST, "", resp_type, resp_payload));

  proto::common::ListAllGFlagsResponsePB resp;
  ASSERT_TRUE(resp.ParseFromString(resp_payload));
  EXPECT_EQ(resp.ret_code(), CommonErr::OK);
}

TEST_F(AdminServerTest, MalformedProtobufPayload) {
  CreateServer("test_bad_proto");

  // Send garbage bytes as payload for GFLAG_GET (expects a valid protobuf)
  std::string garbage = "\x00\xFF\xFE\xAB\xCD";

  auto client = ConnectClient();
  uint16_t resp_type = 0;
  std::string resp_payload;
  ASSERT_TRUE(client.SendRequest(AdminMsgType::GFLAG_GET, garbage, resp_type, resp_payload));

  proto::common::GetGFlagValueResponsePB resp;
  ASSERT_TRUE(resp.ParseFromString(resp_payload));
  EXPECT_EQ(resp.ret_code(), CommonErr::InvalidArgument);
}

// ---------------------------------------------------------------------------
// Concurrent client test
// ---------------------------------------------------------------------------

TEST_F(AdminServerTest, MultipleSequentialClients) {
  CreateServer("test_multi_client");

  for (int i = 0; i < 5; ++i) {
    auto client = ConnectClient();
    uint16_t resp_type = 0;
    std::string resp_payload;
    ASSERT_TRUE(client.SendRequest(AdminMsgType::GFLAG_LIST, "", resp_type, resp_payload));

    proto::common::ListAllGFlagsResponsePB resp;
    ASSERT_TRUE(resp.ParseFromString(resp_payload));
    EXPECT_EQ(resp.ret_code(), CommonErr::OK);
  }
}

TEST_F(AdminServerTest, ConcurrentClients) {
  CreateServer("test_concurrent");

  constexpr int kNumClients = 10;
  std::atomic<int> success_count{0};
  std::vector<std::thread> threads;

  for (int i = 0; i < kNumClients; ++i) {
    threads.emplace_back([&, i]() {
      // Stagger connections slightly
      std::this_thread::sleep_for(std::chrono::milliseconds(i * 10));

      UdsClient client;
      if (!client.Connect(server_->SocketPath())) return;

      uint16_t resp_type = 0;
      std::string resp_payload;
      if (!client.SendRequest(AdminMsgType::GFLAG_LIST, "", resp_type, resp_payload)) return;

      proto::common::ListAllGFlagsResponsePB resp;
      if (resp.ParseFromString(resp_payload) && resp.ret_code() == CommonErr::OK) {
        success_count.fetch_add(1);
      }
    });
  }

  for (auto& t : threads) t.join();

  // AdminServer handles clients sequentially (single-threaded accept loop),
  // so all clients should eventually be served.
  EXPECT_EQ(success_count.load(), kNumClients);
}

// ---------------------------------------------------------------------------
// Shutdown tests
// ---------------------------------------------------------------------------

TEST_F(AdminServerTest, ShutdownWhileClientConnected) {
  CreateServer("test_shutdown_client");
  std::string path = server_->SocketPath();

  // Connect but don't send anything yet
  UdsClient client;
  ASSERT_TRUE(client.Connect(path));

  // Destroy server — should not hang or crash
  server_.reset();

  // Socket file should be cleaned up
  struct stat st;
  EXPECT_NE(::stat(path.c_str(), &st), 0);
}

TEST_F(AdminServerTest, DoubleDestructionSafe) {
  CreateServer("test_double_destroy");
  // Just destroy — no crash or hang
  server_.reset();
  // Calling reset again is a no-op (unique_ptr)
  server_.reset();
}

TEST_F(AdminServerTest, StaleSocketFileOverwritten) {
  // Create a stale socket file manually
  std::string stale_path = std::string("/run/simm/simm_stale.") +
                           std::to_string(::getpid()) + ".sock";
  {
    int fd = ::creat(stale_path.c_str(), 0666);
    if (fd >= 0) ::close(fd);
  }

  // AdminServer should unlink the stale file and bind successfully
  auto server = std::make_unique<AdminServer>("stale");
  struct stat st;
  EXPECT_EQ(::stat(server->SocketPath().c_str(), &st), 0);
  EXPECT_TRUE(S_ISSOCK(st.st_mode));

  server.reset();
  // Clean up
  ::unlink(stale_path.c_str());
}

}  // namespace
}  // namespace common
}  // namespace simm
