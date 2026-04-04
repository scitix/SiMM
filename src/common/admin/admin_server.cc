#include "common/admin/admin_server.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <string>
#include <vector>

#include <gflags/gflags.h>

#include "common/errcode/errcode_def.h"
#include "common/logging/logging.h"
#include "proto/common.pb.h"

#ifdef SIMM_ENABLE_TRACE
#include "common/trace/trace.h"
#endif

DECLARE_LOG_MODULE("admin_server");

namespace simm {
namespace common {

// ---------------------------------------------------------------------------
// Construction: create socket, bind, listen, spawn serve thread
// ---------------------------------------------------------------------------

AdminServer::AdminServer(const std::string& role) : role_(role) {
  // Ensure /run/simm/ exists
  const char* base_dir = "/run/simm";
  struct stat st;
  if (::stat(base_dir, &st) != 0) {
    if (::mkdir(base_dir, 0777) != 0 && errno != EEXIST) {
      MLOG_ERROR("mkdir({}) failed, errno={}", base_dir, errno);
      return;
    }
  }

  // Socket path: /run/simm/simm_<role>.<pid>.sock
  socket_path_ = std::string(base_dir) + "/simm_" + role_ + "." +
                  std::to_string(::getpid()) + ".sock";
  ::unlink(socket_path_.c_str());

  // Create self-pipe for clean shutdown
  if (::pipe(shutdown_pipe_) < 0) {
    MLOG_ERROR("pipe() failed for shutdown self-pipe, errno={}", errno);
    return;
  }
  // Make read end non-blocking
  ::fcntl(shutdown_pipe_[0], F_SETFL, O_NONBLOCK);

  // Create listen socket
  listen_fd_ = ::socket(AF_UNIX, SOCK_STREAM, 0);
  if (listen_fd_ < 0) {
    MLOG_ERROR("socket(AF_UNIX) failed, errno={}", errno);
    return;
  }

  sockaddr_un addr;
  std::memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  std::strncpy(addr.sun_path, socket_path_.c_str(), sizeof(addr.sun_path) - 1);
  addr.sun_path[sizeof(addr.sun_path) - 1] = '\0';

  socklen_t addr_len = static_cast<socklen_t>(
      offsetof(sockaddr_un, sun_path) + std::strlen(addr.sun_path));

  if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), addr_len) < 0) {
    MLOG_ERROR("bind({}) failed, errno={}", socket_path_, errno);
    Shutdown();
    return;
  }

  if (::listen(listen_fd_, 16) < 0) {
    MLOG_ERROR("listen({}) failed, errno={}", socket_path_, errno);
    Shutdown();
    return;
  }

  // Register built-in handlers (gflag + trace, available for all processes)
  handlers_[static_cast<uint16_t>(AdminMsgType::GFLAG_LIST)] =
      [this](const std::string& p) { return HandleGFlagList(p); };
  handlers_[static_cast<uint16_t>(AdminMsgType::GFLAG_GET)] =
      [this](const std::string& p) { return HandleGFlagGet(p); };
  handlers_[static_cast<uint16_t>(AdminMsgType::GFLAG_SET)] =
      [this](const std::string& p) { return HandleGFlagSet(p); };
  handlers_[static_cast<uint16_t>(AdminMsgType::TRACE_TOGGLE)] =
      [this](const std::string& p) { return HandleTraceToggle(p); };

  // Spawn serve thread
  running_.store(true);
  worker_ = std::thread(&AdminServer::ServeLoop, this);

  MLOG_INFO("AdminServer({}) listening on {}", role_, socket_path_);
}

// ---------------------------------------------------------------------------
// Destruction: signal thread to exit, join, close fds, unlink socket
// ---------------------------------------------------------------------------

AdminServer::~AdminServer() {
  Shutdown();
}

void AdminServer::Shutdown() {
  if (!running_.exchange(false)) {
    // Already shut down or never started — just clean up fds
    if (listen_fd_ >= 0) { ::close(listen_fd_); listen_fd_ = -1; }
    if (shutdown_pipe_[0] >= 0) { ::close(shutdown_pipe_[0]); shutdown_pipe_[0] = -1; }
    if (shutdown_pipe_[1] >= 0) { ::close(shutdown_pipe_[1]); shutdown_pipe_[1] = -1; }
    if (!socket_path_.empty()) { ::unlink(socket_path_.c_str()); }
    return;
  }

  // Wake the serve loop via self-pipe
  if (shutdown_pipe_[1] >= 0) {
    char c = 'x';
    (void)::write(shutdown_pipe_[1], &c, 1);
  }

  // Join serve thread
  if (worker_.joinable()) {
    worker_.join();
  }

  // Close all fds
  if (listen_fd_ >= 0) { ::close(listen_fd_); listen_fd_ = -1; }
  if (shutdown_pipe_[0] >= 0) { ::close(shutdown_pipe_[0]); shutdown_pipe_[0] = -1; }
  if (shutdown_pipe_[1] >= 0) { ::close(shutdown_pipe_[1]); shutdown_pipe_[1] = -1; }

  // Remove socket file
  if (!socket_path_.empty()) {
    ::unlink(socket_path_.c_str());
    MLOG_INFO("AdminServer({}) shut down, unlinked {}", role_, socket_path_);
  }

  handlers_.clear();
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

void AdminServer::RegisterHandler(AdminMsgType msg_type, Handler handler) {
  handlers_[static_cast<uint16_t>(msg_type)] = std::move(handler);
}

// ---------------------------------------------------------------------------
// Serve loop: poll on listen_fd + shutdown_pipe, accept clients
// ---------------------------------------------------------------------------

void AdminServer::ServeLoop() {
  while (running_.load()) {
    struct pollfd fds[2];
    fds[0].fd = listen_fd_;
    fds[0].events = POLLIN;
    fds[1].fd = shutdown_pipe_[0];
    fds[1].events = POLLIN;

    int ret = ::poll(fds, 2, -1);  // block until event
    if (ret < 0) {
      if (errno == EINTR) continue;
      MLOG_ERROR("poll() failed on {}, errno={}", socket_path_, errno);
      break;
    }

    // Check shutdown signal
    if (fds[1].revents & POLLIN) {
      break;  // woken up by destructor
    }

    // Check new client connection
    if (fds[0].revents & POLLIN) {
      int client_fd = ::accept(listen_fd_, nullptr, nullptr);
      if (client_fd < 0) {
        if (errno == EINTR) continue;
        if (!running_.load()) break;
        MLOG_ERROR("accept() failed on {}, errno={}", socket_path_, errno);
        continue;
      }
      HandleClient(client_fd);
      ::close(client_fd);
    }
  }
}

// ---------------------------------------------------------------------------
// Client handling: read frame, dispatch to handler, send response
// ---------------------------------------------------------------------------

void AdminServer::HandleClient(int client_fd) {
  // Read frame: [uint32_t len][uint16_t type][payload]
  uint32_t len_net = 0;
  if (!ReadExact(client_fd, &len_net, sizeof(len_net))) {
    MLOG_WARN("Failed to read frame length on {}", socket_path_);
    return;
  }
  uint32_t len = ntohl(len_net);
  if (len < sizeof(uint16_t)) {
    MLOG_WARN("Invalid frame length {} on {}", len, socket_path_);
    return;
  }

  uint16_t type_net = 0;
  if (!ReadExact(client_fd, &type_net, sizeof(type_net))) {
    MLOG_WARN("Failed to read msg type on {}", socket_path_);
    return;
  }
  uint16_t type_raw = ntohs(type_net);

  uint32_t payload_len = len - static_cast<uint32_t>(sizeof(type_net));
  std::string payload(payload_len, '\0');
  if (payload_len > 0 && !ReadExact(client_fd, payload.data(), payload_len)) {
    MLOG_WARN("Failed to read payload on {}", socket_path_);
    return;
  }

  // Dispatch to registered handler
  auto it = handlers_.find(type_raw);
  if (it != handlers_.end()) {
    std::string response = it->second(payload);
    SendResponse(client_fd, static_cast<AdminMsgType>(type_raw), response);
  } else {
    MLOG_WARN("No handler for AdminMsgType {} on {}", type_raw, socket_path_);
  }
}

// ---------------------------------------------------------------------------
// Built-in handlers
// ---------------------------------------------------------------------------

std::string AdminServer::HandleGFlagList(const std::string& /*payload*/) {
  proto::common::ListAllGFlagsResponsePB resp;
  std::vector<gflags::CommandLineFlagInfo> all_flags;
  gflags::GetAllFlags(&all_flags);
  for (const auto& f : all_flags) {
    auto* rf = resp.add_flags();
    rf->set_flag_name(f.name);
    rf->set_flag_value(f.current_value);
    rf->set_flag_default_value(f.default_value);
    rf->set_flag_type(f.type);
    rf->set_flag_description(f.description);
  }
  resp.set_ret_code(CommonErr::OK);
  std::string buf;
  resp.SerializeToString(&buf);
  return buf;
}

std::string AdminServer::HandleGFlagGet(const std::string& payload) {
  proto::common::GetGFlagValueRequestPB req;
  proto::common::GetGFlagValueResponsePB resp;
  if (!req.ParseFromString(payload)) {
    resp.set_ret_code(CommonErr::InvalidArgument);
  } else {
    gflags::CommandLineFlagInfo info;
    if (!gflags::GetCommandLineFlagInfo(req.flag_name().c_str(), &info)) {
      resp.set_ret_code(CommonErr::GFlagNotFound);
    } else {
      resp.set_ret_code(CommonErr::OK);
      auto* fi = resp.mutable_flag_info();
      fi->set_flag_name(info.name);
      fi->set_flag_value(info.current_value);
      fi->set_flag_default_value(info.default_value);
      fi->set_flag_type(info.type);
      fi->set_flag_description(info.description);
    }
  }
  std::string buf;
  resp.SerializeToString(&buf);
  return buf;
}

std::string AdminServer::HandleGFlagSet(const std::string& payload) {
  proto::common::SetGFlagValueRequestPB req;
  proto::common::SetGFlagValueResponsePB resp;
  if (!req.ParseFromString(payload)) {
    resp.set_ret_code(CommonErr::InvalidArgument);
  } else {
    gflags::CommandLineFlagInfo info;
    if (!gflags::GetCommandLineFlagInfo(req.flag_name().c_str(), &info)) {
      resp.set_ret_code(CommonErr::GFlagNotFound);
    } else {
      auto res = gflags::SetCommandLineOption(
          req.flag_name().c_str(), req.flag_value().c_str());
      resp.set_ret_code(res.empty() ? CommonErr::GFlagSetFailed : CommonErr::OK);
    }
  }
  std::string buf;
  resp.SerializeToString(&buf);
  return buf;
}

std::string AdminServer::HandleTraceToggle(const std::string& payload) {
  proto::common::TraceToggleRequestPB req;
  proto::common::TraceToggleResponsePB resp;
  if (!req.ParseFromString(payload)) {
    resp.set_ret_code(CommonErr::InvalidArgument);
  } else {
#ifdef SIMM_ENABLE_TRACE
    simm::trace::TraceManager::SetEnabled(req.enable_trace());
#endif
    resp.set_ret_code(CommonErr::OK);
  }
  std::string buf;
  resp.SerializeToString(&buf);
  return buf;
}

// ---------------------------------------------------------------------------
// UDS I/O helpers
// ---------------------------------------------------------------------------

bool AdminServer::ReadExact(int fd, void* buf, size_t len) {
  char* p = static_cast<char*>(buf);
  size_t remaining = len;
  while (remaining > 0) {
    ssize_t n = ::read(fd, p, remaining);
    if (n > 0) {
      remaining -= static_cast<size_t>(n);
      p += n;
    } else if (n == 0) {
      return false;
    } else {
      if (errno == EINTR) continue;
      return false;
    }
  }
  return true;
}

void AdminServer::SendResponse(int client_fd, AdminMsgType type,
                                const std::string& serialized_resp) {
  uint32_t resp_len = static_cast<uint32_t>(sizeof(uint16_t) + serialized_resp.size());
  uint32_t resp_len_net = htonl(resp_len);
  uint16_t resp_type_net = htons(static_cast<uint16_t>(type));

  struct iovec iov[3];
  iov[0].iov_base = &resp_len_net;
  iov[0].iov_len  = sizeof(resp_len_net);
  iov[1].iov_base = &resp_type_net;
  iov[1].iov_len  = sizeof(resp_type_net);
  iov[2].iov_base = const_cast<char*>(serialized_resp.data());
  iov[2].iov_len  = serialized_resp.size();

  ssize_t n = ::writev(client_fd, iov, 3);
  if (n < 0) {
    MLOG_WARN("Failed to send admin response on {}, errno={}", socket_path_, errno);
  }
}

}  // namespace common
}  // namespace simm
