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

AdminServer::AdminServer(std::string basePath)
    : basePath_(std::move(basePath)), listenFd_(-1), running_(false) {
  // Derive directory from basePath and ensure it exists
  std::string dirStr;
  auto pos = basePath_.find_last_of('/');
  if (pos == std::string::npos) {
    dirStr = ".";
  } else if (pos == 0) {
    dirStr = "/";
  } else {
    dirStr = basePath_.substr(0, pos);
  }
  struct stat st;
  if (::stat(dirStr.c_str(), &st) != 0) {
    if (::mkdir(dirStr.c_str(), 0777) != 0 && errno != EEXIST) {
      MLOG_ERROR("mkdir({}) failed, errno={}", dirStr, errno);
      return;
    }
  } else if (!S_ISDIR(st.st_mode)) {
    MLOG_ERROR("{} exists but is not a directory", dirStr);
    return;
  }

  // Socket path: <basePath>.<pid>.sock
  socketPath_ = basePath_ + "." + std::to_string(::getpid()) + ".sock";
  ::unlink(socketPath_.c_str());

  // Create self-pipe for clean shutdown
  if (::pipe(shutdownPipe_) < 0) {
    MLOG_ERROR("pipe() failed for shutdown self-pipe, errno={}", errno);
    goto err_exit;
  }
  // Make read end non-blocking
  ::fcntl(shutdownPipe_[0], F_SETFL, O_NONBLOCK);

  // Create listen socket
  listenFd_ = ::socket(AF_UNIX, SOCK_STREAM, 0);
  if (listenFd_ < 0) {
    MLOG_ERROR("socket(AF_UNIX) failed, errno={}", errno);
    goto err_exit;
  }

  {
    sockaddr_un addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    std::strncpy(addr.sun_path, socketPath_.c_str(), sizeof(addr.sun_path) - 1);
    addr.sun_path[sizeof(addr.sun_path) - 1] = '\0';

    socklen_t addrLen = static_cast<socklen_t>(
        offsetof(sockaddr_un, sun_path) + std::strlen(addr.sun_path));

    if (::bind(listenFd_, reinterpret_cast<sockaddr*>(&addr), addrLen) < 0) {
      MLOG_ERROR("bind({}) failed, errno={}", socketPath_, errno);
      goto err_exit;
    }
  }

  if (::listen(listenFd_, 16) < 0) {
    MLOG_ERROR("listen({}) failed, errno={}", socketPath_, errno);
    goto err_exit;
  }

  // Register built-in handlers (gflag + trace, available for all SiMM components)
  handlers_[static_cast<uint16_t>(AdminMsgType::GFLAG_LIST)] =
      [this](const std::string& p) { return handleGFlagList(p); };
  handlers_[static_cast<uint16_t>(AdminMsgType::GFLAG_GET)] =
      [this](const std::string& p) { return handleGFlagGet(p); };
  handlers_[static_cast<uint16_t>(AdminMsgType::GFLAG_SET)] =
      [this](const std::string& p) { return handleGFlagSet(p); };
  handlers_[static_cast<uint16_t>(AdminMsgType::TRACE_TOGGLE)] =
      [this](const std::string& p) { return handleTraceToggle(p); };

  // Spawn serve thread
  running_.store(true);
  worker_ = std::thread(&AdminServer::serveLoop, this);

  MLOG_INFO("AdminServer listening on {}", socketPath_);
  return;

err_exit:
  shutdown();
}

// ---------------------------------------------------------------------------
// Destruction: signal thread to exit, join, close fds, unlink socket
// ---------------------------------------------------------------------------

AdminServer::~AdminServer() {
  shutdown();
}

void AdminServer::shutdown() {
  if (!running_.exchange(false)) {
    // Already shut down or never started — just clean up fds
    if (listenFd_ >= 0) { ::close(listenFd_); listenFd_ = -1; }
    if (shutdownPipe_[0] >= 0) { ::close(shutdownPipe_[0]); shutdownPipe_[0] = -1; }
    if (shutdownPipe_[1] >= 0) { ::close(shutdownPipe_[1]); shutdownPipe_[1] = -1; }
    if (!socketPath_.empty()) { ::unlink(socketPath_.c_str()); }
    return;
  }

  // Wake the serve loop via self-pipe
  if (shutdownPipe_[1] >= 0) {
    char c = 'x';
    (void)::write(shutdownPipe_[1], &c, 1);
  }

  // Join serve thread
  if (worker_.joinable()) {
    worker_.join();
  }

  // Close all fds
  if (listenFd_ >= 0) { ::close(listenFd_); listenFd_ = -1; }
  if (shutdownPipe_[0] >= 0) { ::close(shutdownPipe_[0]); shutdownPipe_[0] = -1; }
  if (shutdownPipe_[1] >= 0) { ::close(shutdownPipe_[1]); shutdownPipe_[1] = -1; }

  // Remove socket file
  if (!socketPath_.empty()) {
    ::unlink(socketPath_.c_str());
    MLOG_INFO("AdminServer shut down, unlinked {}", socketPath_);
  }

  handlers_.clear();
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

void AdminServer::registerHandler(AdminMsgType msg_type, Handler handler) {
  std::unique_lock lock(handlersMutex_);
  handlers_[static_cast<uint16_t>(msg_type)] = std::move(handler);
}

// ---------------------------------------------------------------------------
// Serve loop: poll on listenFd + shutdownPipe, accept clients
// ---------------------------------------------------------------------------

void AdminServer::serveLoop() {
  while (running_.load()) {
    struct pollfd fds[2];
    fds[0].fd = listenFd_;
    fds[0].events = POLLIN;
    fds[1].fd = shutdownPipe_[0];
    fds[1].events = POLLIN;

    int ret = ::poll(fds, 2, -1);  // block until event
    if (ret < 0) {
      if (errno == EINTR) continue;
      MLOG_ERROR("poll() failed on {}, errno={}", socketPath_, errno);
      break;
    }

    // Check shutdown signal
    if (fds[1].revents & POLLIN) {
      break;  // woken up by destructor
    }

    // Check new client connection
    if (fds[0].revents & POLLIN) {
      int clientFd = ::accept(listenFd_, nullptr, nullptr);
      if (clientFd < 0) {
        if (errno == EINTR) continue;
        if (!running_.load()) break;
        MLOG_ERROR("accept() failed on {}, errno={}", socketPath_, errno);
        continue;
      }
      handleClient(clientFd);
      ::close(clientFd);
    }
  }
}

// ---------------------------------------------------------------------------
// Client handling: read frame, dispatch to handler, send response
// ---------------------------------------------------------------------------

void AdminServer::handleClient(int clientFd) {
  // Read frame: [uint32_t len][uint16_t type][payload]
  uint32_t lenNet = 0;
  if (!readExact(clientFd, &lenNet, sizeof(lenNet))) {
    MLOG_WARN("Failed to read frame length on {}", socketPath_);
    return;
  }
  uint32_t len = ntohl(lenNet);
  if (len < sizeof(uint16_t)) {
    MLOG_WARN("Invalid frame length {} on {}", len, socketPath_);
    return;
  }

  uint16_t typeNet = 0;
  if (!readExact(clientFd, &typeNet, sizeof(typeNet))) {
    MLOG_WARN("Failed to read msg type on {}", socketPath_);
    return;
  }
  uint16_t typeRaw = ntohs(typeNet);

  uint32_t payloadLen = len - static_cast<uint32_t>(sizeof(typeNet));
  std::string payload(payloadLen, '\0');
  if (payloadLen > 0 && !readExact(clientFd, payload.data(), payloadLen)) {
    MLOG_WARN("Failed to read payload on {}", socketPath_);
    return;
  }

  // Dispatch to registered handler
  std::shared_lock lock(handlersMutex_);
  auto it = handlers_.find(typeRaw);
  if (it != handlers_.end()) {
    std::string response = it->second(payload);
    lock.unlock();
    sendResponse(clientFd, static_cast<AdminMsgType>(typeRaw), response);
  } else {
    lock.unlock();
    MLOG_WARN("No handler for AdminMsgType {} on {}", typeRaw, socketPath_);
  }
}

// ---------------------------------------------------------------------------
// Built-in handlers
// ---------------------------------------------------------------------------

std::string AdminServer::handleGFlagList(const std::string& /*payload*/) {
  proto::common::ListAllGFlagsResponsePB resp;
  std::vector<gflags::CommandLineFlagInfo> allFlags;
  gflags::GetAllFlags(&allFlags);
  for (const auto& f : allFlags) {
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

std::string AdminServer::handleGFlagGet(const std::string& payload) {
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

std::string AdminServer::handleGFlagSet(const std::string& payload) {
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

std::string AdminServer::handleTraceToggle(const std::string& payload) {
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

bool AdminServer::readExact(int fd, void* buf, size_t len) {
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

void AdminServer::sendResponse(int clientFd, AdminMsgType type,
                                const std::string& serializedResp) {
  uint32_t respLen = static_cast<uint32_t>(sizeof(uint16_t) + serializedResp.size());
  uint32_t respLenNet = htonl(respLen);
  uint16_t respTypeNet = htons(static_cast<uint16_t>(type));

  struct iovec iov[3];
  iov[0].iov_base = &respLenNet;
  iov[0].iov_len  = sizeof(respLenNet);
  iov[1].iov_base = &respTypeNet;
  iov[1].iov_len  = sizeof(respTypeNet);
  iov[2].iov_base = const_cast<char*>(serializedResp.data());
  iov[2].iov_len  = serializedResp.size();

  ssize_t n = ::writev(clientFd, iov, 3);
  if (n < 0) {
    MLOG_WARN("Failed to send admin response on {}, errno={}", socketPath_, errno);
  }
}

}  // namespace common
}  // namespace simm
