#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <atomic>
#include <cerrno>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include <gflags/gflags.h>

#include "trace.h"
#include "trace_server.h"
#include "common/logging/logging.h"
#include "common/errcode/errcode_def.h"
#include "proto/common.pb.h"

DECLARE_LOG_MODULE("trace");

namespace simm {
namespace trace {

// XXX: Should keep in sync with common/admin/admin_msg_types.h
enum class AdminMsgType : uint16_t {
  TRACE_TOGGLE = 1,
  GFLAG_LIST   = 2,
  GFLAG_GET    = 3,
  GFLAG_SET    = 4,
};

TraceServer::TraceServer(std::string basePath)
    : basePath_(std::move(basePath)), listenFd_(-1), running_(false) {
  // Ensure base directory for unix socket exists, derived from basePath_
  std::string dir_str;
  auto pos = basePath_.find_last_of('/');
  if (pos == std::string::npos) {
    dir_str = ".";  // no slash, use current directory
  } else if (pos == 0) {
    dir_str = "/";   // root
  } else {
    dir_str = basePath_.substr(0, pos);
  }

  const char *dir = dir_str.c_str();
  struct stat st;
  if (::stat(dir, &st) != 0) {
    // Not exist, try create
    if (::mkdir(dir, 0777) != 0 && errno != EEXIST) {
      MLOG_ERROR("mkdir({}) failed, errno={}", dir, errno);
      return ;
    }
  } else if (!S_ISDIR(st.st_mode)) {
    MLOG_ERROR("{} exists but is not a directory", dir);
    return ;
  }

  socketPath_ = basePath_ + "." + std::to_string(::getpid()) + ".sock";

  ::unlink(socketPath_.c_str());

  listenFd_ = ::socket(AF_UNIX, SOCK_STREAM, 0);
  if (listenFd_ < 0) {
    MLOG_ERROR("socket(AF_UNIX) failed, errno={}", errno);
    return ;
  }

  sockaddr_un addr;
  std::memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  std::strncpy(addr.sun_path, socketPath_.c_str(), sizeof(addr.sun_path) - 1);
  addr.sun_path[sizeof(addr.sun_path) - 1] = '\0';

  socklen_t addrLen = static_cast<socklen_t>(offsetof(sockaddr_un, sun_path) + std::strlen(addr.sun_path));

  if (::bind(listenFd_, reinterpret_cast<sockaddr*>(&addr), addrLen) < 0) {
    MLOG_ERROR("bind({}) failed, errno={}", socketPath_, errno);
    cleanup();
    return ;
  }

  if (::listen(listenFd_, 16) < 0) {
    MLOG_ERROR("listen({}) failed, errno={}", socketPath_, errno);
    cleanup();
    return ;
  }

  MLOG_INFO("Unix domain socket server listening on {}", socketPath_);

  start();
}

TraceServer::~TraceServer() { stop(); }

void TraceServer::start() {
  if (running_.exchange(true))
    return;  // already running
  worker_ = std::thread(&TraceServer::serveLoop, this);
}

void TraceServer::stop() {
  if (!running_.exchange(false)) {
    cleanup();
    return;
  }

  if (listenFd_ >= 0) {
    ::shutdown(listenFd_, SHUT_RDWR);
  }
  if (worker_.joinable()) {
    worker_.join();
  }
  cleanup();
}

void TraceServer::serveLoop() {
  for (;;) {
    if (!running_.load()) {
      break;
    }
    int clientFd = ::accept(listenFd_, nullptr, nullptr);
    if (clientFd < 0) {
      if (errno == EINTR) {
        continue;
      }
      if (!running_.load()) {
        break;  // likely shutdown
      }
      MLOG_ERROR("accept() failed on {}, errno={}", socketPath_, errno);
      break;
    }

    MLOG_INFO("Client connected: {}", socketPath_);

    // Read 4-byte length header
    uint32_t len_net = 0;
    char *p = reinterpret_cast<char *>(&len_net);
    size_t remaining = sizeof(len_net);
    bool ok = true;
    while (remaining > 0) {
      ssize_t n = ::read(clientFd, p, remaining);
      if (n > 0) {
        remaining -= static_cast<size_t>(n);
        p += n;
      } else if (n == 0) {
        MLOG_WARN("Client closed before sending length on {}", socketPath_);
        ok = false;
        break;
      } else {
        if (errno == EINTR) {
          continue;
        }
        MLOG_ERROR("read(length) failed on {}, errno={}", socketPath_, errno);
        ok = false;
        break;
      }
    }

    if (!ok) {
      ::close(clientFd);
      continue;
    }

    uint32_t len = ntohl(len_net);
    if (len < sizeof(uint16_t)) {
      MLOG_WARN("Received invalid frame length {} on {}", len, socketPath_);
      ::close(clientFd);
      continue;
    }

    // Read 2-byte type field
    uint16_t type_net = 0;
    p = reinterpret_cast<char *>(&type_net);
    remaining = sizeof(type_net);
    while (remaining > 0) {
      ssize_t n = ::read(clientFd, p, remaining);
      if (n > 0) {
        remaining -= static_cast<size_t>(n);
        p += n;
      } else if (n == 0) {
        MLOG_WARN("Client closed before sending type on {}", socketPath_);
        ok = false;
        break;
      } else {
        if (errno == EINTR) {
          continue;
        }
        MLOG_ERROR("read(type) failed on {}, errno={}", socketPath_, errno);
        ok = false;
        break;
      }
    }

    if (!ok) {
      ::close(clientFd);
      continue;
    }

    uint16_t type_raw = ntohs(type_net);
    auto msg_type = static_cast<AdminMsgType>(type_raw);

    // Remaining bytes are payload
    uint32_t payload_len = len - static_cast<uint32_t>(sizeof(type_net));
    std::string payload(payload_len, '\0');
    p = payload.data();
    remaining = payload_len;
    while (remaining > 0) {
      ssize_t n = ::read(clientFd, p, remaining);
      if (n > 0) {
        remaining -= static_cast<size_t>(n);
        p += n;
      } else if (n == 0) {
        MLOG_WARN("Client closed before sending full payload on {}", socketPath_);
        ok = false;
        break;
      } else {
        if (errno == EINTR) {
          continue;
        }
        MLOG_ERROR("read(payload) failed on {}, errno={}", socketPath_, errno);
        ok = false;
        break;
      }
    }

    if (!ok) {
      ::close(clientFd);
      continue;
    }
    
    // helper to send a protobuf response over UDS with our framed protocol
    auto send_response = [&](AdminMsgType type, const google::protobuf::Message &resp) {
      std::string resp_buf;
      if (!resp.SerializeToString(&resp_buf)) {
        return;
      }
      uint32_t resp_len = static_cast<uint32_t>(sizeof(uint16_t) + resp_buf.size());\
      uint32_t resp_len_net = htonl(resp_len);
      uint16_t resp_type_net = htons(static_cast<uint16_t>(type));

      struct iovec iov[3];
      iov[0].iov_base = &resp_len_net;
      iov[0].iov_len  = sizeof(resp_len_net);
      iov[1].iov_base = &resp_type_net;
      iov[1].iov_len  = sizeof(resp_type_net);
      iov[2].iov_base = const_cast<char *>(resp_buf.data());
      iov[2].iov_len  = resp_buf.size();

      ssize_t n = ::writev(clientFd, iov, 3);
      if (n < 0) {
        MLOG_WARN("Failed to send UDS admin response on {}, errno={}", socketPath_, errno);
      }
    };

    // Dispatch by msg_type, similar to common_rpc_handlers
    switch (msg_type) {
      case AdminMsgType::TRACE_TOGGLE: {
        proto::common::TraceToggleRequestPB req;
        proto::common::TraceToggleResponsePB resp;
        if (!req.ParseFromArray(payload.data(), static_cast<int>(payload.size()))) {
          MLOG_WARN("Failed to parse TraceToggleRequestPB from client on {}", socketPath_);
        } else {
          bool enable = req.enable_trace();
          MLOG_INFO("Trace toggle request via protobuf: {}", enable ? "ON" : "OFF");
#ifdef SIMM_ENABLE_TRACE
          simm::trace::TraceManager::SetEnabled(enable);
#endif
          resp.set_ret_code(CommonErr::OK);
        }
        send_response(AdminMsgType::TRACE_TOGGLE, resp);
        break;
      }
      case AdminMsgType::GFLAG_LIST: {
        proto::common::ListAllGFlagsRequestPB req;
        proto::common::ListAllGFlagsResponsePB resp;
        if (!req.ParseFromArray(payload.data(), static_cast<int>(payload.size()))) {
          MLOG_WARN("Failed to parse ListAllGFlagsRequestPB from client on {}", socketPath_);
        } else {
          // Mirror ListGFlagsHandler::Work from common_rpc_handlers
          std::vector<gflags::CommandLineFlagInfo> all_flags;
          gflags::GetAllFlags(&all_flags);
          for (const auto &f : all_flags) {
            auto *resp_flag = resp.add_flags();
            resp_flag->set_flag_name(f.name);
            resp_flag->set_flag_value(f.current_value);
            resp_flag->set_flag_default_value(f.default_value);
            resp_flag->set_flag_type(f.type);
            resp_flag->set_flag_description(f.description);
          }
          resp.set_ret_code(CommonErr::OK);
        }
        send_response(AdminMsgType::GFLAG_LIST, resp);
        break;
      }
      case AdminMsgType::GFLAG_GET: {
        proto::common::GetGFlagValueRequestPB req;
        proto::common::GetGFlagValueResponsePB resp;
        if (!req.ParseFromArray(payload.data(), static_cast<int>(payload.size()))) {
          MLOG_WARN("Failed to parse GetGFlagValueRequestPB from client on {}", socketPath_);
        } else {
          gflags::CommandLineFlagInfo info;
          if (!gflags::GetCommandLineFlagInfo(req.flag_name().c_str(), &info)) {
            resp.set_ret_code(CommonErr::GFlagNotFound);
          } else {
            resp.set_ret_code(CommonErr::OK);
            auto *resp_gflag_info = resp.mutable_flag_info();
            resp_gflag_info->set_flag_name(info.name);
            resp_gflag_info->set_flag_value(info.current_value);
            resp_gflag_info->set_flag_default_value(info.default_value);
            resp_gflag_info->set_flag_type(info.type);
            resp_gflag_info->set_flag_description(info.description);
          }
        }
        send_response(AdminMsgType::GFLAG_GET, resp);
        break;
      }
      case AdminMsgType::GFLAG_SET: {
        proto::common::SetGFlagValueRequestPB req;
        proto::common::SetGFlagValueResponsePB resp;
        if (!req.ParseFromArray(payload.data(), static_cast<int>(payload.size()))) {
          MLOG_WARN("Failed to parse SetGFlagValueRequestPB from client on {}", socketPath_);
        } else {
          gflags::CommandLineFlagInfo info;
          if (!gflags::GetCommandLineFlagInfo(req.flag_name().c_str(), &info)) {
            resp.set_ret_code(CommonErr::GFlagNotFound);
          } else {
            resp.set_ret_code(CommonErr::OK);
            auto res = gflags::SetCommandLineOption(req.flag_name().c_str(), req.flag_value().c_str());
            if (res.empty()) {
              resp.set_ret_code(CommonErr::GFlagSetFailed);
            }
          }
        }
        send_response(AdminMsgType::GFLAG_SET, resp);
        break;
      }
      default: {
        MLOG_WARN("Unknown AdminMsgType {} on {}", type_raw, socketPath_);
        break;
      }
    }

    ::close(clientFd);
  }
}

void TraceServer::cleanup() {
  if (listenFd_ >= 0) {
    ::close(listenFd_);
    listenFd_ = -1;
  }
  if (!socketPath_.empty()) {
    ::unlink(socketPath_.c_str());
    MLOG_INFO("Unlinked unix socket: {}", socketPath_);
  }
}

}  // namespace trace
}  // namespace simm