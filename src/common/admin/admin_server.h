#pragma once

#include <atomic>
#include <functional>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include "common/admin/admin_msg_types.h"

namespace simm {
namespace common {

// UDS-based admin server for SiMM components.
//
// The constructor creates the Unix domain socket, binds, listens, and spawns
// the serve thread. The destructor shuts down the thread, closes the socket,
// and unlinks the socket file. No explicit start()/stop() — lifecycle is
// tied to object lifetime (RAII).
//
// Socket path: <basePath>.<pid>.sock
//   e.g. /run/simm/admin/simm_ds.12345.sock
//
// Built-in handlers for GFLAG_LIST/GET/SET and TRACE_TOGGLE are always
// registered. Additional handlers (e.g. DS_STATUS) can be registered via
// registerHandler() by the owning service after construction.
//
// Wire protocol: [uint32_t frame_len][uint16_t type][payload]
class AdminServer {
 public:
  // basePath: e.g. "/run/simm/admin/simm_cm" or "/run/simm/admin/simm_ds".
  // Socket path = <basePath>.<pid>.sock
  explicit AdminServer(std::string basePath);
  ~AdminServer();

  AdminServer(const AdminServer&) = delete;
  AdminServer& operator=(const AdminServer&) = delete;

  // Handler callback: receives raw request payload, returns serialized response.
  using Handler = std::function<std::string(const std::string& request_payload)>;

  // Register a custom handler for a given message type.
  // Safe to call after construction and before the first client connects.
  void registerHandler(AdminMsgType msg_type, Handler handler);

  const std::string& socketPath() const { return socketPath_; }

  bool isRunning() const { return running_.load(); }

 private:
  void serveLoop();
  void handleClient(int client_fd);
  void shutdown();

  // Built-in handlers
  std::string handleGFlagList(const std::string& payload);
  std::string handleGFlagGet(const std::string& payload);
  std::string handleGFlagSet(const std::string& payload);
  std::string handleTraceToggle(const std::string& payload);

  // UDS I/O helpers
  static bool readExact(int fd, void* buf, size_t len);
  void sendResponse(int client_fd, AdminMsgType type, const std::string& serialized_resp);

  std::string basePath_;
  std::string socketPath_;
  int listenFd_{-1};
  int shutdownPipe_[2]{-1, -1};   // self-pipe for clean shutdown
  std::atomic<bool> running_{false};
  std::thread worker_;
  mutable std::shared_mutex handlersMutex_;
  std::unordered_map<uint16_t, Handler> handlers_;
};

}  // namespace common
}  // namespace simm
