#pragma once

#include <atomic>
#include <functional>
#include <string>
#include <thread>
#include <unordered_map>

#include "common/admin/admin_msg_types.h"

namespace simm {
namespace common {

// UDS-based admin server for CM and DS processes.
//
// Modeled after Ceph's AdminSocket: the constructor creates the Unix domain
// socket, binds, listens, and spawns the serve thread. The destructor shuts
// down the thread, closes the socket, and unlinks the socket file. There is
// no explicit Start()/Stop() — lifecycle is tied to object lifetime.
//
// Socket path: /run/simm/simm_<name>.<pid>.sock
//   e.g. /run/simm/simm_ds-svc-001.12345.sock
//
// Built-in handlers for GFLAG_LIST/GET/SET and TRACE_TOGGLE are always
// registered. Additional handlers (e.g. DS_STATUS) can be registered via
// RegisterHandler() by the owning service after construction.
//
// Wire protocol: [uint32_t frame_len][uint16_t type][payload]
class AdminServer {
 public:
  // name: pod/service name (e.g. "ds-svc-001", "cm-svc-001").
  // Socket path = /run/simm/simm_<name>.<pid>.sock
  explicit AdminServer(const std::string& name);
  ~AdminServer();

  AdminServer(const AdminServer&) = delete;
  AdminServer& operator=(const AdminServer&) = delete;

  // Handler callback: receives raw request payload, returns serialized response.
  using Handler = std::function<std::string(const std::string& request_payload)>;

  // Register a custom handler for a given message type.
  // Safe to call after construction and before the first client connects.
  void RegisterHandler(AdminMsgType msg_type, Handler handler);

  const std::string& SocketPath() const { return socket_path_; }

 private:
  void ServeLoop();
  void HandleClient(int client_fd);
  void Shutdown();

  // Built-in handlers
  std::string HandleGFlagList(const std::string& payload);
  std::string HandleGFlagGet(const std::string& payload);
  std::string HandleGFlagSet(const std::string& payload);
  std::string HandleTraceToggle(const std::string& payload);

  // UDS I/O helpers
  static bool ReadExact(int fd, void* buf, size_t len);
  void SendResponse(int client_fd, AdminMsgType type, const std::string& serialized_resp);

  std::string name_;
  std::string socket_path_;
  int listen_fd_{-1};
  int shutdown_pipe_[2]{-1, -1};   // self-pipe for clean shutdown
  std::atomic<bool> running_{false};
  std::thread worker_;
  std::unordered_map<uint16_t, Handler> handlers_;
};

}  // namespace common
}  // namespace simm
