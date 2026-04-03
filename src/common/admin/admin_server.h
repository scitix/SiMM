#pragma once

#include <atomic>
#include <functional>
#include <string>
#include <thread>
#include <unordered_map>

#include "common/admin/admin_msg_types.h"

namespace simm {
namespace common {

// Generic UDS-based admin server for CM and DS processes.
//
// Listens on /run/simm/simm_<role>.<pid>.sock and dispatches incoming
// admin messages to registered handlers by AdminMsgType.
//
// Built-in handlers for GFLAG_LIST/GET/SET and TRACE_TOGGLE are always
// available. Additional handlers (e.g. DS_STATUS) can be registered
// by the owning component via RegisterHandler().
//
// Wire protocol is identical to TraceServer:
//   Request:  [uint32_t frame_len][uint16_t type][payload bytes]
//   Response: [uint32_t frame_len][uint16_t type][payload bytes]
class AdminServer {
 public:
  // role: "cm" or "ds". Socket path = /run/simm/simm_<role>.<pid>.sock
  explicit AdminServer(std::string role);
  ~AdminServer();

  AdminServer(const AdminServer&) = delete;
  AdminServer& operator=(const AdminServer&) = delete;

  // Handler callback: receives raw request payload, returns serialized response.
  using Handler = std::function<std::string(const std::string& request_payload)>;

  // Register a custom handler for a given message type.
  // Must be called before Start(). Not thread-safe with ServeLoop.
  void RegisterHandler(AdminMsgType msg_type, Handler handler);

  void Start();
  void Stop();

  const std::string& SocketPath() const { return socket_path_; }

 private:
  void ServeLoop();
  void HandleClient(int client_fd);
  void Cleanup();

  // Built-in handlers (registered automatically in constructor)
  std::string HandleGFlagList(const std::string& payload);
  std::string HandleGFlagGet(const std::string& payload);
  std::string HandleGFlagSet(const std::string& payload);
  std::string HandleTraceToggle(const std::string& payload);

  // UDS I/O helpers
  static bool ReadExact(int fd, void* buf, size_t len);
  static bool WriteAll(int fd, const void* buf, size_t len);
  void SendResponse(int client_fd, AdminMsgType type, const std::string& serialized_resp);

  std::string role_;
  std::string socket_path_;
  int listen_fd_{-1};
  std::atomic<bool> running_{false};
  std::thread worker_;
  std::unordered_map<uint16_t, Handler> handlers_;
};

}  // namespace common
}  // namespace simm
