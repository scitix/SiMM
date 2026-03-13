#pragma once

#include <atomic>
#include <string>
#include <thread>

namespace simm {
namespace trace {

// A simple Unix domain socket server that listens on
//   <basePath>.<pid>.sock
// and dispatches payloads received from clients to user code.
//
// The server runs its accept/read loop on a background thread after
// start() is called, and is stopped/joined automatically in the
// destructor.
class TraceServer {
 public:
  // Construct server with a base path prefix, e.g. "/tmp/simm_trace".
  // The actual socket path will be "<basePath>.<pid>.sock".
  explicit TraceServer(std::string basePath);

  TraceServer(const TraceServer&) = delete;
  TraceServer& operator=(const TraceServer&) = delete;

  ~TraceServer();

  // Start the background serve loop if not already running.
  void start();

  // Stop the background thread and close/unlink the socket.
  void stop();

  // Return the full socket path, e.g. "/tmp/simm_trace.<pid>.sock".
  const std::string& socketPath() const { return socketPath_; }

 private:
  void serveLoop();
  void cleanup();

  std::string basePath_;
  std::string socketPath_;
  int listenFd_{-1};
  std::atomic<bool> running_{false};
  std::thread worker_;
};

}  // namespace trace
}  // namespace simm
