#pragma once

#include <chrono>
#include <memory>
#include <optional>
#include "common/trace/trace_handle.h"
#include "common/trace/trace_shm.h"

#include "rpc/rpc.h"

namespace simm {
namespace common {

// Lightweight context type that currently carries tracing information.
// Designed to be extended later to carry general per-request context between
// functions. It wraps a TraceHandle and exposes a small, stable API.
class SimmContext {
 public:
  explicit SimmContext() {}

  trace::RequestTraceShm *get_trace() {
    if (!trace_) {
      trace_ = std::make_shared<trace::TraceHandle>();
    }
    return trace_->get();
  }

  void set_rpc_ctx(const std::shared_ptr<sicl::rpc::RpcContext> rpc_ctx) {
    rpc_ctx_ = rpc_ctx;
  }

  std::shared_ptr<sicl::rpc::RpcContext> get_rpc_ctx() const {
    return rpc_ctx_;
  }

  std::chrono::time_point<std::chrono::steady_clock> GetReqStartTs() const {
    return req_start_ts_;
  }

 private:

  std::shared_ptr<sicl::rpc::RpcContext> rpc_ctx_{nullptr};
  std::shared_ptr<trace::TraceHandle> trace_{nullptr};
  std::chrono::time_point<std::chrono::steady_clock> req_start_ts_{std::chrono::steady_clock::now()};
};

}  // namespace common
}  // namespace simm
