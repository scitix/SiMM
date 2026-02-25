#pragma once

#include <atomic>
#include <optional>
#include "common/trace/trace.h"

namespace simm {
namespace trace {

// RAII helper for a RequestTraceShm* allocated from the shared trace pool.
// When tracing is disabled (SIMM_ENABLE_TRACE not defined) this header
// provides a zero-cost no-op TraceHandle implementation so callers need not
// litter code with #ifdefs.

#ifdef SIMM_ENABLE_TRACE

// Generate a thread-safe req_id when callers do not provide one.
static inline uint64_t NextReqId() {
  static std::atomic<uint64_t> g_msg_id_counter{1};
  return g_msg_id_counter.fetch_add(1, std::memory_order_relaxed);
}

class TraceHandle {
 public:
  // Create a trace handle with an explicit req_id
  explicit TraceHandle(uint64_t req_id) : trace_(nullptr) {
    trace_ = TraceManager::Instance().StartTrace(req_id);
  }

  // Create a trace handle and generate a thread-safe req_id internally
  TraceHandle() : trace_(nullptr) {
    uint64_t id = NextReqId();
    trace_ = TraceManager::Instance().StartTrace(id);
  }

  ~TraceHandle() = default;

  // Movable, not copyable
  TraceHandle(TraceHandle &&o) noexcept : trace_(o.trace_) { o.trace_ = nullptr; }
  TraceHandle &operator=(TraceHandle &&o) noexcept {
    if (this != &o) {
      trace_ = o.trace_;
      o.trace_ = nullptr;
    }
    return *this;
  }

  TraceHandle(const TraceHandle &) = delete;
  TraceHandle &operator=(const TraceHandle &) = delete;

  bool valid() const { return trace_ != nullptr; }
  RequestTraceShm *get() const { return trace_; }

  // Surrender ownership: after release the destructor won't touch the trace
  void release() { trace_ = nullptr; }

 private:
  RequestTraceShm *trace_;
};

#else  // SIMM_ENABLE_TRACE not defined

// No-op implementation when tracing is disabled.
class TraceHandle {
 public:
  explicit TraceHandle(uint64_t /*req_id*/) {}
  TraceHandle() {}
  ~TraceHandle() = default;

  // Movable, not copyable
  TraceHandle(TraceHandle &&) noexcept = default;
  TraceHandle &operator=(TraceHandle &&) noexcept = default;

  TraceHandle(const TraceHandle &) = delete;
  TraceHandle &operator=(const TraceHandle &) = delete;

  RequestTraceShm *get() const { return nullptr; }
};

#endif  // SIMM_ENABLE_TRACE

}  // namespace trace
}  // namespace simm
