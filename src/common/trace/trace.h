#pragma once

#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <mutex>
#include <string>

#include <gflags/gflags.h>

DECLARE_double(simm_trace_sample_rate);

namespace simm {
namespace trace {
class TraceSharedMemory;
struct RequestTraceShm;
}  // namespace trace
}  // namespace simm

namespace simm {
namespace trace {

enum class TracePointType : uint16_t {
  CLIENT_PUT_START = 300,
  CLIENT_PUT_END,
  CLIENT_PUT_MESSENGER_START,
  CLIENT_PUT_MESSENGER_END,
  CLIENT_CALLSYNC_BEFORE_RPC,
  CLIENT_CALLSYNC_AFTER_RPC,

  DS_PUT_START = 400,
  DS_PUT_END,
  DS_PUT_EMPLACE_START,
  DS_PUT_WRITE_START,

  CUSTOM_START = 1000
};

#ifdef SIMM_ENABLE_TRACE

static inline uint64_t get_timestamp_ns() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

// Single trace point record (16 bytes, cache-friendly)
struct TracePoint {
  uint64_t timestamp;  // 8 bytes: nanoseconds
  uint16_t type;       // 2 bytes: TracePointType
  uint16_t thread_id;  // 2 bytes: Thread ID hash
  uint32_t reserved;   // 4 bytes: Reserved for future use

  TracePoint() : timestamp(0), type(0), thread_id(0), reserved(0) {}

  TracePoint(TracePointType t, uint16_t tid = 0)
      : timestamp(get_timestamp_ns()), type(static_cast<uint16_t>(t)), thread_id(tid), reserved(0) {}
};

// Global Trace Manager
class TraceManager {
 public:
  static TraceManager &Instance() {
    static TraceManager instance;
    return instance;
  }

  // Start tracing a request
  // Returns RequestTraceShm*, which can be cast to void* and stored in ctx->SetTraceHandle()
  RequestTraceShm *StartTrace(uint64_t msg_tag_id);

  // Add trace point
  static void AddPoint(RequestTraceShm *trace, TracePointType type);

  static void SetEnabled(bool enabled);

 private:
  TraceManager();
  ~TraceManager();

  TraceManager(const TraceManager &) = delete;
  TraceManager &operator=(const TraceManager &) = delete;

  void InitSharedMemory();

  static double GetSampleRate() { return FLAGS_simm_trace_sample_rate; }

  // Judge whether to trace the current request based on sample rate
  static bool ShouldTrace() {
    static thread_local uint32_t counter = 0;
    counter++;

    double rate = GetSampleRate();
    if (rate >= 1.0)
      return true;
    if (rate <= 0.0)
      return false;

    // Simple sampling strategy: trace every N requests
    uint32_t n = static_cast<uint32_t>(1.0 / rate);
    // Use counter-1 to ensure the first request also has a chance to be sampled (when rate >= 1.0, it has already
    // returned true)
    return ((counter - 1) % n) == 0;
  }

  std::mutex init_mutex_;
  std::once_flag init_once_;
  std::atomic<TraceSharedMemory *> shm_;
  std::atomic<bool> should_trace_;
};

#endif  // SIMM_ENABLE_TRACE

// Convenience macros
#ifdef SIMM_ENABLE_TRACE
#define SIMM_TRACE_POINT(ctx, point)                   \
  do {                                                   \
    simm::trace::TraceManager::AddPoint((ctx).get_trace(), point); \
  } while (0)
#else
// If tracing is not enabled, the macro expands to a no-op, and the parameters are ignored
#define SIMM_TRACE_POINT(ctx, point) 
#endif

// These helpers are useful to analyzers and printers even when tracing is
// disabled at compile time. Always declare them so tools can link/compile.
std::string GetTracePointTypeName(TracePointType type);

std::string GetSegmentName(TracePointType from, TracePointType to);

}  // namespace trace
}  // namespace simm
