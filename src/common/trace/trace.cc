#include "trace.h"
#include <gflags/gflags_declare.h>
#ifdef SIMM_ENABLE_TRACE
#include "trace_shm.h"
#endif
#include <cstdlib>
#include <iostream>

#include <gflags/gflags.h>
#include "common/logging/logging.h"

DECLARE_int32(simm_trace_shm_size_mb);

namespace simm {
namespace trace {

#ifdef SIMM_ENABLE_TRACE

std::string GetTracePointTypeName(TracePointType type) {
  switch (type) {
    case TracePointType::CLIENT_PUT_START:
      return "CLIENT_PUT_START";
    case TracePointType::CLIENT_PUT_END:
      return "CLIENT_PUT_END";
    case TracePointType::CLIENT_PUT_MESSENGER_START:
      return "CLIENT_PUT_MESSENGER_START";
    case TracePointType::CLIENT_PUT_MESSENGER_END:
      return "CLIENT_PUT_MESSENGER_END";
    case TracePointType::CLIENT_CALLSYNC_BEFORE_RPC:
      return "CLIENT_CALLSYNC_BEFORE_RPC";
    case TracePointType::CLIENT_CALLSYNC_AFTER_RPC:
      return "CLIENT_CALLSYNC_AFTER_RPC";

    case TracePointType::DS_PUT_START:
      return "DS_PUT_START";
    case TracePointType::DS_PUT_END:
      return "DS_PUT_END";
    case TracePointType::DS_PUT_EMPLACE_START:
      return "DS_PUT_EMPLACE_START";
    case TracePointType::DS_PUT_WRITE_START:
      return "DS_PUT_WRITE_START";

    default:
      if (static_cast<uint16_t>(type) >= 1000) {
        return "CUSTOM_" + std::to_string(static_cast<uint16_t>(type));
      }
      return "UNKNOWN_" + std::to_string(static_cast<uint16_t>(type));
  }
}

std::string GetSegmentName(TracePointType from, TracePointType to) {
  return GetTracePointTypeName(from) + " -> " + GetTracePointTypeName(to);
}

// Implementation of TraceManager
TraceManager::TraceManager() : shm_(nullptr) {}

TraceManager::~TraceManager() {
  auto *shm = shm_.load(std::memory_order_acquire);
  if (shm) {
    shm->Close(false);
    delete shm;
    shm_.store(nullptr, std::memory_order_release);
  }
}

void TraceManager::InitSharedMemory() {
  std::call_once(init_once_, [this]() {
    std::function<void()> init_shm = [this]() {
      // Get size from gflag
      size_t size_mb = FLAGS_simm_trace_shm_size_mb;

      TraceSharedMemory *new_shm = TraceSharedMemory::Create(size_mb);
      if (!new_shm) {
        return;
      }

      shm_.store(new_shm, std::memory_order_release);
    };

    std::thread init_thread(init_shm);
    // Asynchronous initialization
    init_thread.detach();
    // init_thread.join();
  });
}

void TraceManager::SetEnabled(bool enabled_flag) {
  auto &instance = Instance();
  instance.should_trace_.store(enabled_flag, std::memory_order_release);
  if (enabled_flag) {
    instance.InitSharedMemory();
  }
}

RequestTraceShm *TraceManager::StartTrace(uint64_t msg_tag_id) {
  auto &instance = Instance();
  if (!instance.should_trace_.load(std::memory_order_acquire) || 
      !ShouldTrace()) {
    return nullptr;
  }

  auto shm = instance.shm_.load(std::memory_order_acquire);
  if (!shm) {
    return nullptr;
  }

  // Allocate trace from shared memory
  return shm->AllocTrace(msg_tag_id);
}

void TraceManager::AddPoint(RequestTraceShm *trace, TracePointType type) {
  if (!trace) {
    return;
  }

  auto &instance = Instance();

  auto shm = instance.shm_.load(std::memory_order_acquire);
  if (!shm) {
    return;
  }

  // Get current timestamp (nanoseconds)
  uint64_t timestamp = get_timestamp_ns();

  // Write directly to shared memory (accessing private member shm_ through instance)
  shm->AddTracePoint(trace, type, timestamp);
}

#endif  // SIMM_ENABLE_TRACE

}  // namespace trace
}  // namespace simm
