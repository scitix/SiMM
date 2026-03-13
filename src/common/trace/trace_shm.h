#pragma once

#include <sys/types.h>
#include <unistd.h>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace simm {
namespace trace {
enum class TracePointType : uint16_t;
}
}  // namespace simm

namespace simm {
namespace trace {

static constexpr uint64_t TRACE_SHM_MAGIC = 0x5349434C54524143ULL;  // "SIMMTRAC"
static constexpr uint32_t TRACE_SHM_VERSION = 1;
static constexpr const char *TRACE_SHM_ROOT_DIR = "/dev/shm/";
static constexpr const char *TRACE_SHM_PREFIX = ".simm_trace.";
static constexpr uint32_t MAX_TRACE_POINTS_PER_REQUEST = 64;
static constexpr uint32_t DEFAULT_SHM_SIZE_MB = 256;

struct TracePointShm {
  uint32_t type;  // TracePointType
  uint64_t timestamp;
} __attribute__((packed));

static_assert(sizeof(TracePointShm) == 12, "TracePointShm must be 12 bytes");

// Aligned to cache line (64 bytes) for better performance
struct RequestTraceShm {
  uint64_t msg_tag_id;
  std::atomic<uint32_t> point_count;
  uint32_t flags;
  uint64_t reserved[2];

  TracePointShm points[MAX_TRACE_POINTS_PER_REQUEST];

  RequestTraceShm() : msg_tag_id(0), point_count(0), flags(0), reserved{0, 0} {
    for (size_t i = 0; i < MAX_TRACE_POINTS_PER_REQUEST; ++i) {
      points[i].type = 0;
      points[i].timestamp = 0;
    }
  }
} __attribute__((aligned(64)));  // Cache line alignment

static_assert(sizeof(RequestTraceShm) % 64 == 0, "RequestTraceShm must be cache-line aligned");
static_assert(offsetof(RequestTraceShm, point_count) == 8, "point_count must be properly aligned");

// Header for the shared memory segment
struct TraceSharedMemoryHeader {
  uint64_t magic;        // Magic number: 0x5349434C54524143
  uint32_t version;      // Version number
  uint32_t header_size;  // Header size
  uint64_t total_size;   // Total size of shared memory (bytes)
  uint64_t capacity;     // Capacity (number of RequestTraceShm entries)

  // Atomic fields (for lock-free concurrency)
  std::atomic<uint64_t> write_idx;       // Current write position (incremental allocation)
  std::atomic<uint64_t> wrap_count;      // Number of wraparounds in circular buffer
  std::atomic<uint64_t> total_traces;    // Total number of traces written
  std::atomic<uint64_t> dropped_traces;  // Number of traces dropped due to concurrency issues

  // Metadata
  uint32_t trace_size;            // Size of a single RequestTraceShm
  uint32_t max_points_per_trace;  // Maximum number of points per trace
  pid_t pid;                      // Process PID
  uint64_t create_time;           // Creation time (nanoseconds)
  char proc_name[64];             // Process name
  char reserved[64];              // Reserved fields

  TraceSharedMemoryHeader()
      : magic(0),
        version(0),
        header_size(0),
        total_size(0),
        capacity(0),
        trace_size(0),
        max_points_per_trace(0),
        pid(0),
        create_time(0) {
    memset(proc_name, 0, sizeof(proc_name));
    memset(reserved, 0, sizeof(reserved));
  }
} __attribute__((aligned(256)));

static_assert(sizeof(TraceSharedMemoryHeader) % 256 == 0, "TraceSharedMemoryHeader must be 256-byte aligned");

class TraceSharedMemory {
 public:
  static TraceSharedMemory *Create(size_t size_mb = DEFAULT_SHM_SIZE_MB);

  static TraceSharedMemory *Open(const std::string &shm_path);

  RequestTraceShm *AllocTrace(uint64_t msg_tag_id);

  void AddTracePoint(RequestTraceShm *trace, TracePointType type, uint64_t timestamp);

  std::vector<RequestTraceShm *> ReadAllTraces();

  const TraceSharedMemoryHeader *GetHeader() const { return header_; }

  const std::string &GetShmPath() const { return shm_path_; }

  RequestTraceShm *GetTraceAtSlot(uint64_t slot) const {
    if (!trace_data_ || slot >= header_->capacity) {
      return nullptr;
    }
    return &trace_data_[slot];
  }

  void Close(bool unlink = false);

  ~TraceSharedMemory() { Close(false); }

 private:
  TraceSharedMemory() = default;

  int fd_ = -1;
  void *addr_ = nullptr;
  size_t size_ = 0;
  std::string shm_path_;
  TraceSharedMemoryHeader *header_ = nullptr;
  RequestTraceShm *trace_data_ = nullptr;
  bool read_only_ = false;
};

std::string ReadProcName();

}  // namespace trace
}  // namespace simm
