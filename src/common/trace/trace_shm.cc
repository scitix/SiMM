#include "trace_shm.h"
#include "trace.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <chrono>
#include <cstring>
#include <fstream>
#include <sstream>

#include "common/logging/logging.h"

DECLARE_LOG_MODULE("trace");

extern "C" {
extern char *program_invocation_short_name;
}

namespace simm {
namespace trace {

std::string ReadProcName() {
  std::string proc_name;
  std::ifstream comm_file("/proc/self/comm");
  if (comm_file.is_open()) {
    std::getline(comm_file, proc_name);
    comm_file.close();
  }
  if (proc_name.empty()) {
    if (program_invocation_short_name) {
      proc_name = program_invocation_short_name;
    } else {
      proc_name = "unknown";
    }
  }
  return proc_name;
}

static std::string GetTraceShmName() {
  std::string proc_name = ReadProcName();
  pid_t pid = getpid();

  std::ostringstream oss;
  oss << TRACE_SHM_PREFIX << proc_name << "." << pid;
  return oss.str();
}

static std::string GetTraceShmFullPath() {
  std::string proc_name = ReadProcName();
  pid_t pid = getpid();

  std::ostringstream oss;
  oss << TRACE_SHM_ROOT_DIR << TRACE_SHM_PREFIX << proc_name << "." << pid;
  return oss.str();
}

TraceSharedMemory *TraceSharedMemory::Create(size_t size_mb) {
  auto *shm = new TraceSharedMemory();
  std::string shm_name = GetTraceShmName();
  shm->shm_path_ = GetTraceShmFullPath();
  shm->size_ = size_mb * 1024 * 1024;
  shm->read_only_ = false;

  mode_t old_umask = umask(0);

  shm->fd_ =
      shm_open(shm_name.c_str(), O_CREAT | O_RDWR | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);

  umask(old_umask);

  if (shm->fd_ < 0) {
    MLOG_ERROR("Failed to create shared memory: {}, errno={}", shm_name.c_str(), errno);
    delete shm;
    return nullptr;
  }

  if (ftruncate(shm->fd_, shm->size_) != 0) {
    MLOG_ERROR("Failed to truncate shared memory: {}, errno={}", shm_name.c_str(), errno);
    close(shm->fd_);
    shm_unlink(shm_name.c_str());
    delete shm;
    return nullptr;
  }

  shm->addr_ = mmap(nullptr, shm->size_, PROT_READ | PROT_WRITE, MAP_SHARED, shm->fd_, 0);

  if (shm->addr_ == MAP_FAILED) {
    MLOG_ERROR("Failed to mmap shared memory: {}, errno={}", shm_name.c_str(), errno);
    close(shm->fd_);
    shm_unlink(shm_name.c_str());
    delete shm;
    return nullptr;
  }

  close(shm->fd_);
  shm->fd_ = -1;

  shm->header_ = static_cast<TraceSharedMemoryHeader *>(shm->addr_);
  shm->trace_data_ =
      reinterpret_cast<RequestTraceShm *>(static_cast<char *>(shm->addr_) + sizeof(TraceSharedMemoryHeader));

  shm->header_->magic = TRACE_SHM_MAGIC;
  shm->header_->version = TRACE_SHM_VERSION;
  shm->header_->header_size = sizeof(TraceSharedMemoryHeader);
  shm->header_->total_size = shm->size_;
  shm->header_->trace_size = sizeof(RequestTraceShm);
  shm->header_->max_points_per_trace = MAX_TRACE_POINTS_PER_REQUEST;
  shm->header_->capacity = (shm->size_ - sizeof(TraceSharedMemoryHeader)) / sizeof(RequestTraceShm);
  shm->header_->write_idx.store(0, std::memory_order_relaxed);
  shm->header_->wrap_count.store(0, std::memory_order_relaxed);
  shm->header_->total_traces.store(0, std::memory_order_relaxed);
  shm->header_->dropped_traces.store(0, std::memory_order_relaxed);
  shm->header_->pid = getpid();

  shm->header_->create_time =
      std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();

  std::string proc_name = ReadProcName();
  strncpy(shm->header_->proc_name, proc_name.c_str(), 63);
  shm->header_->proc_name[63] = '\0';

  MLOG_INFO(
      "Created trace shared memory: {}, size={} MB, capacity={}", shm->shm_path_, size_mb, shm->header_->capacity);

  return shm;
}

TraceSharedMemory *TraceSharedMemory::Open(const std::string &shm_path) {
  auto *shm = new TraceSharedMemory();
  shm->shm_path_ = shm_path;
  shm->read_only_ = true;

  std::string shm_name = shm_path;
  size_t pos = shm_path.find_last_of('/');
  if (pos != std::string::npos && pos + 1 < shm_path.length()) {
    shm_name = shm_path.substr(pos + 1);
  }

  shm->fd_ = shm_open(shm_name.c_str(), O_RDONLY, 0);

  if (shm->fd_ < 0) {
    MLOG_ERROR("Failed to open shared memory: {}, errno={}", shm_path, errno);
    delete shm;
    return nullptr;
  }

  struct stat st;
  if (fstat(shm->fd_, &st) != 0) {
    MLOG_ERROR("Failed to stat shared memory: {}, errno={}", shm_path, errno);
    close(shm->fd_);
    delete shm;
    return nullptr;
  }
  shm->size_ = st.st_size;

  shm->addr_ = mmap(nullptr, shm->size_, PROT_READ, MAP_SHARED, shm->fd_, 0);

  if (shm->addr_ == MAP_FAILED) {
    MLOG_ERROR("Failed to mmap shared memory: {}, errno={}", shm_path, errno);
    close(shm->fd_);
    delete shm;
    return nullptr;
  }

  close(shm->fd_);
  shm->fd_ = -1;

  shm->header_ = static_cast<TraceSharedMemoryHeader *>(shm->addr_);

  if (shm->header_->magic != TRACE_SHM_MAGIC) {
    MLOG_ERROR("Invalid magic number: 0x{}, expected 0x{}", shm->header_->magic, TRACE_SHM_MAGIC);
    munmap(shm->addr_, shm->size_);
    delete shm;
    return nullptr;
  }

  if (shm->header_->version != TRACE_SHM_VERSION) {
    MLOG_ERROR("Unsupported version: {}, expected {}", shm->header_->version, TRACE_SHM_VERSION);
    munmap(shm->addr_, shm->size_);
    delete shm;
    return nullptr;
  }

  shm->trace_data_ =
      reinterpret_cast<RequestTraceShm *>(static_cast<char *>(shm->addr_) + sizeof(TraceSharedMemoryHeader));

  MLOG_INFO("Opened trace shared memory: {}, size={}, capacity={}", shm_path, shm->size_, shm->header_->capacity);

  return shm;
}

RequestTraceShm *TraceSharedMemory::AllocTrace(uint64_t msg_tag_id) {
  if (!header_) {
    return nullptr;
  }

  uint64_t idx = header_->write_idx.fetch_add(1, std::memory_order_relaxed);
  uint64_t slot = idx % header_->capacity;

  if (idx >= header_->capacity) {
    uint64_t current_wrap = idx / header_->capacity;
    uint64_t prev_wrap = header_->wrap_count.load(std::memory_order_relaxed);
    if (current_wrap > prev_wrap) {
      header_->wrap_count.store(current_wrap, std::memory_order_relaxed);
    }
  }

  RequestTraceShm *trace = &trace_data_[slot];

  trace->msg_tag_id = 0;
  trace->flags = 0;
  trace->reserved[0] = 0;
  trace->reserved[1] = 0;
  
  for (size_t i = 0; i < MAX_TRACE_POINTS_PER_REQUEST; ++i) {
    trace->points[i].type = 0;
    trace->points[i].timestamp = 0;
  }

  std::atomic_thread_fence(std::memory_order_release);

  trace->msg_tag_id = msg_tag_id;
  std::atomic_thread_fence(std::memory_order_release);

  trace->point_count.store(0, std::memory_order_release);

  std::atomic_thread_fence(std::memory_order_release);

  header_->total_traces.fetch_add(1, std::memory_order_relaxed);

  return trace;
}

void TraceSharedMemory::AddTracePoint(RequestTraceShm *trace, TracePointType type, uint64_t timestamp) {
  if (!trace) {
    return;
  }

  uint32_t idx = trace->point_count.fetch_add(1, std::memory_order_acq_rel);

  if (idx >= header_->max_points_per_trace) {
    header_->dropped_traces.fetch_add(1, std::memory_order_relaxed);
    return;
  }

  trace->points[idx].type = static_cast<uint32_t>(type);
  trace->points[idx].timestamp = timestamp;

  std::atomic_thread_fence(std::memory_order_release);
}

std::vector<RequestTraceShm *> TraceSharedMemory::ReadAllTraces() {
  std::vector<RequestTraceShm *> result;

  if (!header_ || !trace_data_) {
    return result;
  }

  uint64_t write_idx = header_->write_idx.load(std::memory_order_acquire);
  uint64_t wrap_count = header_->wrap_count.load(std::memory_order_acquire);

  uint64_t start_idx = 0;
  uint64_t end_idx = write_idx;

  if (wrap_count > 0) {
    if (write_idx >= header_->capacity) {
      start_idx = write_idx - header_->capacity;
    } else {
      start_idx = 0;
    }
    end_idx = write_idx;
  }

  if (write_idx == 0) {
    return result;
  }

  std::atomic_thread_fence(std::memory_order_acquire);

  uint64_t snapshot_write_idx = write_idx;

  if (wrap_count > 0 && snapshot_write_idx >= header_->capacity) {
    start_idx = snapshot_write_idx - header_->capacity;
  } else {
    start_idx = 0;
  }
  end_idx = snapshot_write_idx;

  for (uint64_t i = start_idx; i < end_idx; ++i) {
    uint64_t slot = i % header_->capacity;
    RequestTraceShm *trace = &trace_data_[slot];

    uint32_t point_count = trace->point_count.load(std::memory_order_acquire);

    if (point_count > 0 && point_count <= header_->max_points_per_trace) {
      result.push_back(trace);
    }
  }

  return result;
}

void TraceSharedMemory::Close(bool unlink) {
  if (addr_) {
    munmap(addr_, size_);
    addr_ = nullptr;
  }

  if (fd_ >= 0) {
    close(fd_);
    fd_ = -1;
  }

  if (unlink && !shm_path_.empty()) {
    std::string shm_name = shm_path_;
    size_t pos = shm_path_.find_last_of('/');
    if (pos != std::string::npos && pos + 1 < shm_path_.length()) {
      shm_name = shm_path_.substr(pos + 1);
    }
    shm_unlink(shm_name.c_str());
    MLOG_INFO("Unlinked shared memory: {}", shm_name.c_str());
  }

  header_ = nullptr;
  trace_data_ = nullptr;
}

}  // namespace trace
}  // namespace simm
