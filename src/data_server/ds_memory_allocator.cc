#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <mutex>
#include <stdexcept>
#include <unordered_map>

#include "transport/types.h"
#include "common/logging/logging.h"
#include "data_server/ds_common.h"
#include "data_server/ds_memory_allocator.h"

DECLARE_LOG_MODULE("data_server");

namespace simm {
namespace ds {

ShmAllocator::ShmAllocator(sicl::transport::Mempool *registry) : registry_(registry) {}

ShmAllocator::~ShmAllocator() {
  for (auto &[buf, record] : shm_map_) {
    munmap(buf, record->len);
    shm_unlink(record->shmname.c_str());
  }
}

int ShmAllocator::allocate(MemBlock *mb) {
  std::string &shm_name = mb->shmname;
  size_t size = mb->len;
  // Create or open shared memory
  int fd = shm_open(shm_name.c_str(), O_CREAT | O_RDWR, 0666);
  if (fd == -1) {
    MLOG_ERROR("Failed to open shared memory:{}", shm_name);
    return -1;
  }

  // Set size
  if (ftruncate(fd, size) == -1) {
    close(fd);
    MLOG_ERROR("Failed to ftruncate shared memory size:{}, filename:{}", size, shm_name);
    return -1;
  }

  // Map memory
  void *addr = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (addr == MAP_FAILED) {
    close(fd);
    MLOG_ERROR("Failed to mmap shared memory, filename:{}", shm_name);
    return -1;
  }

  close(fd);
  sicl::transport::MemDesc *descr = nullptr;
  if (registry_) {
    auto res = registry_->regMr(descr, addr, size);
    if (res) {
      MLOG_ERROR("Failed to register memory region, addr:{}, size:{}", static_cast<void*>(addr), size);
      munmap(addr, size);
      return -1;
    }
  }

  auto record = new MemBlock(size, shm_name.c_str());
  record->buf = reinterpret_cast<char *>(addr);
  record->descr = descr;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto pair_ret = shm_map_.emplace(record->buf, record);
    if (!pair_ret.second) {
      MLOG_ERROR("SHM map insert failed: {}", static_cast<void*>(record->buf));
      return -1;
    }
  }
  mb->buf = record->buf;
  mb->descr = record->descr;
  return 0;
}

int ShmAllocator::free(MemBlock *mb) {
  MemBlock *record = nullptr;
  {
    auto iter = shm_map_.find(mb->buf);
    if (iter == shm_map_.end()) {
      MLOG_ERROR("SHM map find failed: {}", static_cast<void*>(mb->buf));
      return -1;
    }
    record = iter->second;
    shm_map_.erase(iter);
  }

  sicl::transport::MemDesc *descr = nullptr;
  if (registry_) {
    descr = static_cast<sicl::transport::MemDesc *>(record->descr);
    if (descr) {
      auto res = registry_->deregMr(descr);
      if (res) {
        MLOG_ERROR("Failed to deregister memory region, res:{}", std::to_string(res));
      }
    }
  }

  munmap(record->buf, record->len);
  std::string &shm_name = record->shmname;
  int ret = shm_unlink(shm_name.c_str());
  if (ret) {
    MLOG_ERROR("Failed to shm unlink:{}, ret: {}", shm_name.c_str(), strerror(errno));
  }
  delete record;
  mb->buf = nullptr;
  mb->len = 0;
  mb->descr = nullptr;
  return ret;
}

}  // namespace ds
}  // namespace simm
