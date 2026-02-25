#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "common/base/memory.h"
#include "data_server/ds_common.h"
#include "transport/mempool.h"

namespace simm {
namespace ds {

using common::MemBlock;

class MemoryAllocator {
 public:
  virtual ~MemoryAllocator() = default;

  virtual int allocate(MemBlock *mb) = 0;

  virtual int free(MemBlock *mb) = 0;
};

class ShmAllocator : public MemoryAllocator {
 public:
  ShmAllocator(sicl::transport::Mempool *registry = nullptr);
  ~ShmAllocator() override;

  int allocate(MemBlock *mb) override;
  int free(MemBlock *mb) override;

  static ShmAllocator &Instance(sicl::transport::Mempool *registry = nullptr) {
    static ShmAllocator allocator(registry);
    return allocator;
  }

 private:
  std::mutex mutex_;
  std::unordered_map<char *, MemBlock *> shm_map_;
  sicl::transport::Mempool *registry_;
};

}  // namespace ds
}  // namespace simm
