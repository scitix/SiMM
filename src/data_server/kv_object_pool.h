#pragma once

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <vector>

#include "blockingconcurrentqueue.h"
#include "folly/memory/Arena.h"

namespace simm {
namespace ds {
struct KVEntry;
struct KVMeta;
/**
 * @brief Thread-safe object pool for key & value pointer in hash table
 *
 * This pool manages reusable kv objects to avoid frequent heap
 * allocations. Objects are automatically returned to the pool when their
 * smart pointer is destroyed.
 */
class KVObjectPool {
 public:
  ~KVObjectPool();

  KVMeta* AcquireKey();
  void ReleaseKey(KVMeta* meta);

  KVEntry* AcquireValue();
  void ReleaseValue(KVEntry *entry);

  size_t CapacityKey() const;
  size_t InflightKey() const;

  size_t CapacityValue() const;
  size_t InflightValue() const;

  static KVObjectPool &Instance() {
    static KVObjectPool pool;
    return pool;
  }

  struct KVMetaDeleter {
    void operator()(KVMeta* key) const noexcept {
      KVObjectPool::Instance().ReleaseKey(key);
    }
  };

 private:
  explicit KVObjectPool();
  static folly::SysArena& arena();
  std::atomic<size_t> key_inflight_{0};
  std::atomic<size_t> val_inflight_{0};
  std::atomic<size_t> key_capacity_{0};
  std::atomic<size_t> val_capacity_{0};
  moodycamel::BlockingConcurrentQueue<KVMeta *> key_queue_;
  moodycamel::BlockingConcurrentQueue<KVEntry *> val_queue_;
};

}  // namespace ds
}  // namespace simm
