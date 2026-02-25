#include <atomic>
#include "common/logging/logging.h"
#include "data_server/kv_cache_pool.h"
#include "data_server/kv_hash_table.h"
#include "data_server/kv_object_pool.h"

DECLARE_LOG_MODULE("data_server");

namespace simm {
namespace ds {

folly::SysArena& KVObjectPool::arena() {
  static thread_local folly::SysArena* arena =
      new folly::SysArena(/* params */);
  return *arena;
}

KVObjectPool::KVObjectPool() {}

KVObjectPool::~KVObjectPool() {
  key_inflight_.store(0, std::memory_order_relaxed);
  key_capacity_.store(0, std::memory_order_relaxed);
  val_inflight_.store(0, std::memory_order_relaxed);
  val_capacity_.store(0, std::memory_order_relaxed);
}

KVMeta* KVObjectPool::AcquireKey() {
  KVMeta *meta = nullptr;
  if (key_queue_.try_dequeue(meta)) {
    key_inflight_.fetch_add(1, std::memory_order_acq_rel);
    return meta;
  }
  void* p = arena().allocate(sizeof(KVMeta));
  new (p) KVMeta;
  meta = static_cast<KVMeta*>(p);
  MLOG_DEBUG("KVObjectPool arena allocate meta: {}", (void*)meta);
  key_capacity_.fetch_add(1, std::memory_order_release);
  key_inflight_.fetch_add(1, std::memory_order_acq_rel);
  return meta;
}

void KVObjectPool::ReleaseKey(KVMeta *meta) {
  // MLOG_DEBUG("KVObjectPool release meta: {:p});
  // Reset entry state before returning to pool
  meta->key_hash = 0;
  meta->key_len = 0;
  meta->key_ptr = nullptr;

  bool ret = key_queue_.enqueue(meta);
  if (!ret) {
    MLOG_ERROR("KVObjectPool enqueue meta {} failed, ret:{}",
              (void*)meta, ret);
  }
  key_inflight_.fetch_sub(1, std::memory_order_release);
}

KVEntry* KVObjectPool::AcquireValue() {
  KVEntry *entry = nullptr;
  if (val_queue_.try_dequeue(entry)) {
    val_inflight_.fetch_add(1, std::memory_order_acq_rel);
    return entry;
  }
  void* p = arena().allocate(sizeof(KVEntry));
  new (p) KVEntry();
  entry = static_cast<KVEntry*>(p);
  MLOG_DEBUG("KVObjectPool arena allocate entry: {}", (void*)entry);
  val_capacity_.fetch_add(1, std::memory_order_release);
  val_inflight_.fetch_add(1, std::memory_order_acq_rel);
  return entry;
}

void KVObjectPool::ReleaseValue(KVEntry *entry) {
#ifndef NDEBUG
  MLOG_DEBUG("KVObjectPool release entry: {:p}, previous snapshot: {}",
            (void*)entry, entry->ToString());
#endif
  // Reset entry state before returning to pool
  entry->slab_info = {};
  entry->status.store(0, std::memory_order_relaxed);
  entry->ref_cnt_lock.lock.store(0, std::memory_order_relaxed);

  bool ret = val_queue_.enqueue(entry);
  if (!ret) {
    MLOG_ERROR("KVObjectPool enqueue entry {} failed, ret:{}",
              (void*)entry, ret);
  }
  val_inflight_.fetch_sub(1, std::memory_order_release);
}

size_t KVObjectPool::CapacityKey() const {
  return key_capacity_.load(std::memory_order_acquire);
}

size_t KVObjectPool::InflightKey() const {
  return key_inflight_.load(std::memory_order_acquire);
}

size_t KVObjectPool::CapacityValue() const {
  return val_capacity_.load(std::memory_order_acquire);
}

size_t KVObjectPool::InflightValue() const {
  return val_inflight_.load(std::memory_order_acquire);
}

}  // namespace ds
}  // namespace simm
