#pragma once

#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>

#include "data_server/ds_common.h"
#include "data_server/kv_cache_pool.h"
#include "data_server/kv_object_pool.h"
#include "folly/container/F14Map.h"
#include "folly/container/IntrusiveList.h"
#include "folly/channels/detail/IntrusivePtr.h"

namespace simm {
namespace ds {

// KV status definition
enum class KVStatus : uint32_t {
  KV_NONE = 0x00,
  KV_INITIALIZING = 0x01,
  KV_VALID = 0x02,
  KV_NOT_IN_MEMORY = 0x04,
  KV_MIGRATING = 0x08,
  KV_EVICTING = 0x10,
  KV_CLEAR = 0x20,
  KV_EXCLOCK = (0x01U << 31),
};

inline std::string KVStatusToString(uint32_t status) {
  if (status == static_cast<uint32_t>(KVStatus::KV_NONE))
    return "KVStatus{NONE}";
  std::vector<std::string> flags;
  if (static_cast<uint32_t>(status) & static_cast<uint32_t>(KVStatus::KV_INITIALIZING))
    flags.push_back("INITIALIZING");
  if (static_cast<uint32_t>(status) & static_cast<uint32_t>(KVStatus::KV_VALID))
    flags.push_back("VALID");
  if (static_cast<uint32_t>(status) & static_cast<uint32_t>(KVStatus::KV_NOT_IN_MEMORY))
    flags.push_back("NOT_IN_MEMORY");
  if (static_cast<uint32_t>(status) & static_cast<uint32_t>(KVStatus::KV_MIGRATING))
    flags.push_back("MIGRATING");
  if (static_cast<uint32_t>(status) & static_cast<uint32_t>(KVStatus::KV_EVICTING))
    flags.push_back("EVICTING");
  if (static_cast<uint32_t>(status) & static_cast<uint32_t>(KVStatus::KV_CLEAR))
    flags.push_back("CLEAR");
  if (static_cast<uint32_t>(status) & static_cast<uint32_t>(KVStatus::KV_EXCLOCK))
    flags.push_back("KV_EXCLOCK");
  std::ostringstream oss;
  oss << "KVStatus{";
  for (size_t i = 0; i < flags.size(); i++) {
    oss << flags[i];
    if (i != flags.size() - 1)
      oss << " | ";
  }
  oss << "}";
  return oss.str();
}

struct KVRefCntLock {
  // bit 0-28 : ref count
  // bit 29   : exclusive lock bit
  // bit 30   : linked in eviction list
  // bit 31   : accessed bit
  std::atomic<uint32_t> lock;
  static constexpr int EXCLUSIVE_BIT = 29;
  static constexpr int LINKED_BIT = 30;
  static constexpr int ACCESSED_BIT = 31;

  uint32_t GetValue() { return lock.load(std::memory_order_acquire); }
  void ResetValue() { lock.store(0, std::memory_order_relaxed); }

  bool IsBitSet(uint32_t val, int pos) { return (val >> pos) & 0x01U; }
  bool SetBit(uint32_t val, int pos) {
    uint32_t expect = val;
    volatile bool is_bit_set = IsBitSet(expect, pos);
    while (!is_bit_set) {
      val = expect | (0x01U << pos);
      bool res = lock.compare_exchange_strong(expect, val, std::memory_order_acq_rel, std::memory_order_acquire);
      if (!res) {
        is_bit_set = IsBitSet(expect, pos);
        continue;
      }
      else
        return true;
    }
    return false;
  }
  void ClearBit(int pos) {
    uint32_t mask = ~(0x01U << pos);
    lock.fetch_and(mask, std::memory_order_release);
  }

  bool IsExclusive(uint32_t value = UINT32_MAX) {
    if (value == UINT32_MAX)
      value = GetValue();
    return IsBitSet(value, EXCLUSIVE_BIT);
  }
  bool IsLinked(uint32_t value = UINT32_MAX) {
    if (value == UINT32_MAX)
      value = GetValue();
    return IsBitSet(value, LINKED_BIT);
  }
  bool IsAccessed(uint32_t value = UINT32_MAX) {
    if (value == UINT32_MAX)
      value = GetValue();
    return IsBitSet(value, ACCESSED_BIT);
  }

  bool SetExclusive() {
    uint32_t value = GetValue();
    return SetBit(value, EXCLUSIVE_BIT);
  }
  bool SetLinked() {
    uint32_t value = GetValue();
    return SetBit(value, LINKED_BIT);
  }
  bool SetAccessed() {
    uint32_t value = GetValue();
    return SetBit(value, ACCESSED_BIT);
  }

  void ClearExclusive() { ClearBit(EXCLUSIVE_BIT); }
  void ClearLinked() { ClearBit(LINKED_BIT); }
  void ClearAccessed() { ClearBit(ACCESSED_BIT); }

  bool IncRefCnt() {
    uint32_t value = GetValue();
    uint32_t expect = value;
    volatile bool exclusive = IsExclusive(expect);
    while (!exclusive) {
      value = expect + 1;
      bool res = lock.compare_exchange_strong(expect, value, std::memory_order_acq_rel, std::memory_order_acquire);
      if (!res) {
        exclusive = IsExclusive(expect);
        continue;
      }
      else
        return true;
    }
    return false;
  }
  void DecRefCnt() { lock.fetch_sub(1, std::memory_order_release); }
  uint32_t GetRefCnt(uint32_t value = UINT32_MAX) {
    if (value == UINT32_MAX)
      value = GetValue();
    uint32_t mask = (0x01U << EXCLUSIVE_BIT) - 0x01U;
    return value & mask;
  }

  std::string ToString(uint32_t value = UINT32_MAX) {
    std::ostringstream oss;
    if (value == UINT32_MAX)
      value = GetValue();
    oss << "KVRefCntLock{"
        << "IsExclusive: " << IsExclusive(value) << ", "
        << "IsLinked: " << IsLinked(value) << ", "
        << "IsAccessed: " << IsAccessed(value) << ", "
        << "RefCnt: " << GetRefCnt(value) << ", "
        << "}";
    return oss.str();
  }
};

struct KVEntry : public folly::channels::detail::IntrusivePtrBase<KVEntry> {
  SlabInfo slab_info;            // slab info
  KVRefCntLock ref_cnt_lock;     // ref count and lock
  std::atomic<uint32_t> status;  // kv status
  folly::SafeIntrusiveListHook evict_hook;

  static folly::channels::detail::intrusive_ptr<KVEntry> createIntrusivePtr() {
    auto ptr = KVObjectPool::Instance().AcquireValue();
    return folly::channels::detail::intrusive_ptr<KVEntry>(ptr);
  }

  static void operator delete(void* p) {
    KVObjectPool::Instance().ReleaseValue(static_cast<KVEntry*>(p));
  }

  std::string ToString() {
    std::ostringstream oss;
    oss << "KVEntry{" << slab_info.ToString() << ", " << ref_cnt_lock.ToString() << ", "
        << KVStatusToString(status.load(std::memory_order_acquire)) << ", "
        << "}";
    return oss.str();
  }
 private:
  KVEntry() = default;
  KVEntry(const KVEntry&) = delete;
  KVEntry(KVEntry&&) = delete;
  friend class KVObjectPool;
};
using KVEntryIntrusivePtr = folly::channels::detail::intrusive_ptr<KVEntry>;

struct KVHashKey {
  KVMeta* meta{nullptr};
  bool operator==(const KVHashKey &rhs) const {
    return meta->key_hash == rhs.meta->key_hash &&
        meta->key_len == rhs.meta->key_len &&
        ((meta->key_ptr && !rhs.meta->key_ptr && memcmp(meta->key_ptr, rhs.meta->key, meta->key_len) == 0 ) ||
        (!meta->key_ptr && rhs.meta->key_ptr && memcmp(meta->key, rhs.meta->key_ptr, meta->key_len) == 0 ) ||
        (!meta->key_ptr && !rhs.meta->key_ptr && memcmp(meta->key, rhs.meta->key, meta->key_len) == 0 ) ||
        (meta->key_ptr && rhs.meta->key_ptr && memcmp(meta->key_ptr, rhs.meta->key_ptr, meta->key_len) == 0 ));
  }
  KVHashKey(KVMeta* _meta) : meta(_meta) {}

  inline std::string KeyString(size_t head_chars = 16, size_t tail_chars = 4) {
    if (meta->key_len <= head_chars + tail_chars)
      return !meta->key_ptr ? std::string(meta->key, meta->key_len) : std::string(meta->key_ptr, meta->key_len);
    if (!meta->key_ptr)
      return std::string(meta->key, head_chars) + "..." +
        std::string(meta->key + meta->key_len - tail_chars, tail_chars);
    else
      return std::string(meta->key_ptr, head_chars) + "..." +
        std::string(meta->key_ptr + meta->key_len - tail_chars, tail_chars);
  }
  std::string ToString() {
    std::ostringstream oss;
    oss << "KVHashKey{"
        << "hash_value: " << meta->key_hash << ", "
        << "str: " << KeyString() << ", "
        << "len: " << meta->key_len << ","
        << "}";
    return oss.str();
  }
};

class KVHashTable {
 public:
  explicit KVHashTable();
  ~KVHashTable();

  // init kv hash table
  int Init(int shard_id);

  // find one kv
  bool Find(const KVHashKey &key, KVEntryIntrusivePtr &value);

  // insert one kv
  bool Insert(const KVHashKey &key, KVEntryIntrusivePtr &value);

  // remove one kv
  std::pair<bool, KVEntryIntrusivePtr> Remove(const KVHashKey &key);

  // Commit kv from pending to formal one
  bool Commit(const KVHashKey &key);

  // clear all elements
  void Clear();

  // Check if the table is empty.
  inline bool Empty() const { return hash_table_.empty(); }

  inline int GetShardID() { return shard_id_; }

 private:
  struct KVHasher {
    uint64_t operator()(const KVHashKey &key) const { return key.meta->key_hash; }
  };
  using HashTable = folly::F14FastMap<KVHashKey, KVEntryIntrusivePtr, KVHasher>;

  int shard_id_;

  std::shared_mutex rw_mutex_;
  HashTable hash_table_;
};

}  // namespace ds
}  // namespace simm
