#include <atomic>
#include <mutex>
#include <thread>

#include <folly/Likely.h>
#include <folly/ThreadLocal.h>
#include <folly/hash/Hash.h>

#include "data_server/kv_hash_table.h"

namespace simm {
namespace ds {

KVHashTable::KVHashTable() : shard_id_(-1) {}

KVHashTable::~KVHashTable() {
  Clear();
}

int KVHashTable::Init(int shard_id) {
  shard_id_ = shard_id;
  return 0;
}

bool KVHashTable::Find(const KVHashKey &key, KVEntryIntrusivePtr &value) {
  bool found = false;
  {
    std::shared_lock<std::shared_mutex> read_lock(rw_mutex_);
    auto iter = hash_table_.find(key);
    if (iter != hash_table_.end()) {
      found = true;
      value = iter->second;
    }
  }
  return found;
}

bool KVHashTable::Insert(const KVHashKey &key, KVEntryIntrusivePtr &value) {
  bool res = false;
  {
    std::unique_lock<std::shared_mutex> write_lock(rw_mutex_);
    auto ret = hash_table_.emplace(key, value);
    res = ret.second;
  }
  return res;
}

std::pair<bool, KVEntryIntrusivePtr> KVHashTable::Remove(const KVHashKey &key) {
  bool res = false;
  KVEntryIntrusivePtr value;
  {
    std::unique_lock<std::shared_mutex> write_lock(rw_mutex_);
    auto iter = hash_table_.find(key);
    if (iter != hash_table_.end()) {
      value = iter->second;
      hash_table_.erase(iter);
      res = true;
    }
  }
  return std::make_pair(res, value);
}

bool KVHashTable::Commit(const KVHashKey &key) {
  bool res = false;
  {
    std::unique_lock<std::shared_mutex> write_lock(rw_mutex_);
    auto iter = hash_table_.find(key);
    if (FOLLY_UNLIKELY(iter == hash_table_.end())) {
      return false;
    }
    auto value = iter->second;
    hash_table_.erase(iter);
    auto ret = hash_table_.emplace(key, value);
    res = ret.second;
  }
  return res;
}

void KVHashTable::Clear() {
  std::unique_lock<std::shared_mutex> write_lock(rw_mutex_);
  hash_table_.clear();
}

}  // namespace ds
}  // namespace simm
