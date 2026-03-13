#include "data_server/kv_cache_evictor.h"

#include <atomic>
#include <thread>

#include "data_server/kv_cache_pool.h"
#include "data_server/kv_hash_table.h"
#include "data_server/kv_rpc_service.h"
#include "common/utils/time_util.h"
#include "common/logging/logging.h"

DECLARE_LOG_MODULE("data_server");

namespace simm {
namespace ds {

KVCacheEvictor::KVCacheEvictor(KVRpcService *service) : service_(service) {
  // Initialize all slab lists
  for (auto &lst : slab_lists_) {
    lst.lock.init();
    lst.iter = lst.list.end();
  }
}

KVCacheEvictor::~KVCacheEvictor() {
  // Clear all lists
  for (auto &lst : slab_lists_) {
    lst.list.clear();
  }
}

void KVCacheEvictor::enque(KVEntryIntrusivePtr &entry) {
  auto sc_idx = entry->slab_info.slab_class - 1;
  auto &lst = slab_lists_[sc_idx];

  lst.lock.lock();
  lst.list.push_back(*entry);
  lst.lock.unlock();
}

void KVCacheEvictor::deque(KVEntryIntrusivePtr &entry) {
  auto sc_idx = entry->slab_info.slab_class - 1;
  auto &lst = slab_lists_[sc_idx];

  lst.lock.lock();
  if (entry->evict_hook.is_linked()) {
    auto del_iter = lst.list.iterator_to(*entry);
    if (del_iter == lst.iter) {
      if (++lst.iter == lst.list.end() && lst.list.begin() != del_iter) {
        lst.iter = lst.list.begin();
      }
    }
    lst.list.erase(del_iter);
    if (lst.list.empty()) lst.iter = lst.list.end();
  }
  lst.lock.unlock();
}

std::pair<error_code_t, SlabInfo> KVCacheEvictor::evict_slabinfo(SlabClass sc) {
  size_t sc_idx = slab_index(sc);
  auto &lst = slab_lists_[sc_idx];

  lst.lock.lock();
  if (lst.list.empty()) {
    lst.lock.unlock();
    return {DsErr::NoEntriesToEvict, {}};
  }

  SlabInfo info;
  auto current = lst.iter == lst.list.end() ? lst.list.begin() : lst.iter;
  auto start = current;
  while (true) {
    uint32_t val = current->ref_cnt_lock.GetValue();
    if (!current->ref_cnt_lock.IsAccessed(val) && current->ref_cnt_lock.SetExclusive()) {
      auto &entry = *current;
      auto status = entry.status.load(std::memory_order_acquire);
      if (status == static_cast<uint32_t>(KVStatus::KV_VALID)) {
        info = entry.slab_info;
        current->ref_cnt_lock.ClearExclusive();
        lst.iter = ++current == lst.list.end() ? lst.list.begin() : current;
        lst.lock.unlock();
        return {CommonErr::OK, info};
      }
      current->ref_cnt_lock.ClearExclusive();
    }
    current->ref_cnt_lock.ClearAccessed();
    if (++current == lst.list.end())
      current = lst.list.begin();
    if (current == start) break;
  }
  lst.lock.unlock();
  return {DsErr::NoEntriesToEvict, info};
}

std::pair<error_code_t, std::vector<SlabInfo>> KVCacheEvictor::evict_slabinfos(SlabClass sc, size_t num) {
  size_t sc_idx = slab_index(sc);
  auto &lst = slab_lists_[sc_idx];

  lst.lock.lock();
  if (lst.list.empty()) {
    lst.lock.unlock();
    return {DsErr::NoEntriesToEvict, {}};
  }

  int step = 1;
  std::vector<SlabInfo> info_vec;
  info_vec.reserve(num);
  auto current = lst.iter == lst.list.end() ? lst.list.begin() : lst.iter;
  auto start = current;
  size_t loop_cnt = 0, loop_limit = 5;
  while (true) {
    uint32_t val = current->ref_cnt_lock.GetValue();
    if (!current->ref_cnt_lock.IsAccessed(val) && current->ref_cnt_lock.SetExclusive()) {
      auto &entry = *current;
      auto status = entry.status.load(std::memory_order_acquire);
      if (status == static_cast<uint32_t>(KVStatus::KV_VALID)) {
        info_vec.push_back(entry.slab_info);
        current->ref_cnt_lock.ClearExclusive();
        step = slab_count(sc);
        if (--num == 0) {
          lst.iter = ++current == lst.list.end() ? lst.list.begin() : current;
          lst.lock.unlock();
          return {CommonErr::OK, info_vec};
        }
      } else current->ref_cnt_lock.ClearExclusive();
    }
    current->ref_cnt_lock.ClearAccessed();
    while (step-- > 0) {
      if (++current == lst.list.end()) {
        current = lst.list.begin();
        loop_cnt++;
      }
    }
    step = 1;
    if (current == start || loop_cnt > loop_limit) break;
  }
  lst.iter = ++current == lst.list.end() ? lst.list.begin() : current;
  lst.lock.unlock();
  if (info_vec.empty())
    return {DsErr::NoEntriesToEvict, {}};
  else
    return {CommonErr::OK, info_vec};
}

bool KVCacheEvictor::evict_slabs(SlabClass sc, std::vector<KVMeta*>& kvmeta_vec) {
  size_t sc_idx = slab_index(sc);

  size_t kvmeta_size = kvmeta_vec.size();
  for (size_t i = 0; i < kvmeta_size; i++) {
    auto meta = kvmeta_vec[i];
    auto shard_id = meta->shard_id;
    KVHashKey key(meta);
    KVEntryIntrusivePtr value;
    auto table = service_->GetHashTable(shard_id);
    bool res = table->Find(key, value);
    if (res) {
      volatile bool set_exclusive = value->ref_cnt_lock.SetExclusive();
      while (!set_exclusive) {
        std::this_thread::yield();
        set_exclusive = value->ref_cnt_lock.SetExclusive();
      }
      auto status = value->status.load(std::memory_order_acquire);
      if (status == static_cast<uint32_t>(KVStatus::KV_VALID)) {
        volatile uint32_t ref_cnt = value->ref_cnt_lock.GetRefCnt();
        while (ref_cnt) {
          std::this_thread::yield();
          ref_cnt = value->ref_cnt_lock.GetRefCnt();
        }
        value->status.store(static_cast<uint32_t>(KVStatus::KV_EVICTING), std::memory_order_release);
        deque(value);
        value->ref_cnt_lock.ClearLinked();
        value->status.store(static_cast<uint32_t>(KVStatus::KV_CLEAR), std::memory_order_release);
        table->Remove(key);
        service_->shard_used_bytes_[value->slab_info.shard_id].fetch_sub(
            slab_size(sc) + META_SIZE, std::memory_order_relaxed);
      } else {
        MLOG_WARN("KVCacheEvictor evict_slabs class idx {} found entry object status abnormal {}",
            sc_idx, value->ToString())
      }
      value->ref_cnt_lock.ClearExclusive();
    }
  }
  return true;
}

std::pair<error_code_t, SlabInfo> KVCacheEvictor::evict_slab(SlabClass sc) {
  size_t sc_idx = slab_index(sc);
  auto &lst = slab_lists_[sc_idx];

  lst.lock.lock();

  if (lst.list.empty()) {
    lst.lock.unlock();
    return {DsErr::NoEntriesToEvict, {}};
  }

  // Start from current iterator position
  auto current = lst.iter == lst.list.end() ? lst.list.begin() : lst.iter;
  auto start = current;
  // size_t count = 0, limit = 1000;

  while (true) {
    // Check if entry can be evicted (accessed bit not set)
    uint32_t val = current->ref_cnt_lock.GetValue();
    if (!current->ref_cnt_lock.IsAccessed(val) && current->ref_cnt_lock.SetExclusive()) {
      // Found candidate for eviction
      auto &entry = *current;
      auto status = entry.status.load(std::memory_order_acquire);
      if (status == static_cast<uint32_t>(KVStatus::KV_VALID)) {
        uint32_t ref_cnt = entry.ref_cnt_lock.GetRefCnt();
        if (ref_cnt > 0) {
          bool timeout = false;
          auto beg_ts = utils::current_microseconds();
          while (ref_cnt > 0 && !timeout) {
            ref_cnt = entry.ref_cnt_lock.GetRefCnt();
            if (utils::delta_microseconds_to_now(beg_ts) > FLAGS_busy_wait_timeout_us) {
              timeout = true;
            }
          }
          if (ref_cnt > 0) {
            entry.ref_cnt_lock.ClearExclusive();
            if (++current == lst.list.end()) {
              current = lst.list.begin();
            }
            if (current == start) break;
            continue;
          }
        }
#ifndef NDEBUG
        MLOG_DEBUG("KVCacheEvictor evict slab class idx {} found entry object {}", sc_idx, entry.ToString());
#endif
        entry.status.store(static_cast<uint32_t>(KVStatus::KV_EVICTING), std::memory_order_release);
        SlabInfo info = current->slab_info;
        auto del_iter = current++;
        lst.list.erase(del_iter);
        if (lst.list.empty()) lst.iter = lst.list.end();
        else lst.iter = current == lst.list.end() ? lst.list.begin() : current;
        lst.lock.unlock();

        entry.ref_cnt_lock.ClearLinked();
        auto [meta, _] = KVCachePool::GetBufferPair(&info);
        KVHashKey key(meta);
        std::atomic_ref<uint8_t> active_flag(meta->active_flag);
        bool reset_flag = false;
        bool is_active = service_->cache_pool_->is_active(&info);
        if (is_active) {
          reset_flag = true;
          active_flag.store(0, std::memory_order_release);
        }
        is_active = service_->cache_pool_->is_active(&info); // double confirm
        entry.status.store(static_cast<uint32_t>(KVStatus::KV_CLEAR), std::memory_order_release);
        auto table = service_->GetHashTable(info.shard_id);
        auto [res, value] = table->Remove(key);
        service_->shard_used_bytes_[info.shard_id].fetch_sub(
            slab_size(sc) + META_SIZE, std::memory_order_relaxed);
        value->ref_cnt_lock.ClearExclusive();
        if (!is_active) {
          if (reset_flag) active_flag.store(1, std::memory_order_release);
          lst.lock.lock();
          if (!lst.list.empty()) {
            current = lst.iter == lst.list.end() ? lst.list.begin() : lst.iter;
            start = current;
            continue;
          } else {
            break;
          }
        }
#ifndef NDEBUG
        MLOG_DEBUG("KVCacheEvictor evict done for key {} at entry: {} {}",
                  key.ToString(), (void*)value.get(), value->ToString());
#endif
        return {CommonErr::OK, info};
      }
      entry.ref_cnt_lock.ClearExclusive();
    }

    // Clear accessed bit for next round
    current->ref_cnt_lock.ClearAccessed();
    if (++current == lst.list.end())
      current = lst.list.begin();
    if (current == start) break;
    // if (++count > limit) break; get away from long latency due to all accessed
  }

  if (lst.list.empty()) {
    lst.lock.unlock();
    return {DsErr::NoEntriesToEvict, {}};
  }

  // randomly evict one and ignored accessed bit
  current = lst.list.begin();
  start = current;
  while (true) {
    if (current->ref_cnt_lock.SetExclusive()) {
      auto &entry = *current;
      auto status = entry.status.load(std::memory_order_acquire);
      if (status == static_cast<uint32_t>(KVStatus::KV_VALID)) {
        uint32_t ref_cnt = entry.ref_cnt_lock.GetRefCnt();
        if (ref_cnt > 0) {
          bool timeout = false;
          auto beg_ts = utils::current_microseconds();
          while (ref_cnt > 0 && !timeout) {
            ref_cnt = entry.ref_cnt_lock.GetRefCnt();
            if (utils::delta_microseconds_to_now(beg_ts) > FLAGS_busy_wait_timeout_us) {
              timeout = true;
            }
          }
          if (ref_cnt > 0) {
            entry.ref_cnt_lock.ClearExclusive();
            if (++current == lst.list.end()) {
              current = lst.list.begin();
            }
            if (current == start) break;
            continue;
          }
        }
#ifndef NDEBUG
        MLOG_DEBUG("KVCacheEvictor evict slab class idx {} found entry object {}", sc_idx, entry.ToString());
#endif
        entry.status.store(static_cast<uint32_t>(KVStatus::KV_EVICTING), std::memory_order_release);
        SlabInfo info = current->slab_info;
        auto del_iter = current++;
        lst.list.erase(del_iter);
        if (lst.list.empty()) lst.iter = lst.list.end();
        else lst.iter = current == lst.list.end() ? lst.list.begin() : current;
        // lst.iter = current;
        lst.lock.unlock();

        entry.ref_cnt_lock.ClearLinked();
        auto [meta, _] = KVCachePool::GetBufferPair(&info);
        KVHashKey key(meta);
        std::atomic_ref<uint8_t> active_flag(meta->active_flag);
        bool reset_flag = false;
        bool is_active = service_->cache_pool_->is_active(&info);
        if (is_active) {
          reset_flag = true;
          active_flag.store(0, std::memory_order_release);
        }
        is_active = service_->cache_pool_->is_active(&info); // double confirm
        entry.status.store(static_cast<uint32_t>(KVStatus::KV_CLEAR), std::memory_order_release);
        auto table = service_->GetHashTable(info.shard_id);
        auto [res, value] = table->Remove(key);
        service_->shard_used_bytes_[info.shard_id].fetch_sub(
            slab_size(sc) + META_SIZE, std::memory_order_relaxed);
        entry.ref_cnt_lock.ClearExclusive();
        if (!is_active) {
          if (reset_flag) active_flag.store(1, std::memory_order_release);
          lst.lock.lock();
          if (!lst.list.empty()) {
            current = lst.iter == lst.list.end() ? lst.list.begin() : lst.iter;
            start = current;
            continue;
          } else {
            break;
          }
        }
#ifndef NDEBUG
        MLOG_DEBUG("KVCacheEvictor evict done for key {} at entry: {} {}",
                  key.ToString(), (void*)value.get(), value->ToString());
#endif
        return {CommonErr::OK, info};
      }
      entry.ref_cnt_lock.ClearExclusive();
    }
    if (++current == lst.list.end()) {
      current = lst.list.begin();
    }
    if (current == start) break;
  }
  lst.iter = lst.list.end();
  MLOG_ERROR("KVCacheEvictor cannot find a slab to evict in slab idx {} with {} entries",
            sc_idx, lst.list.size());
  lst.lock.unlock();
  return {DsErr::NoEntriesToEvict, {}};
}

}  // namespace ds
}  // namespace simm
