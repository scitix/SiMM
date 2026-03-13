#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <vector>

#include "common/errcode/errcode_def.h"
#include "data_server/kv_cache_pool.h"
#include "data_server/kv_hash_table.h"
#include "folly/container/IntrusiveList.h"
#include "folly/synchronization/MicroSpinLock.h"

namespace simm {
namespace ds {

class KVRpcService;

class KVCacheEvictor {
 public:
  explicit KVCacheEvictor(KVRpcService *service);
  ~KVCacheEvictor();

  // Add an entry to the eviction list for its slab class
  void enque(KVEntryIntrusivePtr &entry);

  // Remove an entry from the eviction list
  void deque(KVEntryIntrusivePtr &entry);

  std::pair<error_code_t, SlabInfo> evict_slabinfo(SlabClass sc);

  std::pair<error_code_t, std::vector<SlabInfo>> evict_slabinfos(SlabClass sc, size_t num);

  // Evict a slab from the specified class
  // Returns OK if evicted, other error codes if failed
  std::pair<error_code_t, SlabInfo> evict_slab(SlabClass sc);

  bool evict_slabs(SlabClass sc, std::vector<KVMeta*>& kvmeta_vec);

 private:
  // Per-slab-class eviction list and iterator
  struct SlabEvictionList {
    using EvictionList = folly::CountedIntrusiveList<KVEntry, &KVEntry::evict_hook>;
    EvictionList list;
    EvictionList::iterator iter;
    folly::MicroSpinLock lock;
  };

  KVRpcService *service_;
  std::array<SlabEvictionList, SlabClassCount> slab_lists_;
};

}  // namespace ds
}  // namespace simm
