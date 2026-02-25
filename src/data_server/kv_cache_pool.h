#pragma once

#include <atomic>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <condition_variable>
#include "common/base/common_types.h"
#include "common/base/consts.h"
#include "data_server/ds_common.h"
#include "data_server/ds_memory_allocator.h"
#include "transport/mempool.h"
#include "folly/executors/CPUThreadPoolExecutor.h"

namespace simm {
namespace ds {

/*
 * Block and Chunk Structure Layout
 * =================================
 *
 * BLOCK (1GB = 1024MB)
 * ┌─────────────────────────────────────────────────────────────────────────┐
 * │                                                                         │
 * │  CHUNK 0 (64MB)  │  CHUNK 1 (64MB)  │  ...  │  CHUNK 15 (64MB)          │
 * │                                                                         │
 * └─────────────────────────────────────────────────────────────────────────┘
 *         │                    │                           │
 *   16 chunks per block (CHUNKS_PER_BLOCK = 16)
 *
 *
 * CHUNK (64MB)
 * ┌──────────────────────────────────────────────────────────────────────┐
 * │ HEADER_SIZE (16KB)                                                   │
 * │ ┌──────────────────────────────────────────────────────────────────┐ │
 * │ │ ChunkHeader (16 bytes)                                           │ │
 * │ │   - slab_count: uint32_t                                         │ │
 * │ │   - slab_class: SlabClass                                        │ │
 * │ │   - active_flag: uint8_t                                         │ │
 * │ │   - bitmap_offset: uint16_t (points to CHUNK_BITMAP_OFFSET)      │ │
 * │ │   - data_offset: uint64_t (points to data region start)          │ │
 * │ ├──────────────────────────────────────────────────────────────────┤ │
 * │ │ Reserved area (CHUNK_BITMAP_OFFSET - sizeof(ChunkHeader))        │ │
 * │ ├──────────────────────────────────────────────────────────────────┤ │
 * │ │ Bitmap Region (starts at CHUNK_BITMAP_OFFSET = 64)               │ │
 * │ │   - One bit per slab for allocation tracking                     │ │
 * │ │   - Size: align_up(slab_count, 64) / 8 bytes                     │ │
 * │ │   - Must fit within HEADER_SIZE (16KB)                           │ │
 * │ ├──────────────────────────────────────────────────────────────────┤ │
 * │ │ Padding to align to 16KB boundary                                │ │
 * │ └──────────────────────────────────────────────────────────────────┘ │
 * ├──────────────────────────────────────────────────────────────────────┤
 * │ META REGION (4KB-aligned)                                            │
 * │ ┌──────────────────┬──────────────────┬──────────┬────────────────┐  │
 * │ │ KVMeta[0] (512B) │ KVMeta[1] (512B) │   ...    │ KVMeta[N-1]    │  │
 * │ └──────────────────┴──────────────────┴──────────┴────────────────┘  │
 * │   Total: slab_count × META_SIZE (aligned up to 4KB boundary)         │
 * ├──────────────────────────────────────────────────────────────────────┤
 * │ DATA REGION (slab data storage)                                      │
 * │ ┌──────────────────┬──────────────────┬──────────┬────────────────┐  │
 * │ │ Slab[0]          │ Slab[1]          │   ...    │ Slab[N-1]      │  │
 * │ │ (slab_size bytes)│ (slab_size bytes)│          │ (slab_size)    │  │
 * │ └──────────────────┴──────────────────┴──────────┴────────────────┘  │
 * │   Total: slab_count × slab_size (where slab_size depends on          │
 * │         SlabClass, e.g., 4KB, 32KB, 256KB, 1MB, 4MB, 8MB, 16MB,      │
 * │         or 64MB-20KB for SLAB_64MB_LESS20KB)                         │
 * └──────────────────────────────────────────────────────────────────────┘
 *
 * CHUNK SIZE CALCULATION:
 *   CHUNK_SIZE (64MB) = HEADER_SIZE + meta_aligned + (slab_count × slab_size)
 *   where:
 *     - HEADER_SIZE = 16KB (contains ChunkHeader + bitmap)
 *     - meta_aligned = align_up(slab_count × META_SIZE, PAGE_SIZE)
 *     - data_offset = HEADER_SIZE + meta_aligned
 *     - Each chunk belongs to ONE SlabClass only
 *     - All slabs in a chunk have the same size (slab_size)
 *
 * SLAB LAYOUT:
 *   Each slab consists of:
 *   1. One KVMeta entry (512 bytes) in the META REGION
 *   2. One data region (slab_size bytes) in the DATA REGION
 *
 *   To access a specific slab:
 *     - Meta pointer: chunk + HEADER_SIZE + slab_idx × META_SIZE
 *     - Data pointer: chunk + data_offset + slab_idx × slab_size
 */

// Slab classes definition
enum class SlabClass : uint8_t {
  SLAB_NONE = 0,
  SLAB_4KB = 1,
  SLAB_32KB = 2,
  SLAB_256KB = 3,
  SLAB_1MB = 4,
  SLAB_4MB = 5,
  SLAB_8MB = 6,
  SLAB_16MB = 7,
  SLAB_64MB_LESS20KB = 8,
};

constexpr std::array<SlabClass, 8> all_slabs = {
    SlabClass::SLAB_4KB,
    SlabClass::SLAB_32KB,
    SlabClass::SLAB_256KB,
    SlabClass::SLAB_1MB,
    SlabClass::SLAB_4MB,
    SlabClass::SLAB_8MB,
    SlabClass::SLAB_16MB,
    SlabClass::SLAB_64MB_LESS20KB,
};
constexpr size_t SlabClassCount = all_slabs.size();

constexpr int SLAB_CLASS_INDICES[] = {-1, 0, 1, 2, 3, 4, 5, 6, 7};

constexpr size_t slab_index(SlabClass sc) {
  return static_cast<size_t>(SLAB_CLASS_INDICES[static_cast<uint8_t>(sc)]);
}

// Slab sizes in bytes
constexpr size_t SLAB_CLASS_SIZES[] = {0, 1ULL << 12, 1ULL << 15,
                                      1ULL << 18, 1ULL << 20, 1ULL << 22,
                                      1ULL << 23, 1ULL << 24, (1ULL << 26) - (20ULL << 10)};

// Get size in bytes of a slab class
constexpr size_t slab_size(SlabClass sc) {
  return SLAB_CLASS_SIZES[static_cast<uint8_t>(sc)];
}

constexpr size_t max_slab_cnt(size_t slab_sz) {
  size_t max_n = 0;
  for (size_t n = 1;; n++) {
    size_t meta_aligned = align_up(n * META_SIZE, PAGE_SIZE);
    size_t total = HEADER_SIZE + meta_aligned + n * slab_sz;
    if (total > CHUNK_SIZE) break;
    max_n = n;
  }
  return max_n;
}

// Slab count calculation per chunk
template <SlabClass sc>
class SlabCount {
  static constexpr size_t getNumber() {
    const uint8_t idx = static_cast<uint8_t>(sc);
    if (idx > 0 && idx <= SlabClassCount) {
      return max_slab_cnt(slab_size(sc));
    } else
      return 0;
  }

 public:
  static constexpr size_t N = getNumber();
};

constexpr size_t SLAB_CLASS_COUNT[] = {
    0,
    SlabCount<SlabClass::SLAB_4KB>::N,
    SlabCount<SlabClass::SLAB_32KB>::N,
    SlabCount<SlabClass::SLAB_256KB>::N,
    SlabCount<SlabClass::SLAB_1MB>::N,
    SlabCount<SlabClass::SLAB_4MB>::N,
    SlabCount<SlabClass::SLAB_8MB>::N,
    SlabCount<SlabClass::SLAB_16MB>::N,
    SlabCount<SlabClass::SLAB_64MB_LESS20KB>::N,
};

constexpr size_t slab_count(SlabClass sc) {
  return SLAB_CLASS_COUNT[static_cast<uint8_t>(sc)];
}

template <SlabClass sc>
constexpr bool slab_bitmap_allowable() {
  return CHUNK_BITMAP_OFFSET + align_up(SlabCount<sc>::N, sizeof(uint64_t) * 8) / 8 < HEADER_SIZE;
}

constexpr bool all_slabs_bitmap_allowable() {
  for (SlabClass sc : all_slabs) {
    switch (sc) {
      case SlabClass::SLAB_4KB:
        if (!slab_bitmap_allowable<SlabClass::SLAB_4KB>())
          return false;
        break;
      case SlabClass::SLAB_32KB:
        if (!slab_bitmap_allowable<SlabClass::SLAB_32KB>())
          return false;
        break;
      case SlabClass::SLAB_256KB:
        if (!slab_bitmap_allowable<SlabClass::SLAB_256KB>())
          return false;
        break;
      case SlabClass::SLAB_1MB:
        if (!slab_bitmap_allowable<SlabClass::SLAB_1MB>())
          return false;
        break;
      case SlabClass::SLAB_4MB:
        if (!slab_bitmap_allowable<SlabClass::SLAB_4MB>())
          return false;
        break;
      case SlabClass::SLAB_8MB:
        if (!slab_bitmap_allowable<SlabClass::SLAB_8MB>())
          return false;
        break;
      case SlabClass::SLAB_16MB:
        if (!slab_bitmap_allowable<SlabClass::SLAB_16MB>())
          return false;
        break;
      case SlabClass::SLAB_64MB_LESS20KB:
        if (!slab_bitmap_allowable<SlabClass::SLAB_64MB_LESS20KB>())
          return false;
        break;
      default:
        return false;
    }
  }
  return true;
}
static_assert(all_slabs_bitmap_allowable(), "Not all slabs bitmap space adequate");

// Chunk header
struct ChunkHeader {
  uint32_t slab_count;     // 4 bytes
  SlabClass slab_class;    // 1 byte
  uint8_t active_flag;     // 1 byte
  uint16_t bitmap_offset;  // 2 bytes
  uint64_t data_offset;    // 8 bytes
};
static_assert(sizeof(ChunkHeader) <= HEADER_SIZE, "ChunkHeader size mismatch");

// KV meta (512 bytes)
struct KVMeta {
  uint8_t active_flag;   // 1 bytes
  uint16_t key_len;      // 2 bytes
  uint32_t value_len;    // 4 bytes
  uint64_t key_hash;     // 8 bytes
  uint64_t value_crc;    // 8 bytes
  uint32_t ctime;        // 4 bytes
  uint32_t ttl;          // 4 bytes
  char* key_ptr;         // 8 bytes (tmp pointer to key string)
  uint32_t shard_id;     // 4 bytes
  uint8_t reserved[20];  // 20 bytes
  char key[448];         // 448 bytes (total 512 bytes)

  KVMeta() = default;
  KVMeta(const char *_key, const size_t _key_len) {
    key_len = (uint16_t)_key_len;
    memcpy(key, _key, key_len);
  }
};
static_assert(sizeof(KVMeta) == META_SIZE, "KVMeta size mismatch");

// Slab information
struct SlabInfo {
  uint32_t shard_id = 0;
  uint8_t slab_class = 0;
  uint16_t block_idx = 0;
  uint8_t chunk_idx = 0;
  MemBlock *block_addr = nullptr;
  size_t slab_idx = 0;
  size_t chunk_ctime_tag = 0;

  std::string ToString() const {
    std::ostringstream oss;
    oss << "SlabInfo{"
        << "shard_id: " << shard_id << ", "
        << "slab_class: " << slab_class << ", "
        << "block_idx: " << block_idx << ", "
        << "chunk_idx: " << chunk_idx << ", "
        << "block_addr: " << block_addr << ", "
        << "slab_idx: " << slab_idx << ", "
        << "}";
    return oss.str();
  }
} __attribute__((packed));

class KVCacheEvictor;

class KVCachePool {
 public:
  explicit KVCachePool();
  ~KVCachePool();

  int init(uint64_t bound_bytes,
           sicl::transport::Mempool *mem_registry = nullptr,
           KVCacheEvictor *evictor = nullptr,
           int initial_blocks = 3,
           bool hugepage = false);

  // return chunk info and allocated slab pointer
  bool allocate(size_t value_len, SlabInfo *slab_info);

  // free slab
  bool free(SlabInfo *slab_info);

  // determin whether a given slab is still active on data
  bool is_active(SlabInfo *slab_info);

  // Get shard usage bytes
  size_t mem_total_bytes() { return cache_memory_bound_bytes_; }
  size_t mem_used_bytes() {
    size_t blk_idx = block_seq_idx_.load(std::memory_order_relaxed);
    return blk_idx * BLOCK_SIZE;
  }
  size_t mem_free_bytes() {
    size_t blk_idx = block_seq_idx_.load(std::memory_order_relaxed);
    return (max_memory_block_num_ - blk_idx) * BLOCK_SIZE;
  }

  static std::pair<KVMeta *, void *> GetBufferPair(SlabInfo *slab_info) {
    char *addr = slab_info->block_addr->buf;
    char *chunk = addr + (size_t)slab_info->chunk_idx * CHUNK_SIZE;
    SlabClass sc = static_cast<SlabClass>(slab_info->slab_class);
    ChunkHeader *header = (ChunkHeader *)chunk;
    size_t ofs = header->data_offset;
    char *meta = chunk + HEADER_SIZE + slab_info->slab_idx * META_SIZE;
    char *data = chunk + ofs + slab_info->slab_idx * slab_size(sc);
    return std::make_pair((KVMeta *)meta, (void *)data);
  }

 private:
  void clean_stale_block(const std::string& shm_path);

  bool init_block(size_t idx);

  bool init_chunk(size_t block_idx, size_t chunk_idx, SlabClass sc);

  SlabClass select_slab_class(size_t size) const;

  bool evict_chunk(size_t block_idx, size_t chunk_idx, size_t ctime_tag);

 private:
  static constexpr std::string_view block_shmname_prefix_ = "kv_cache_blk_";
  static constexpr double chunk_eviction_threshold_ = 0.9;

  bool hugepage_;
  volatile bool initialized_;
  size_t pre_alloc_block_num_;
  std::unique_ptr<std::thread> pre_alloc_blk_thread_;
  std::atomic<size_t> block_seq_idx_;
  std::vector<MemBlock *> block_shm_vec_;

  // we maintain a cursor for every slab class to accelerate slab finding
  std::array<std::atomic<size_t>, SlabClassCount> slab_cursor_{std::atomic<size_t>{0}};

  // following container's size is in consistent with block number
  // each chunk use one bit for global bitmap to accelerate slab finding
  std::deque<std::atomic<uint16_t>> chunk_bitmap_;

  // each chunk uses 4 bits to represent slab class and slab status
  // 0: unallocated and uninitialized
  // 1-8: corresponding slab class
  // d: in transition(class level changing)
  // e: free to use(has not assigned any class level)
  // f: block allocate in progress
  enum ChunkStatus : uint8_t {
    UNINIT = 0,
    CHANGING = 13,
    FREETOUSE = 14,
    ALLOCATE = 15,
  };
  // each block uses 64 bits to represent status of all chunks(16) 
  std::deque<std::atomic<uint64_t>> chunk_status_;

  // each chunk uses a counter to write down assigned number
  std::deque<std::array<std::atomic<uint32_t>, CHUNKS_PER_BLOCK>> chunk_assign_cnt_;

  // each chunk uses a uint64 to track create time tag
  std::deque<std::array<std::atomic<size_t>, CHUNKS_PER_BLOCK>> chunk_ctime_tag_;

  // following varaibles are for memory stats
  size_t cache_memory_bound_bytes_;
  size_t max_memory_block_num_;

  std::array<std::atomic<size_t>, SlabClassCount> slab_allocate_num_{std::atomic<size_t>{0}};
  std::array<std::atomic<size_t>, SlabClassCount> slab_used_num_{std::atomic<size_t>{0}};
  std::array<std::atomic<size_t>, SlabClassCount> slab_used_bytes_{std::atomic<size_t>{0}};
  std::array<std::atomic<size_t>, SlabClassCount> slab_chunk_num_{std::atomic<size_t>{0}};

  // other module variables
  MemoryAllocator *allocator_;
  KVCacheEvictor *evictor_;

  std::mutex pre_alloc_blk_thread_mutex_;
  std::condition_variable pre_alloc_blk_thread_condv_;
  std::atomic<bool> stop_pre_alloc_blk_thread_{false};
  std::atomic<size_t> done_alloc_blk_idx_;

  std::unique_ptr<std::thread> bg_evict_chk_thread_;
  std::mutex bg_evict_chk_thread_mutex_;
  std::condition_variable bg_evict_chk_thread_condv_;
  std::atomic<bool> stop_bg_evict_chk_thread_{false};
  std::unique_ptr<folly::CPUThreadPoolExecutor> bg_evict_chk_threadpool_;

#if defined(SIMM_UNIT_TEST)
  FRIEND_TEST(KVPoolTest, TestAllocateFree);
#endif
};

}  // namespace ds
}  // namespace simm
