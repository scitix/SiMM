#include <gflags/gflags_declare.h>
#include <pthread.h>
#include <unistd.h>
#include <algorithm>
#include <atomic>
#include <bitset>
#include <chrono>
#include <cstring>
#include <limits>
#include <mutex>
#include <numeric>
#include <set>
#include <stdexcept>
#include <thread>
#include "fmt/format.h"

#include "common/base/consts.h"
#include "common/errcode/errcode_def.h"
#include "common/logging/logging.h"
#include "common/utils/file_util.h"
#include "common/utils/time_util.h"
#include "data_server/ds_common.h"
#include "data_server/ds_memory_allocator.h"
#include "data_server/kv_cache_evictor.h"
#include "data_server/kv_cache_pool.h"

DECLARE_LOG_MODULE("data_server");

DECLARE_uint32(shard_total_num);
DECLARE_int32(ds_bg_evict_thread_num);
DECLARE_uint32(ds_bg_evict_interval_ms);
DECLARE_uint32(ds_bg_evict_prefetch_factor);
DECLARE_double(ds_bg_evict_trigger_threshold);
DECLARE_uint32(ds_bg_evict_slab_class_num);
DECLARE_uint64(ds_bg_evict_chunk_cooldown_ms);
DECLARE_bool(ds_clean_stale_block);

namespace simm {
namespace ds {

KVCachePool::KVCachePool() {
  initialized_ = false;
  block_seq_idx_.store(0, std::memory_order_relaxed);
}

KVCachePool::~KVCachePool() {
  {
    stop_pre_alloc_blk_thread_ = true;
    std::lock_guard<std::mutex> lock(pre_alloc_blk_thread_mutex_);
    pre_alloc_blk_thread_condv_.notify_all();
  }
  if (pre_alloc_blk_thread_) {
    pre_alloc_blk_thread_->join();
    pre_alloc_blk_thread_.reset();
  }
  if (bg_evict_chk_threadpool_) {
    bg_evict_chk_threadpool_->stop();
    bg_evict_chk_threadpool_->join();
    bg_evict_chk_threadpool_.reset();
  }
  {
    stop_bg_evict_chk_thread_ = true;
    std::lock_guard<std::mutex> lock(bg_evict_chk_thread_mutex_);
    bg_evict_chk_thread_condv_.notify_all();
  }
  if (bg_evict_chk_thread_) {
    bg_evict_chk_thread_->join();
    bg_evict_chk_thread_.reset();
  }
  for (auto &block : block_shm_vec_) {
    if (block) {
      allocator_->free(block);
      delete block;
      block = nullptr;
    }
  }
  block_shm_vec_.clear();
}

int KVCachePool::init(uint64_t bound_bytes,
                      sicl::transport::Mempool *mem_registry,
                      KVCacheEvictor *evictor,
                      int initial_blocks,
                      bool hugepage) {
  if (!initialized_) {
    hugepage_ = hugepage;
    allocator_ = &ShmAllocator::Instance(mem_registry);
    evictor_ = evictor;
    max_memory_block_num_ = bound_bytes / BLOCK_SIZE;
    if (initial_blocks < 0 || (size_t)initial_blocks > max_memory_block_num_) {
      MLOG_WARN("KVCachePool::init initial_blocks:{} is greater than max_memory_block_num_:{}, set to upper bound",
                initial_blocks, max_memory_block_num_);
      initial_blocks = max_memory_block_num_;
    }
    cache_memory_bound_bytes_ = max_memory_block_num_ * BLOCK_SIZE;
    if (max_memory_block_num_ == 0 || initial_blocks == 0) {
      MLOG_ERROR("KVCachePool::init bound_bytes:{} or initial_blocks:{} are incorrect",
                 cache_memory_bound_bytes_,
                 initial_blocks);
      return -1;
    }
    pre_alloc_block_num_ = (size_t)initial_blocks;
    block_shm_vec_.resize(max_memory_block_num_, nullptr);
    chunk_bitmap_.resize(max_memory_block_num_);
    chunk_status_.resize(max_memory_block_num_);
    chunk_assign_cnt_.resize(max_memory_block_num_);
    chunk_ctime_tag_.resize(max_memory_block_num_);
    for (size_t i = 0; i < max_memory_block_num_; i++) {
      chunk_bitmap_[i].store(0, std::memory_order_relaxed);
      chunk_status_[i].store(0, std::memory_order_relaxed);
      for (size_t j = 0; j < chunk_assign_cnt_[i].size(); j++) {
        chunk_assign_cnt_[i][j].store(0, std::memory_order_relaxed);
        chunk_ctime_tag_[i][j].store(0, std::memory_order_relaxed);
      }
    }
    if (FLAGS_ds_clean_stale_block) {
      std::string shm_path = "/dev/shm";
      clean_stale_block(shm_path);
    }
    // init blocks
    for (size_t i = 0; i < pre_alloc_block_num_; i++) {
      size_t block_idx = block_seq_idx_.fetch_add(1, std::memory_order_relaxed);
      bool res = init_block(block_idx);
      if (!res) {
        MLOG_ERROR("Init cache block failed, res:{}", res);
        return -1;
      }
    }
    // init chunks for the first block
    if (max_memory_block_num_ != 1) {
      for (size_t i = 0; i < CHUNKS_PER_BLOCK; i++) {
        size_t slab_idx = i % SlabClassCount;
        auto sc = all_slabs[slab_idx];
        bool res = init_chunk(0, i, sc);
        if (!res) {
          MLOG_ERROR("Init cache chunk failed, res:{}", res);
        }
      }
    }

    done_alloc_blk_idx_ = pre_alloc_block_num_ - 1;
    pre_alloc_blk_thread_ = std::make_unique<std::thread>([this] {
      while (true) {
        {
          std::unique_lock<std::mutex> lock(pre_alloc_blk_thread_mutex_);
          pre_alloc_blk_thread_condv_.wait_for(lock, std::chrono::seconds(5), [this] {
            return stop_pre_alloc_blk_thread_.load(std::memory_order_acquire) ||
                block_seq_idx_.load(std::memory_order_acquire) > done_alloc_blk_idx_ + 1;
          });
        }
        if (stop_pre_alloc_blk_thread_.load(std::memory_order_acquire)) {
          break;
        }
        size_t blk_seq = block_seq_idx_.load(std::memory_order_acquire);
        while (done_alloc_blk_idx_ + 1 < blk_seq) {
          for (size_t i = done_alloc_blk_idx_ + 1; i < blk_seq; i++) {
            bool res = init_block(i);
            if (res) {
              done_alloc_blk_idx_ = i;
            } else {
              MLOG_ERROR("[Caution] Alloc cache block {} failed !!!", i);
              break;
            }
          }
          blk_seq = block_seq_idx_.load(std::memory_order_acquire);
        }
      }
    });
    pthread_setname_np(pre_alloc_blk_thread_->native_handle(), "pre_alloc_tid");

    if (FLAGS_ds_bg_evict_thread_num >= 0) {
      size_t th_num = FLAGS_ds_bg_evict_thread_num;
      if (th_num > 0) {
        bg_evict_chk_threadpool_ = std::make_unique<folly::CPUThreadPoolExecutor>(th_num,
            std::make_shared<folly::NamedThreadFactory>("bg_evict_tp"));
      }
      bg_evict_chk_thread_ = std::make_unique<std::thread>([th_num, this] {
        while (true) {
          {
            std::unique_lock<std::mutex> lock(bg_evict_chk_thread_mutex_);
            bg_evict_chk_thread_condv_.wait_for(lock, std::chrono::milliseconds(FLAGS_ds_bg_evict_interval_ms), [this] {
                return stop_bg_evict_chk_thread_.load(std::memory_order_acquire);
            });
          }
          if (stop_bg_evict_chk_thread_.load(std::memory_order_acquire)) break;
          // evict chunks only when blocks are all initialized
          if (max_memory_block_num_ <= block_seq_idx_.load(std::memory_order_acquire)) {
            size_t max_chk_num = max_memory_block_num_ * CHUNKS_PER_BLOCK;
            bool first_check = true;
            while (true) {
              size_t tot_chk_num = 0;
              bool continue_to_run = false;
              std::vector<size_t> slab_chunk_num(slab_chunk_num_.size());
              std::vector<size_t> slab_indices(slab_chunk_num_.size());
              std::iota(slab_indices.begin(), slab_indices.end(), 0);
              for (size_t i = 0; i < slab_chunk_num_.size(); i++) {
                size_t chk_num = slab_chunk_num_[i].load(std::memory_order_acquire);
                slab_chunk_num[i] = chk_num;
                tot_chk_num += chk_num;
              }
              std::sort(slab_indices.begin(), slab_indices.end(),
                  [&](size_t i, size_t j) {
                    return slab_chunk_num[i] > slab_chunk_num[j];
              });
              std::vector<size_t> most_sc_idx_vec(slab_indices.begin(), slab_indices.begin() + FLAGS_ds_bg_evict_slab_class_num);

              // evict chunks if total chunk number exceeds trigger threshold
              if ((double)tot_chk_num >= (double)max_chk_num * FLAGS_ds_bg_evict_trigger_threshold) {
                // clean most slab class occupied chunks
                std::set<size_t> empty_slab_class;
                for (auto most_sc_idx : most_sc_idx_vec) {
                  if (slab_chunk_num[most_sc_idx] > 0) {
                    if (bg_evict_chk_threadpool_) {
                      auto pair = evictor_->evict_slabinfos(all_slabs[most_sc_idx], th_num * FLAGS_ds_bg_evict_prefetch_factor);
                      if (pair.first == CommonErr::OK) {
                        for (size_t i = 0; i < pair.second.size(); i++) {
                          auto& slab_info = pair.second[i];
                          bg_evict_chk_threadpool_->add( [slab_info, this] {
                            bool ret = evict_chunk(slab_info.block_idx, slab_info.chunk_idx, slab_info.chunk_ctime_tag);
                            if (ret) {
                              MLOG_INFO("bg_evict_tp evict block idx {} chunk idx {}",
                                        (size_t)slab_info.block_idx, slab_info.chunk_idx);
                            }
                          });
                        }
                        continue_to_run = true;
                      }
                    } else {
                      auto pair = evictor_->evict_slabinfo(all_slabs[most_sc_idx]);
                      if (pair.first == CommonErr::OK) {
                        auto& slab_info = pair.second;
                        bool ret = evict_chunk(slab_info.block_idx, slab_info.chunk_idx, slab_info.chunk_ctime_tag);
                        if (ret) {
                          MLOG_INFO("bg_evict_tid evict block idx {} chunk idx {}",
                                    (size_t)slab_info.block_idx, slab_info.chunk_idx);
                          continue_to_run = true;
                        } else {
                          MLOG_DEBUG("bg_evict_tid evict failed for block idx {} chunk idx {}",
                            (size_t)slab_info.block_idx, slab_info.chunk_idx);
                        }
                      }
                    }
                    if (slab_used_num_[most_sc_idx].load(std::memory_order_acquire) == 0) {  // empty slab class
                      empty_slab_class.insert(most_sc_idx);
                    }
                  }
                }
                // clean empty chunk which cooldowns for specific period
                if (first_check) {
                  auto curr_tag = utils::current_microseconds();
                  size_t start_block_idx = (max_memory_block_num_ == 1) ? 0 : 1;
                  for (size_t block_idx = start_block_idx; block_idx < max_memory_block_num_; block_idx++) {
                    for (size_t chunk_idx = 0; chunk_idx < CHUNKS_PER_BLOCK; chunk_idx++) {
                      auto assign_cnt = chunk_assign_cnt_[block_idx][chunk_idx].load(std::memory_order_acquire);
                      if (assign_cnt == 0) {
                        auto ctime_tag = chunk_ctime_tag_[block_idx][chunk_idx].load(std::memory_order_acquire);
                        auto table = chunk_status_[block_idx].load(std::memory_order_acquire);
                        auto status = (table >> (chunk_idx * 4)) & 0x0fULL;
                        if (ctime_tag && (empty_slab_class.count(status) ||
                            curr_tag - ctime_tag > FLAGS_ds_bg_evict_chunk_cooldown_ms * 1000UL)) {
                          if (bg_evict_chk_threadpool_) {
                            bg_evict_chk_threadpool_->add( [block_idx, chunk_idx, ctime_tag, this] {
                              bool ret = evict_chunk(block_idx, chunk_idx, ctime_tag);
                              if (ret) {
                                MLOG_INFO("bg_evict_tp evict block idx {} chunk idx {}", block_idx, chunk_idx);
                              }
                            });
                          } else {
                            bool ret = evict_chunk(block_idx, chunk_idx, ctime_tag);
                            if (ret) {
                              MLOG_INFO("bg_evict_tid evict block idx {} chunk idx {}", block_idx, chunk_idx);
                            }
                          }
                        }
                      }
                    }
                  }
                }
                if (continue_to_run) {
                  first_check = false;
                  continue;
                }
              }
              break;
            }
          }
        }
      });
      pthread_setname_np(bg_evict_chk_thread_->native_handle(), "bg_evict_tid");
    }

    initialized_ = true;
  }
  return 0;
}

void KVCachePool::clean_stale_block(const std::string& shm_path) {
  auto pair = utils::ListDirectoryFiles(shm_path);
  if (pair.first == 0) {
    for (size_t i = 0; i < pair.second.size(); i++) {
      std::string& file_name = pair.second[i];
      if (file_name.find(block_shmname_prefix_) == 0) {
        std::string file_path = shm_path + "/" + file_name;
        auto ret = utils::RemoveFileOrDir(file_path.c_str());
        if (ret.first) {
          MLOG_WARN("clear_stale_block {} return {}", file_name, ret.second);
        }
      }
    }
  } else {
    MLOG_WARN("clear_stale_block listdir {} failed", shm_path);
  }
}

bool KVCachePool::init_block(size_t block_idx) {
  uint64_t expect_value = 0;
  bool ret = chunk_status_[block_idx].compare_exchange_strong(
      expect_value, std::numeric_limits<uint64_t>::max(), std::memory_order_acq_rel);
  if (!ret) {
    MLOG_ERROR("KVCachePool init block {} Chunk table status unexpected, ret: {}", block_idx, ret);
    return false;
  }
  std::string shm_name = fmt::format("{}{}_{:05}", block_shmname_prefix_, getpid(), block_idx);
  MemBlock *mb = new MemBlock(BLOCK_SIZE, shm_name.c_str());
  auto res = allocator_->allocate(mb);
  // expect_value = std::numeric_limits<uint64_t>::max();
  expect_value = ChunkStatus::ALLOCATE;
  for (size_t i = 0; i < CHUNKS_PER_BLOCK - 1; i++) {
    expect_value <<= 4;
    expect_value |= ChunkStatus::ALLOCATE;
  }
  if (res) {
    MLOG_ERROR("KVCachePool init block {} Allocate shm failed: {}, res: {}", block_idx, shm_name, res);
    delete mb;
    chunk_status_[block_idx].compare_exchange_strong(expect_value, 0, std::memory_order_acq_rel);
    return false;
  }
  memset(mb->buf, 0, BLOCK_SIZE);
  block_shm_vec_[block_idx] = mb;
  // uint64_t value = 0xeeeeeeeeeeeeeeeeULL;  // 16 x e
  uint64_t value = ChunkStatus::FREETOUSE;
  for (size_t i = 0; i < CHUNKS_PER_BLOCK - 1; i++) {
    value <<= 4;
    value |= ChunkStatus::FREETOUSE;
  }
  ret = chunk_status_[block_idx].compare_exchange_strong(expect_value, value, std::memory_order_acq_rel);
  MLOG_INFO("KVCachePool init block_idx {} success: {} {}", block_idx, (void *)mb, mb->ToString());
  return ret;
}

bool KVCachePool::init_chunk(size_t block_idx, size_t chunk_idx, SlabClass sc) {
  uint64_t value = chunk_status_[block_idx].load(std::memory_order_acquire);
  size_t offset = chunk_idx * 4;
  uint64_t mask = 0x0fULL << offset;
  uint64_t expect = value;
  uint64_t status = (value >> offset) & 0x0fULL;
  while (status == ChunkStatus::FREETOUSE) {
    status = ChunkStatus::CHANGING;
    value = (expect & ~mask) | (status << offset);
    bool ret =
        chunk_status_[block_idx].compare_exchange_strong(expect, value, std::memory_order_acq_rel, std::memory_order_relaxed);
    if (!ret) {
      expect = chunk_status_[block_idx].load(std::memory_order_acquire);
      status = (expect >> offset) & 0x0fULL;
      continue;
    }
    expect = value;
    MemBlock *mb = block_shm_vec_[block_idx];
    ChunkHeader *chunk_header = (ChunkHeader *)(mb->buf + chunk_idx * CHUNK_SIZE);
    chunk_header->slab_count = slab_count(sc);
    chunk_header->slab_class = sc;
    chunk_header->bitmap_offset = CHUNK_BITMAP_OFFSET;
    chunk_header->data_offset = HEADER_SIZE + align_up(slab_count(sc) * META_SIZE, PAGE_SIZE);
    chunk_header->active_flag = 1;
    chunk_ctime_tag_[block_idx][chunk_idx].store(utils::current_microseconds());
    while (status == ChunkStatus::CHANGING) {
      status = static_cast<uint64_t>(sc);
      value = (expect & ~mask) | (status << offset);
      ret =
          chunk_status_[block_idx].compare_exchange_strong(expect, value, std::memory_order_acq_rel, std::memory_order_relaxed);
      if (!ret) {
        expect = chunk_status_[block_idx].load(std::memory_order_acquire);
        status = (expect >> offset) & 0x0fULL;
        continue;
      }
      const int slab_idx = slab_index(sc);
      slab_allocate_num_[slab_idx].fetch_add(slab_count(sc), std::memory_order_relaxed);
      slab_chunk_num_[slab_idx].fetch_add(1, std::memory_order_acq_rel);
      MLOG_INFO(
          "KVCachePool init chunk for block_idx {} chunk_idx[{}] with slab_class idx {}", block_idx, chunk_idx, slab_idx);
      return true;
    }
    memset((void *)chunk_header, 0, CHUNK_BITMAP_OFFSET);
    chunk_ctime_tag_[block_idx][chunk_idx].store(0);
    break;
  }
  return false;
}

SlabClass KVCachePool::select_slab_class(size_t size) const {
  for (size_t i = 0; i < SlabClassCount; ++i) {
    if (size <= slab_size(all_slabs[i])) {
      return all_slabs[i];
    }
  }
  return SlabClass::SLAB_NONE;
}

bool KVCachePool::allocate(size_t value_len, SlabInfo *slab_info) {
  SlabClass sc = select_slab_class(value_len);
  int sc_idx = slab_index(sc);
  size_t cursor = slab_cursor_[sc_idx].load(std::memory_order_acquire);
  size_t blk_seq = block_seq_idx_.load(std::memory_order_acquire);

  for (size_t i = 0; i < max_memory_block_num_; i++) {  // loop for blocks
    size_t block_idx = (cursor + i) % max_memory_block_num_;
    while (block_idx + pre_alloc_block_num_ > blk_seq && blk_seq < max_memory_block_num_) {
      bool res = block_seq_idx_.compare_exchange_strong(
          blk_seq, blk_seq + 1, std::memory_order_acq_rel, std::memory_order_relaxed);
      if (res) {  // pre alloc block in advance
        std::lock_guard<std::mutex> lock(pre_alloc_blk_thread_mutex_);
        pre_alloc_blk_thread_condv_.notify_all();
        break;
      } else {
        blk_seq = block_seq_idx_.load(std::memory_order_acquire);
      }
    }
    MemBlock *mb = block_shm_vec_[block_idx];
    int slab_idx = -1;
    auto table = chunk_status_[block_idx].load(std::memory_order_acquire);
    for (int j = 0; j < CHUNKS_PER_BLOCK; j++) {  // loop for chunks
      uint64_t status = (table >> (j * 4)) & 0x0fULL;
      size_t beg_us = 0;
      while (status == ChunkStatus::UNINIT || status == ChunkStatus::ALLOCATE) {
        if (beg_us == 0) beg_us = utils::current_microseconds();
        std::this_thread::yield();
        table = chunk_status_[block_idx].load(std::memory_order_acquire);
        status = (table >> (j * 4)) & 0x0fULL;
      }
      if (beg_us != 0) {
        size_t dur_us = utils::delta_microseconds_to_now(beg_us);
        if (dur_us > 10000) {  // 10ms
          MLOG_WARN("KVCachePool allocate for slab_class idx {} block_idx {} chunk_idx {} took {} us",
                     sc_idx, block_idx, j, dur_us);
        }
      }
      if (status == ChunkStatus::FREETOUSE) {
        bool res = init_chunk(block_idx, j, sc);
        if (!res)
          continue;
        else
          status = static_cast<uint64_t>(sc);
      }
      if (status == static_cast<uint64_t>(sc)) {
        mb = block_shm_vec_[block_idx];
        auto bitmap = chunk_bitmap_[block_idx].load(std::memory_order_acquire);
        ChunkHeader *chunk_header = (ChunkHeader *)(mb->buf + j * CHUNK_SIZE);
        bool drain = (bitmap >> j) & uint16_t{0x01};
        size_t ctime_tag = chunk_ctime_tag_[block_idx][j].load(std::memory_order_acquire);
        std::atomic_ref<uint8_t> active_flag(chunk_header->active_flag);
        if (active_flag.load(std::memory_order_acquire) && !drain && ctime_tag) {
          size_t k = 0;
          // bool drain_ready = true;
          for (k = 0; k < slab_count(sc) / 64; k++) {  // loop for slabs
            auto *ptr = reinterpret_cast<uint64_t *>((char *)chunk_header + CHUNK_BITMAP_OFFSET + k * sizeof(uint64_t));
            std::atomic_ref<uint64_t> bits(*ptr);
            while (slab_idx == -1) {
              uint64_t val = bits.load(std::memory_order_acquire);
              uint64_t inv = ~val;
              if (inv == 0)
                break;
              int pos = __builtin_ctzll(inv);
              uint64_t mask = 0x01ULL << pos;
              uint64_t exp = val;
              while ((exp & mask) == 0) {
                val = exp | mask;
                bool res = bits.compare_exchange_strong(exp, val, std::memory_order_acq_rel, std::memory_order_relaxed);
                if (res) {
                  slab_idx = k * 64 + pos;
                  // if (val != UINT64_MAX) drain_ready = false;
                  break;
                } else {
                  exp = bits.load(std::memory_order_acquire);
                }
              }
            }
            if (slab_idx != -1)
              break;
          }
          size_t last_bitwidth = slab_count(sc) & (64ULL - 1);
          if (slab_idx == -1 && last_bitwidth) {  // residual slab
            auto *ptr = reinterpret_cast<uint64_t *>((char *)chunk_header + CHUNK_BITMAP_OFFSET + k * sizeof(uint64_t));
            std::atomic_ref<uint64_t> bits(*ptr);
            while (slab_idx == -1) {
              uint64_t val = bits.load(std::memory_order_acquire);
              uint64_t filter = ~((1ULL << last_bitwidth) - 1);
              val |= filter;
              uint64_t inv = ~val;
              if (inv == 0)
                break;
              int pos = __builtin_ctzll(inv);
              uint64_t mask = 0x01ULL << pos;
              uint64_t exp = val & ~filter;
              while ((exp & mask) == 0) {
                val = exp | mask;
                bool res = bits.compare_exchange_strong(exp, val, std::memory_order_acq_rel, std::memory_order_relaxed);
                if (res) {
                  slab_idx = k * 64 + pos;
                  // if (val != (filter ^ UINT64_MAX)) drain_ready = false;
                  break;
                } else {
                  exp = bits.load(std::memory_order_acquire);
                }
              }
            }
          }
          if (slab_idx != -1) {  // found available slab
            auto table = chunk_status_[block_idx].load(std::memory_order_acquire);
            uint64_t status = (table >> (j * 4)) & 0x0fULL;
            size_t ctime_tag_reload = chunk_ctime_tag_[block_idx][j].load(std::memory_order_acquire);
            if (status == static_cast<uint64_t>(sc) && ctime_tag_reload == ctime_tag) {
              chunk_assign_cnt_[block_idx][j].fetch_add(1, std::memory_order_acq_rel);
              slab_info->slab_class = static_cast<uint8_t>(sc);
              slab_info->block_idx = (uint16_t)block_idx;
              slab_info->block_addr = mb;
              slab_info->chunk_idx = (uint8_t)j;
              slab_info->slab_idx = slab_idx;
              slab_info->chunk_ctime_tag = ctime_tag;
              if (block_idx != cursor) {
                slab_cursor_[sc_idx].compare_exchange_strong(
                    cursor, block_idx, std::memory_order_acq_rel, std::memory_order_relaxed);
              }
              char *header = mb->buf + j * CHUNK_SIZE;
              KVMeta *meta = (KVMeta *)(header + HEADER_SIZE + slab_idx * META_SIZE);
              meta->active_flag = 0;
              meta->value_len = value_len;
              meta->key_ptr = nullptr;
              meta->shard_id = slab_info->shard_id;
              slab_used_num_[sc_idx].fetch_add(1, std::memory_order_acq_rel);
              slab_used_bytes_[sc_idx].fetch_add(value_len, std::memory_order_relaxed);
              return true;
            } else {
              auto *ptr = reinterpret_cast<uint64_t *>((char *)chunk_header + CHUNK_BITMAP_OFFSET + slab_idx / 64 * sizeof(uint64_t));
              std::atomic_ref<uint64_t> bits(*ptr);
              size_t pos = slab_idx & (64ULL - 1);
              size_t val = ~(1ULL << pos);
              bits.fetch_and(val, std::memory_order_acq_rel);
              slab_idx = -1;
              continue;
            }
          } else {  // not found
            uint32_t cnt = chunk_assign_cnt_[block_idx][j].load(std::memory_order_acquire);
            if (cnt == slab_count(sc)) {
              uint16_t exp = uint16_t{0x01} << j;
              chunk_bitmap_[block_idx].fetch_or(exp, std::memory_order_acq_rel);
              cnt = chunk_assign_cnt_[block_idx][j].load(std::memory_order_acquire);  // double check
              if (cnt != slab_count(sc)) {
                exp = ~(uint16_t{0x01} << j);
                chunk_bitmap_[block_idx].fetch_and(exp, std::memory_order_acq_rel);
              }
            }
          }
        }
      }
    }
  }

  // eviction one and replacement
  if (evictor_) {
    auto [ret, evict_info] = evictor_->evict_slab(sc);
    if (ret == CommonErr::OK) {
      slab_info->slab_class = static_cast<uint8_t>(sc);
      slab_info->block_idx = evict_info.block_idx;
      slab_info->block_addr = evict_info.block_addr;
      slab_info->chunk_idx = evict_info.chunk_idx;
      slab_info->slab_idx = evict_info.slab_idx;
      slab_info->chunk_ctime_tag = evict_info.chunk_ctime_tag;
      char *header = slab_info->block_addr->buf + (size_t)slab_info->chunk_idx * CHUNK_SIZE;
      KVMeta *meta = (KVMeta *)(header + HEADER_SIZE + slab_info->slab_idx * META_SIZE);
      if (meta->value_len < value_len) {
        slab_used_bytes_[sc_idx].fetch_add(value_len - meta->value_len, std::memory_order_relaxed);
      } else {
        slab_used_bytes_[sc_idx].fetch_sub(meta->value_len - value_len, std::memory_order_relaxed);
      }
      memset((void *)meta, 0, META_SIZE - sizeof(KVMeta::key) - sizeof(KVMeta::reserved));
      meta->value_len = value_len;
      meta->key_ptr = nullptr;
      meta->shard_id = slab_info->shard_id;
      return true;
    }
  }
  return false;
}

bool KVCachePool::is_active(SlabInfo *slab_info) {
  size_t block_idx = slab_info->block_idx;
  MemBlock *mb = slab_info->block_addr;
  if (mb != block_shm_vec_[block_idx]) {
    MLOG_ERROR("is_active for slab_info not expected");
    return false;
  }
  size_t chunk_idx = slab_info->chunk_idx;
  char *header = mb->buf + chunk_idx * CHUNK_SIZE;
  ChunkHeader *chunk_header = (ChunkHeader *)(header);
  std::atomic_ref<uint8_t> active_flag(chunk_header->active_flag);
  if (active_flag.load(std::memory_order_acquire) != 1) return false;
  auto sc = chunk_header->slab_class;
  if (static_cast<uint8_t>(sc) != slab_info->slab_class) return false;
  auto table = chunk_status_[block_idx].load(std::memory_order_acquire);
  uint64_t mask = 0x0fULL << (chunk_idx * 4);
  uint64_t status = (table & mask) >> (chunk_idx * 4);
  if (static_cast<uint64_t>(sc) != status) return false;
  size_t ctime_tag = chunk_ctime_tag_[block_idx][chunk_idx].load(std::memory_order_acquire);
  if (ctime_tag != slab_info->chunk_ctime_tag) return false;
  return true;
}

bool KVCachePool::free(SlabInfo *slab_info) {
  size_t block_idx = slab_info->block_idx;
  MemBlock *mb = slab_info->block_addr;
  if (mb != block_shm_vec_[block_idx]) {
    MLOG_ERROR("Free for slab_info not expected");
    return false;
  }
  size_t chunk_idx = slab_info->chunk_idx;
  size_t slab_idx = slab_info->slab_idx;
  char *header = mb->buf + chunk_idx * CHUNK_SIZE;
  ChunkHeader *chunk_header = (ChunkHeader *)(header);
  auto sc = chunk_header->slab_class;
  uint64_t *ptr = (uint64_t *)(header + chunk_header->bitmap_offset + slab_idx / 64 * sizeof(uint64_t));
  std::atomic_ref<uint64_t> bits(*ptr);
  int pos = slab_idx & (64ULL - 1);
  char *meta = header + HEADER_SIZE + slab_idx * META_SIZE;
  size_t value_len = ((KVMeta *)meta)->value_len;
  memset(meta, 0, META_SIZE - sizeof(KVMeta::key) - sizeof(KVMeta::reserved));
  slab_used_num_[slab_index(sc)].fetch_sub(1, std::memory_order_acq_rel);
  slab_used_bytes_[slab_index(sc)].fetch_sub(value_len, std::memory_order_relaxed);
  auto cnt = chunk_assign_cnt_[block_idx][chunk_idx].fetch_sub(1, std::memory_order_acq_rel);
  uint64_t val = ~(0x01ULL << pos);
  if (slab_idx / 64 == chunk_header->slab_count / 64) {
    size_t last_bitwidth = chunk_header->slab_count & (64ULL - 1);
    uint64_t filter = ~((1ULL << last_bitwidth) - 1);
    val = ~((0x01ULL << pos) | filter);
  }
  bits.fetch_and(val, std::memory_order_acq_rel);
  if (cnt == slab_count(sc)) {
    chunk_bitmap_[block_idx].fetch_and(~(uint16_t{0x01} << chunk_idx), std::memory_order_acq_rel);
  }
  return true;
}

// Evicts a chunk by atomically marking it, collecting all active slabs, evicting them,
// and resetting the chunk to FREETOUSE state. The process involves: (1) validating and
// atomically clearing the chunk's ctime_tag to prevent concurrent eviction, (2) transitioning
// chunk status from current slab class to CHANGING to block new allocations, (3) collecting
// all active slabs from the chunk bitmap, (4) evicting collected slabs via evictor,
// (5) clearing chunk state and transitioning to FREETOUSE, (6) updating slab class statistics.
bool KVCachePool::evict_chunk(size_t block_idx, size_t chunk_idx, size_t ctime_tag) {
  if (block_idx >= block_seq_idx_.load(std::memory_order_acquire)) return false;
  size_t ctime_expect = ctime_tag, ctime_desired = 0;
  if (ctime_tag == 0 || !chunk_ctime_tag_[block_idx][chunk_idx].compare_exchange_strong(
      ctime_expect, ctime_desired, std::memory_order_acq_rel, std::memory_order_relaxed)) {
    return false;
  }
  // mark chunk unavailable
  MemBlock *mb = block_shm_vec_[block_idx];
  char *header = mb->buf + chunk_idx * CHUNK_SIZE;
  ChunkHeader *chunk_header = (ChunkHeader *)(header);
  std::atomic_ref<uint8_t> active_flag(chunk_header->active_flag);
  if (active_flag.load(std::memory_order_acquire) == 1) {
    SlabClass sc = chunk_header->slab_class;
    auto table = chunk_status_[block_idx].load(std::memory_order_acquire);
    uint64_t mask = 0x0fULL << (chunk_idx * 4);
    uint64_t status = (table & mask) >> (chunk_idx * 4);
    while (status == static_cast<uint64_t>(sc)) {
      uint64_t value = (table & ~mask) | (static_cast<uint64_t>(ChunkStatus::CHANGING) << (chunk_idx * 4));
      bool res =
          chunk_status_[block_idx].compare_exchange_strong(table, value, std::memory_order_acq_rel, std::memory_order_relaxed);
      if (!res) {
        table = chunk_status_[block_idx].load(std::memory_order_acquire);
        status = (table & mask) >> (chunk_idx * 4);
        continue;
      }
      active_flag.store(0, std::memory_order_release);
      status = static_cast<uint64_t>(ChunkStatus::CHANGING);
    }
    if (status != static_cast<uint64_t>(ChunkStatus::CHANGING)) {
      MLOG_ERROR("evict_chunk found block idx {} chunk idx {} status abnormal", block_idx, chunk_idx);
      return false;
    }
    // evict chunk by slabs
    auto *ptr = reinterpret_cast<uint64_t *>((char *)chunk_header + CHUNK_BITMAP_OFFSET);
    size_t slab_cnt_limit = slab_count(sc);
    std::vector<KVMeta*> chunk_kvmeta_vec;
    chunk_kvmeta_vec.reserve(slab_cnt_limit);
    for (size_t slab_idx = 0; slab_idx < slab_cnt_limit; slab_idx++) {
      bool timeout = false;
      auto ts = utils::current_microseconds();
      while (!timeout) {
        std::atomic_ref<uint64_t> bits(*(ptr + slab_idx / 64));
        int pos = slab_idx & (64ULL - 1);
        auto val = bits.load(std::memory_order_acquire);
        val = (val >> pos) & 1ULL;
        if (val == 0) break;
        char* tmp = header + HEADER_SIZE + slab_idx * META_SIZE;
        KVMeta* meta = (KVMeta*)(tmp);
        std::atomic_ref<uint8_t> active_flag(meta->active_flag);
        if (active_flag.load(std::memory_order_acquire) == 0) { // needs timeout if case put or evict failed
          std::this_thread::sleep_for(std::chrono::microseconds(10));
          if (utils::delta_microseconds_to_now(ts) > 3'000'000) {
            timeout = true;
            MLOG_ERROR("background evict for block idx {} and chunk idx {} found stale slab idx {}: {:p}",
                block_idx, chunk_idx, slab_idx, tmp);
          }
          continue;
        }
        chunk_kvmeta_vec.push_back(meta);
        break;
      }
    }
    size_t slab_cnt_used = chunk_kvmeta_vec.size();
    evictor_->evict_slabs(sc, chunk_kvmeta_vec);
    size_t slab_used_bytes = 0;
    for (size_t i = 0; i < slab_cnt_used; i++) {
      auto meta = chunk_kvmeta_vec[i];
      slab_used_bytes += meta->value_len;
    }
    // erase chunk finally
    chunk_bitmap_[block_idx].fetch_and(~(uint16_t{0x01} << chunk_idx), std::memory_order_acq_rel);
    chunk_assign_cnt_[block_idx][chunk_idx].store(0, std::memory_order_release);
    table = chunk_status_[block_idx].load(std::memory_order_acquire);
    status = (table & mask) >> (chunk_idx * 4);
    bool first_touch = true;
    while (status == static_cast<uint64_t>(ChunkStatus::CHANGING)) {
      if (first_touch) memset(header, 0, CHUNK_SIZE);
      uint64_t value = (table & ~mask) | (static_cast<uint64_t>(ChunkStatus::FREETOUSE) << (chunk_idx * 4));
      bool res = chunk_status_[block_idx].compare_exchange_strong(table, value, std::memory_order_acq_rel, std::memory_order_relaxed);
      if (!res) {
        table = chunk_status_[block_idx].load(std::memory_order_acquire);
        status = (table & mask) >> (chunk_idx * 4);
        first_touch = false;
        continue;
      }
      status = static_cast<uint64_t>(ChunkStatus::FREETOUSE);
    }
    if (status != static_cast<uint64_t>(ChunkStatus::FREETOUSE)) {
      MLOG_ERROR("evict_chunk found block idx {} chunk idx {} status abnormal", block_idx, chunk_idx);
      return false;
    }
    slab_used_bytes_[slab_index(sc)].fetch_sub(slab_used_bytes, std::memory_order_relaxed);
    slab_used_num_[slab_index(sc)].fetch_sub(slab_cnt_used, std::memory_order_acq_rel);
    slab_allocate_num_[slab_index(sc)].fetch_sub(slab_cnt_limit, std::memory_order_acq_rel);
    slab_chunk_num_[slab_index(sc)].fetch_sub(1, std::memory_order_acq_rel);
    return true;
  }
  return false;
}

}  // namespace ds
}  // namespace simm
