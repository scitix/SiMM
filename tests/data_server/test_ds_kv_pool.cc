#include <gtest/gtest.h>
#include <cstddef>
#include <deque>
#include "data_server/kv_cache_pool.h"
#include "data_server/kv_hash_table.h"
#include "data_server/kv_object_pool.h"

#include "folly/logging/xlog.h"

namespace simm {
namespace ds {

class KVPoolTest : public ::testing::Test {
 protected:
  void SetUp() override {
    objPoolPtr.reset(&KVObjectPool::Instance());
    cachePoolPtr = std::make_unique<KVCachePool>();
    int ret = cachePoolPtr->init(cache_bound_bytes, nullptr, nullptr, 1);
    ASSERT_EQ(ret, 0);
  }

  void TearDown() override {
    objPoolPtr.release();
    cachePoolPtr.reset();
  }

  static constexpr size_t cache_bound_bytes = 1ULL << 31;
  static constexpr size_t object_batch_size = 32;
  std::unique_ptr<KVObjectPool> objPoolPtr;
  std::unique_ptr<KVCachePool> cachePoolPtr;
};

TEST_F(KVPoolTest, TestAcquireRelease) {
  std::deque<KVEntryIntrusivePtr> queue;
  const size_t total_count = 80;
  for (size_t i = 0; i < total_count; i++) {
    queue.emplace_back(KVEntry::createIntrusivePtr());
  }
  size_t capacity = total_count;
  size_t inflight = total_count;
  EXPECT_EQ(objPoolPtr->CapacityValue(), capacity);
  EXPECT_EQ(objPoolPtr->InflightValue(), inflight);

  const size_t pop_count = 10;
  for (size_t i = 0; i < pop_count; i++) {
    queue.pop_back();
  }
  EXPECT_EQ(objPoolPtr->CapacityValue(), capacity);
  EXPECT_EQ(objPoolPtr->InflightValue(), total_count - pop_count);
  queue.clear();
  EXPECT_EQ(objPoolPtr->CapacityValue(), capacity);
  EXPECT_EQ(objPoolPtr->InflightValue(), 0);
}

TEST_F(KVPoolTest, TestAllocateFree) {
  size_t size1 = (1ULL << 18) - 1;
  size_t size2 = (1ULL << 18) + 1;
  SlabInfo info1{0}, info2{0};
  bool res;
  res = cachePoolPtr->allocate(size1, &info1);
  EXPECT_EQ(res, true);
  res = cachePoolPtr->allocate(size2, &info2);
  EXPECT_EQ(res, true);
  size_t size3 = size1;
  SlabInfo info3{0};
  res = cachePoolPtr->allocate(size3, &info3);
  EXPECT_EQ(res, true);

  auto sc1 = cachePoolPtr->select_slab_class(size1);
  auto sc2 = cachePoolPtr->select_slab_class(size2);
  size_t idx1 = slab_index(sc1);
  size_t idx2 = slab_index(sc2);
  EXPECT_NE(idx1, idx2);
  size_t cnt;
  cnt = cachePoolPtr->slab_used_num_[idx1].load();
  EXPECT_EQ(cnt, 2);
  cnt = cachePoolPtr->slab_used_num_[idx2].load();
  EXPECT_EQ(cnt, 1);
  cnt = cachePoolPtr->slab_used_bytes_[idx1].load();
  EXPECT_EQ(cnt, 2 * size1);
  cnt = cachePoolPtr->slab_used_bytes_[idx2].load();
  EXPECT_EQ(cnt, 1 * size2);

  res = cachePoolPtr->free(&info1);
  EXPECT_EQ(res, true);
  res = cachePoolPtr->free(&info2);
  EXPECT_EQ(res, true);
  cnt = cachePoolPtr->slab_used_num_[idx1].load();
  EXPECT_EQ(cnt, 1);
  cnt = cachePoolPtr->slab_used_num_[idx2].load();
  EXPECT_EQ(cnt, 0);
  cnt = cachePoolPtr->slab_used_bytes_[idx1].load();
  EXPECT_EQ(cnt, size3);
  cnt = cachePoolPtr->slab_used_bytes_[idx2].load();
  EXPECT_EQ(cnt, 0);

  res = cachePoolPtr->free(&info3);
  EXPECT_EQ(res, true);
}

}  // namespace ds
}  // namespace simm

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
