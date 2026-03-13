#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <cstddef>
#include <deque>

#include "client/clnt_mempool.h"

#include "common/base/memory.h"
#include "common/errcode/errcode_def.h"

namespace simm {
namespace clnt {

using simm::common::MemBlock;

class MempoolTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto ret = MempoolBase::Instance().Init();
    ASSERT_EQ(ret, CommonErr::OK);
  }

  void TearDown() override {}
};

TEST_F(MempoolTest, TestAllocateRelease) {
  std::deque<std::shared_ptr<MemBlock>> queue;
  const size_t total_count = 80;
  const size_t size1 = (1ULL << 10);
  for (size_t i = 0; i < total_count; i++) {
    auto memp = std::make_shared<MemBlock>(size1);
    auto ret = MempoolBase::Instance().Allocate(memp.get());
    EXPECT_EQ(ret, CommonErr::OK);
    queue.emplace_back(memp);
  }

  const size_t pop_count = 0;
  for (size_t i = 0; i < pop_count; i++) {
    queue.pop_back();
  }
  queue.clear();
}

}  // namespace clnt
}  // namespace simm

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
