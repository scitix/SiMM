#include <unordered_set>

#include <gtest/gtest.h>

#include "tools/simm_stable_test_utils.h"

namespace simm::tools::stable_test {
namespace {

TEST(SimmStableTestUtils, BuildWorkerKeyspaceIsStableAndUniquePerWorker) {
  const auto keys_a = BuildWorkerKeyspace(7, 128, 64);
  const auto keys_b = BuildWorkerKeyspace(7, 128, 64);

  ASSERT_EQ(keys_a.size(), 128);
  ASSERT_EQ(keys_b.size(), 128);
  EXPECT_EQ(keys_a, keys_b);

  std::unordered_set<std::string> seen;
  for (const auto &key : keys_a) {
    EXPECT_TRUE(seen.insert(key).second);
  }
}

TEST(SimmStableTestUtils, MinimumUniqueKeyLengthPreservesFullLogicalPrefix) {
  const auto min_len = MinimumUniqueKeyLength(31, 199999);
  const auto key = BuildStableKey(31, 199999, min_len);
  EXPECT_EQ(key, "stable_t31_s199999");
}

}  // namespace
}  // namespace simm::tools::stable_test
