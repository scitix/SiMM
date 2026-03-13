#include <string>
#include <cstring>
#include <gtest/gtest.h>

#include "common/base/hash.h"

TEST(HashFuncTest, MurmurHashFuncTests) {
    const char * key1 = "hello-world";
    const char * key2 = "SiMM";
    EXPECT_EQ(folly::hash::murmurHash64(key1, std::strlen(key1), 0), 6439493127771207189);
    EXPECT_EQ(folly::hash::murmurHash64(key2, std::strlen(key2), 0), 5261682058871404995);

    EXPECT_EQ(simm::common::MurmurHash2_16(key1, std::strlen(key1), 0), 39668);
    EXPECT_EQ(simm::common::MurmurHash2_16(key2, std::strlen(key2), 0), 17975);
}
