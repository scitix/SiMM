// For hash function, SiMM at least needs two types:
// 1. 64 bits hash function, used by shard id routing,
//    which alread provided by folly MurmurHash module in folly/hash/MurmurHash.h.
//    However, its version is MurmurHash2, not MurmurHash3.
// 2. 16 bits hash function, used by dataserver to locate key's memory address,
//    which is not provided by folly, so we need to implement it by MurmurHash2_64.
//
// TODO(ytji): Maybe we need one bash hash class and import more high performance hash functions
//             as sub-class, including MurmurHash3, CityHash, crc16 and so on.

#pragma once

#include "folly/hash/MurmurHash.h"

namespace simm {
namespace common {

constexpr std::uint16_t MurmurHash2_16(const char *key, std::size_t len, std::uint16_t seed) noexcept {
  std::uint64_t hash64 = folly::hash::murmurHash64(key, len, seed);
  return static_cast<std::uint16_t>(hash64 ^ (hash64 >> 32));
}

}  // namespace common
}  // namespace simm