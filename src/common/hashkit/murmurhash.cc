#include "common/hashkit/murmurhash.h"

#include <gflags/gflags.h>

#include "folly/hash/MurmurHash.h"

DECLARE_uint32(hash_seed);

namespace simm {
namespace hashkit {

HashkitMurmur::HashkitMurmur(): seed_(static_cast<uint16_t>(FLAGS_hash_seed)) {}

HashkitMurmur::~HashkitMurmur() {}

uint64_t HashkitMurmur::generate_64bit_hash_value(const char *key, size_t key_length) {
  return folly::hash::murmurHash64(key, key_length, seed_);
}

uint16_t HashkitMurmur::generate_16bit_hash_value(const char *key, size_t key_length) {
  std::uint64_t hash64 = folly::hash::murmurHash64(key, key_length, seed_);
  return static_cast<std::uint16_t>(hash64 ^ (hash64 >> 32));
}

}  // namespace hashkit
}  // namespace simm
