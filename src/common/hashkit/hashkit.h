#pragma once

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

namespace simm {
namespace hashkit {

enum HashkitType { MURMUR = 1 };

class HashkitBase {
 public:
  virtual ~HashkitBase(){};

  virtual uint64_t generate_64bit_hash_value(const char *key, size_t key_length) = 0;

  virtual uint16_t generate_16bit_hash_value(const char *key, size_t key_length) = 0;

  static HashkitBase &Instance(HashkitType type = HashkitType::MURMUR);
};

}  // namespace hashkit
}  // namespace simm
