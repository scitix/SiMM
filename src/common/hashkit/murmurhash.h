#pragma once

#include "common/hashkit/hashkit.h"

namespace simm {
namespace hashkit {

class HashkitMurmur : public HashkitBase {
 public:
  explicit HashkitMurmur();
  virtual ~HashkitMurmur() override;

  virtual uint64_t generate_64bit_hash_value(const char *key, size_t key_length) override;
  virtual uint16_t generate_16bit_hash_value(const char *key, size_t key_length) override;

  static HashkitBase &Instance() {
    static HashkitMurmur hashkit_murmur;
    return hashkit_murmur;
  }

 private:
  uint16_t seed_{0};
};

}  // namespace hashkit
}  // namespace simm
