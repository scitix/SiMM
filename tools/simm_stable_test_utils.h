#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace simm::tools::stable_test {

inline std::string BuildStableKey(uint32_t tid, size_t slot, size_t key_len) {
  std::string key = "stable_t" + std::to_string(tid) + "_s" + std::to_string(slot);
  if (key.size() >= key_len) {
    key.resize(key_len);
    return key;
  }
  key.append(key_len - key.size(), 'x');
  return key;
}

inline size_t MinimumUniqueKeyLength(uint32_t max_thread_id, size_t max_slot_id) {
  return ("stable_t" + std::to_string(max_thread_id) + "_s" + std::to_string(max_slot_id)).size();
}

inline size_t StableKeyLengthForSlot(uint32_t tid, size_t slot, size_t min_len, size_t max_len) {
  if (max_len <= min_len) {
    return min_len;
  }
  uint64_t mix = static_cast<uint64_t>(tid) * 0x9E3779B97F4A7C15ULL;
  mix ^= static_cast<uint64_t>(slot) + 0xBF58476D1CE4E5B9ULL + (mix << 6) + (mix >> 2);
  const size_t span = max_len - min_len + 1;
  return min_len + static_cast<size_t>(mix % span);
}

inline std::vector<std::string> BuildWorkerKeyspace(uint32_t tid, size_t keyspace_size, size_t key_len_limit) {
  std::vector<std::string> keys;
  keys.reserve(keyspace_size);
  const size_t min_len = MinimumUniqueKeyLength(tid, keyspace_size == 0 ? 0 : keyspace_size - 1);
  for (size_t slot = 0; slot < keyspace_size; ++slot) {
    const size_t key_len = StableKeyLengthForSlot(tid, slot, min_len, key_len_limit);
    keys.push_back(BuildStableKey(tid, slot, key_len));
  }
  return keys;
}

}  // namespace simm::tools::stable_test
