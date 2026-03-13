#pragma once

#include <stdint.h>
#include <stdlib.h>

namespace simm {
//////////////////////////////////////////////////////////
// Data Server const varaibles definition
/////////////////////////////////////////////////////////
constexpr size_t BLOCK_SIZE = 1UL << 30;      // 1GB
constexpr size_t CHUNK_SIZE = 1UL << 26;      // 64MB
constexpr size_t HEADER_SIZE = 1UL << 14;     // 16KB
constexpr size_t META_SIZE = 512;             // 512B
constexpr size_t CHUNK_BITMAP_OFFSET = 64;    // 64
constexpr uint8_t CHUNKS_PER_BLOCK = 16;      // 16 chunks per block
constexpr size_t MAX_KEY_SIZE = 256;          // 256B
constexpr size_t MAX_VALUE_SIZE = (1ULL << 26) - (20ULL << 10);  // 64MB - 20KB
constexpr size_t PAGE_SIZE = 4096;                              // 4KB
}  // namespace simm
