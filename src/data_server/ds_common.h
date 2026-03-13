#pragma once

#include <string.h>
#include <sys/types.h>
#include <cstdint>
#include <string>

namespace simm {
namespace ds {

// alignment calculation
constexpr size_t align_up(size_t size, size_t alignment) {
  return (size + alignment - 1) & ~(alignment - 1);
}

}  // namespace ds
}  // namespace simm