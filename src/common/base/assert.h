#pragma once

#include <cassert>
#include <cstdlib>
#include <iostream>

#include "common/logging/logging.h"

/**
 * @brief abort() the process if exp is zero, having written a msg to log.
 * Intended for internal invariants. If the error can be recovered from, without
 * the possibility of corruption, or might best be reflected via an exception in
 * a higher-level, consider returning error code
 *
 */
// current version will throw std::runtime_error under both debug/release modes
#define SIMM_ASSERT(exp, msg, ...)                                             \
  do {                                                                         \
    if (__builtin_expect(!(exp), 0)) {                                         \
      std::string msg_str = simm::logging::vsnprintf_args(msg, ##__VA_ARGS__); \
      LOG_ERROR(msg, ##__VA_ARGS__);                                           \
      throw std::runtime_error(msg_str.c_str());                               \
    }                                                                          \
  } while (0)
