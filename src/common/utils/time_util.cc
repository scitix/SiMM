#include "common/utils/time_util.h"

#include <sys/select.h>
#include <sys/time.h>

#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

namespace simm {
namespace utils {

uint64_t current_microseconds() {
  struct timeval currentTime;
  gettimeofday(&currentTime, NULL);
  return (unsigned long long)1000000 * currentTime.tv_sec + currentTime.tv_usec;
}

// return current_microseconds() - start
uint64_t delta_microseconds_to_now(uint64_t start) {
  return current_microseconds() - start;
}

std::string TimestampToString(const std::chrono::system_clock::time_point &timestamp) {
  std::time_t time = std::chrono::system_clock::to_time_t(timestamp);
  auto microsec = std::chrono::duration_cast<std::chrono::microseconds>(timestamp.time_since_epoch()) % 1000000;

  std::string timestamp_str = std::to_string(time);
  std::time_t timestamp_i;
  if (timestamp_str.size() == 19) {
    timestamp_i = time / 1e9;
  } else if (timestamp_str.size() == 16) {
    timestamp_i = time / 1e6;
  } else if (timestamp_str.size() == 13) {
    timestamp_i = time / 1e3;
  } else {
    timestamp_i = time;
  }
  std::tm local_tm = *std::localtime(&timestamp_i);

  std::ostringstream oss;
  oss << std::put_time(&local_tm, "%Y-%m-%d_%H:%M:%S");
  oss << "." << std::setfill('0') << std::setw(6) << microsec.count();
  oss << "_" << std::put_time(&local_tm, "%Z");
  return oss.str();
}

std::string GetTimestampString() {
  auto current_time = std::chrono::system_clock::now();
  return TimestampToString(current_time);
}

bool hasTimeoutOccurred(const std::chrono::time_point<std::chrono::system_clock> &start_time,
                        const std::chrono::milliseconds &timeout_duration) {
  std::chrono::time_point<std::chrono::system_clock> current_time = std::chrono::system_clock::now();
  std::chrono::milliseconds elapsed_time =
      std::chrono::duration_cast<std::chrono::milliseconds>(current_time - start_time);
  return elapsed_time >= timeout_duration;
}

std::pair<int, uint64_t> ToMicroseconds(std::string ts) {
  int ret = 0;
  uint64_t val = 0;
  if (ts.length() > 0) {
    ts.erase(0, ts.find_first_not_of(" "));
  }
  if (ts.length() > 0) {
    ts.erase(ts.find_last_not_of(" ") + 1);
  }
  try {
    if (ts.length() >= 3) {
      if (ts.compare(ts.length() - 2, 2, "us") == 0) {
        val = std::stoull(ts.substr(0, ts.length() - 2));
      } else if (ts.compare(ts.length() - 2, 2, "ms") == 0) {
        val = std::stoull(ts.substr(0, ts.length() - 2));
        val = val * 1000UL;
      } else if (ts.compare(ts.length() - 1, 1, "s") == 0) {
        val = std::stoull(ts.substr(0, ts.length() - 1));
        val = val * 1000UL * 1000UL;
      } else if (ts.compare(ts.length() - 1, 1, "m") == 0) {
        val = std::stoull(ts.substr(0, ts.length() - 1));
        val = val * 1000UL * 1000UL * 60UL;
      } else if (ts.compare(ts.length() - 1, 1, "h") == 0) {
        val = std::stoull(ts.substr(0, ts.length() - 1));
        val = val * 1000UL * 1000UL * 60UL * 60UL;
      } else {
        ret = -1;
      }
    } else if (ts.length() >= 2) {
      if (ts.compare(ts.length() - 1, 1, "s") == 0) {
        val = std::stoull(ts.substr(0, ts.length() - 1));
        val = val * 1000UL * 1000UL;
      } else if (ts.compare(ts.length() - 1, 1, "m") == 0) {
        val = std::stoull(ts.substr(0, ts.length() - 1));
        val = val * 1000UL * 1000UL * 60UL;
      } else if (ts.compare(ts.length() - 1, 1, "h") == 0) {
        val = std::stoull(ts.substr(0, ts.length() - 1));
        val = val * 1000UL * 1000UL * 60UL * 60UL;
      } else {
        ret = -1;
      }
    } else if (ts.length() > 0) {
      val = std::stoull(ts);
    } else {
      val = 0;
    }
  } catch (std::exception &ex) {
    ret = -1;
  }
  return std::make_pair(ret, val);
}

}  // namespace utils
}  // namespace simm
