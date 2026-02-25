#pragma once

#include <chrono>
#include <cstdint>
#include <ctime>
#include <string>

namespace simm {
namespace utils {

uint64_t current_microseconds();
uint64_t delta_microseconds_to_now(uint64_t start);
std::string TimestampToString(const std::chrono::system_clock::time_point &timestamp);
std::string GetTimestampString();
bool hasTimeoutOccurred(const std::chrono::time_point<std::chrono::system_clock> &start_time,
                        const std::chrono::milliseconds &timeout_duration);
std::pair<int, uint64_t> ToMicroseconds(std::string ts);
inline uint32_t get_current_seconds() {
    return static_cast<uint32_t>(
        std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count()
    );
}

}  // namespace utils
}  // namespace simm
