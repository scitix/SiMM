#pragma once

#include <cstdarg>
#include <cstdint>
#include <string>
#include <vector>

namespace simm {
namespace utils {

std::string FormatString(const char *fmt, ...);

std::string FormatString(const char *fmt, va_list va);

std::vector<std::string> SplitString(const std::string &string_in, const std::string &delim);

std::string charToUtf8(const char *str);

std::string dtypeToStr(const char *dtype);

/**
 * @brief: KB/MB/GB/TB to bytes converter, for example
 *         xxxKB[/kb/Ki/ki] -> return make_pair(0, xxx  << 10)
 *         xxxMB[/mb/Me/me] -> return make_pari(0, xxx  << 20)
 *         xxxGB[/gb/Gi/gi] -> return make_pair(0, xxx  << 30)
 *         xxxTB[/tb/Ti/ti] -> return make_pair(0, xxx  << 40)
 *         xxx              -> return make_pair(0, xxx)
 *         xxxKbytes        -> return make_pair(-1, 0) // Conversion failed
 *
 * @param intput: string number with
 *                (1) KB[/kb/Ki/ki],
 *                (2) MB[/mb/Me/me],
 *                (3) GB[/gb/Gi/gi],
 *                (4) TB[/tb/Ti/ti]
 * @param ret:
 *        int 0 on Success
 *        uint64_t Conversion result (in bytes)
 */
std::pair<int, uint64_t> ToBytes(std::string input);

bool EndsWith(const std::string &str, const std::string &suffix);

bool StartsWith(const std::string &str, const std::string &prefix);

std::string shape_to_string(const std::vector<size_t> &shape, std::string sep = "_");

std::string vector_to_string(const std::vector<std::string> &vec, std::string sep = ",");

bool WildcardMatch(const std::string &str, const std::string &pattern);

std::string strip_string(const std::string &str, bool left = true, char ch = ' ');

bool StringToBool(const std::string &str);

}  // namespace utils
}  // namespace simm
