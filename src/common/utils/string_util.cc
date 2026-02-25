#include "common/utils/string_util.h"

#include <codecvt>
#include <cstdarg>
#include <cstdio>
#include <locale>
#include <regex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/logging/logging.h"

namespace simm {
namespace utils {

static std::unordered_map<const char *, std::string> kDtypeToStrMap = {{"?", "bool"},
                                                                       // (signed) byte
                                                                       {"b", "int8"},
                                                                       {"b1", "bool"},
                                                                       // unsigned byte
                                                                       {"B", "int8"},
                                                                       // (signed) integer
                                                                       {"i", "int32"},
                                                                       {"i1", "int8"},
                                                                       {"i2", "int16"},
                                                                       {"i4", "int32"},
                                                                       {"i8", "int64"},
                                                                       // unsigned integer
                                                                       {"u1", "uint8"},
                                                                       {"u2", "uint16"},
                                                                       {"u4", "uint32"},
                                                                       {"u8", "uint64"},
                                                                       // floating-point
                                                                       {"f", "float32"},
                                                                       {"f2", "float16"},
                                                                       {"f4", "float32"},
                                                                       {"f8", "float64"},
                                                                       {"f16", "float128"}};

std::string FormatString(const char *fmt, ...) {
  va_list va;
  va_start(va, fmt);
  std::string str = FormatString(fmt, va);
  va_end(va);
  return str;
}

std::string FormatString(const char *fmt, va_list va) {
  va_list copied_args;
  va_copy(copied_args, va);
  char buf[4096];
  int ret = vsnprintf(buf, sizeof(buf), fmt, copied_args);
  va_end(copied_args);
  if (ret < 0) {
    // LOG_ERROR("Fail to vsnprintf into buf = {}, size = {}", (void*)buf,
    //           sizeof(buf));
    return std::string();
  } else if (ret > (int)sizeof(buf)) {
    std::vector<char> str;
    str.resize(ret + 1);
    ret = vsnprintf(str.data(), ret, fmt, va);
    va_end(va);
    return std::string(str.data());
  } else {
    return std::string(buf);
  }
}

std::vector<std::string> SplitString(const std::string &string_in, const std::string &delim) {
  std::regex re(delim);
  return std::vector<std::string>{std::sregex_token_iterator(string_in.begin(), string_in.end(), re, -1),
                                  std::sregex_token_iterator()};
}

std::string charToUtf8(const char *str) {
  std::wstring_convert<std::codecvt_utf8<wchar_t>> converter;
  std::wstring wide_str = converter.from_bytes(str);
  std::string utf8_str = converter.to_bytes(wide_str);
  return utf8_str;
}

std::string dtypeToStr(const char *dtype) {
  return kDtypeToStrMap[dtype];
}

std::pair<int, uint64_t> ToBytes(std::string input) {
  int ret = 0;
  uint64_t val = 0, temp;
  try {
    // Not cover all cases
    if (input.length() >= 3) {
      if ((input.compare(input.length() - 2, 2, "Ti") == 0) || (input.compare(input.length() - 2, 2, "ti") == 0) ||
          (input.compare(input.length() - 2, 2, "TB") == 0) || (input.compare(input.length() - 2, 2, "tb") == 0)) {
        temp = std::stoull(input.substr(0, input.length() - 2));
        val = temp << 40;
      } else if ((input.compare(input.length() - 2, 2, "Gi") == 0) ||
                 (input.compare(input.length() - 2, 2, "gi") == 0) ||
                 (input.compare(input.length() - 2, 2, "GB") == 0) ||
                 (input.compare(input.length() - 2, 2, "gb") == 0)) {
        temp = std::stoull(input.substr(0, input.length() - 2));
        val = temp << 30;
      } else if ((input.compare(input.length() - 2, 2, "Me") == 0) ||
                 (input.compare(input.length() - 2, 2, "me") == 0) ||
                 (input.compare(input.length() - 2, 2, "MB") == 0) ||
                 (input.compare(input.length() - 2, 2, "mb") == 0)) {
        temp = std::stoull(input.substr(0, input.length() - 2));
        val = temp << 20;
      } else if ((input.compare(input.length() - 2, 2, "Ki") == 0) ||
                 (input.compare(input.length() - 2, 2, "ki") == 0) ||
                 (input.compare(input.length() - 2, 2, "KB") == 0) ||
                 (input.compare(input.length() - 2, 2, "kb") == 0)) {
        temp = std::stoull(input.substr(0, input.length() - 2));
        val = temp << 10;
      } else {
        val = std::stoull(input);
      }
    } else if (input.length() > 0) {
      val = std::stoull(input);
    } else {
      val = 0;
    }
  } catch (std::exception &ex) {
    ret = -1;
  }
  return std::make_pair(ret, val);
}

bool EndsWith(const std::string &str, const std::string &suffix) {
  if (str.length() < suffix.length()) {
    return false;
  }
  return str.compare(str.length() - suffix.length(), suffix.length(), suffix) == 0;
}

bool StartsWith(const std::string &str, const std::string &prefix) {
  if (str.length() < prefix.length()) {
    return false;
  }
  return str.compare(0, prefix.length(), prefix) == 0;
}

std::string shape_to_string(const std::vector<size_t> &shape, std::string sep) {
  std::string ret = "";
  ret += std::to_string(shape[0]);
  for (size_t i = 1; i < shape.size(); i++) ret += sep + std::to_string(shape[i]);
  return ret;
}

std::string vector_to_string(const std::vector<std::string> &vec, std::string sep) {
  std::string ret = "";
  if (vec.size() == 0) {
    return ret;
  }
  ret += vec[0];
  for (size_t i = 1; i < vec.size(); i++) {
    ret += sep + vec[i];
  }
  return ret;
}

bool WildcardMatch(const std::string &str, const std::string &pattern) {
  size_t si = 0, pi = 0;
  ssize_t prev_wildcard_idx = -1, next_wildcard_idx = -1;
  ssize_t backtrack_idx = -1;
  while (si < str.length()) {
    if (pi < pattern.length() && (pattern[pi] == '?' || pattern[pi] == str[si])) {
      si++;
      pi++;
    } else if (pi < pattern.length() && pattern[pi] == '*') {
      prev_wildcard_idx = pi;
      next_wildcard_idx = prev_wildcard_idx + 1;
      backtrack_idx = si;
      pi++;
    } else if (prev_wildcard_idx == -1) {
      return false;
    } else {  // backtracking
      si = ++backtrack_idx;
      pi = next_wildcard_idx;
    }
  }
  for (size_t i = pi; i < pattern.length(); i++) {
    if (pattern[i] != '*')
      return false;
  }
  return true;
}

std::string strip_string(const std::string &str, bool left, char ch) {
  std::string ret;
  if (left) {
    for (size_t i = 0; i < str.length(); i++) {
      if (str[i] != ch) {
        ret.push_back(str[i]);
      } else if (ret.length() > 0) {
        return ret + strip_string(str.substr(i), false, ch);
      }
    }
  } else {
    int64_t i = str.length() - 1;
    for (; i >= 0; i--) {
      if (str[i] != ch) {
        break;
      }
    }
    ret = str.substr(0, i + 1);
  }
  return ret;
}

bool StringToBool(const std::string &str) {
  std::string s(str);
  std::transform(s.begin(), s.end(), s.begin(), tolower);
  if (str == "true" || str == "yes" || str == "1")
    return true;
  return false;
}

}  // namespace utils
}  // namespace simm
