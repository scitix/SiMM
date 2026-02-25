#pragma once

#include <unistd.h>

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace simm {
namespace utils {

std::string GetUsername();
uint32_t GetUserGID(const std::string &username);
std::vector<gid_t> GetGroupList(const char *username = nullptr);
std::vector<std::string> GetGroupNameList(const char *username = nullptr);
uint64_t GetMapsCount(pid_t pid);
uint64_t GetMaxMapCount();
std::pair<int, std::string> ExecBashCmd(const char* cmd);
int64_t GetMemoryFreeToUse(bool hugepage = false);
}  // namespace utils
}  // namespace simm
