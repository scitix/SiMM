#include "common/utils/sys_util.h"

#include <grp.h>
#include <pwd.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <sstream>
#include <string>
#include <utility>

#include "common/base/assert.h"
#include "common/errcode/errcode_def.h"
#include "common/logging/logging.h"
#include "common/utils/string_util.h"

namespace simm {
namespace utils {

std::string GetUsername() {
  static thread_local std::string s_username;
  if (!s_username.empty())
    return s_username;
  struct passwd pwd;
  struct passwd *result;
  char *buf;
  int64_t bufsize_init = sysconf(_SC_GETPW_R_SIZE_MAX);
  size_t bufsize = bufsize_init == -1 ? 16384 : (size_t)bufsize_init;
  buf = (char *)malloc(bufsize);
  if (buf == NULL) {
    SIMM_ASSERT(false, "Failed to malloc in GetUsername(), strerror = %s", ERR_STR());
  }
  uid_t userid = getuid();
  int ret = getpwuid_r(userid, &pwd, buf, bufsize, &result);
  if (ret == 0 && result != NULL) {
    std::string username = pwd.pw_name;
    free(buf);
    s_username = username;
    return username;
  }
  SIMM_ASSERT(false, "Failed to getpwuid_r in GetUsername(), ret = %d, strerror = %s", ret, ERR_STR());
  // return "";
}

uint32_t GetUserGID(const std::string &username) {
  struct passwd pwd;
  struct passwd *result;
  char *buf;
  int64_t bufsize_init = sysconf(_SC_GETPW_R_SIZE_MAX);
  size_t bufsize = bufsize_init == -1 ? 16384 : (size_t)bufsize_init;
  buf = (char *)malloc(bufsize);
  if (buf == NULL) {
    SIMM_ASSERT(buf != NULL, "Failed to malloc in GetUserGID(), strerror = %s", ERR_STR());
  }
  int ret = getpwnam_r(username.c_str(), &pwd, buf, bufsize, &result);
  if (ret == 0 && result != NULL) {
    uint32_t user_gid = pwd.pw_gid;
    free(buf);
    return user_gid;
  }
  SIMM_ASSERT(false,
              "Failed to getpwnam_r for %s in GetUserGID(), ret = %d, "
              "strerror = %s",
              username.c_str(),
              ret,
              ERR_STR());
}

std::vector<gid_t> GetGroupList(const char *username) {
  std::string uname;
  if (nullptr == username || strlen(username) == 0) {
    uname = GetUsername();
  } else {
    uname = username;
  }
  uint32_t user_gid = GetUserGID(uname);
  int group_count = 0;
  gid_t *groups = (gid_t *)malloc(sizeof(gid_t) * group_count);
  if (groups == nullptr) {
    SIMM_ASSERT(false, "Failed to malloc for getgrouplist for %s: %s", uname.c_str(), ERR_STR());
  }

  int ret = getgrouplist(uname.c_str(), user_gid, groups, &group_count);
  if (ret == -1) {
    free(groups);
    groups = (gid_t *)malloc(sizeof(gid_t) * group_count);
    if (groups == nullptr) {
      SIMM_ASSERT(false, "Failed to malloc2 for getgrouplist for %s: %s", uname.c_str(), ERR_STR());
    }
    ret = getgrouplist(uname.c_str(), user_gid, groups, &group_count);
    if (ret == -1) {
      SIMM_ASSERT(false, "Failed to getgrouplist for %s: %s", uname.c_str(), ERR_STR());
    }
  }

  std::vector<gid_t> group_list(groups, groups + group_count);
  free(groups);
  return group_list;
}

std::vector<std::string> GetGroupNameList(const char *username) {
  std::vector<std::string> groups;
  std::vector<gid_t> gids = GetGroupList(username);
  if (gids.empty()) {
    return groups;
  };

  struct group grp_info;
  struct group *result;
  // int64_t bufsize_init = sysconf(_SC_GETGR_R_SIZE_MAX);
  size_t bufsize = 16384;  // bufsize_init == -1 ? 16384 : (size_t)bufsize_init;
  char *buf = (char *)malloc(bufsize);
  if (buf == NULL) {
    SIMM_ASSERT(buf != NULL, "Failed to malloc in GetGroupNameList() for %s, strerror = %s", username, ERR_STR());
  }
  for (const auto &gid : gids) {
    int ret = getgrgid_r(gid, &grp_info, buf, bufsize, &result);
    if (ret != 0 || result == NULL) {
      SIMM_ASSERT(
          false, "Failed to getgrgid_r for %s's gid = %u, ret = %d, err_msg = %s", username, gid, ret, ERR_STR());
    }
    char *group_name = grp_info.gr_name;
    SIMM_ASSERT(group_name != nullptr, "the group_name of %s's gid %u is empty", username, gid);
    groups.push_back(group_name);
  }
  free(buf);
  return groups;
}

uint64_t GetMapsCount(pid_t pid) {
  std::string maps_file = "/proc/" + std::to_string(pid) + "/maps";
  std::ifstream file(maps_file);

  // Count the number of lines in the file
  uint64_t maps_count = 0;
  std::string line;
  while (std::getline(file, line)) {
    maps_count++;
  }

  return maps_count;
}

uint64_t GetMaxMapCount() {
  std::string max_map_count_str;
  std::ifstream file("/proc/sys/vm/max_map_count");
  if (file.is_open()) {
    std::getline(file, max_map_count_str);
    file.close();
  } else {
    LOG_ERROR("GetMaxMapCount: Open /proc/sys/vm/max_map_count failed");
    return 0;
  }
  return std::stoull(max_map_count_str);
}

std::pair<int, std::string> ExecBashCmd(const char *cmd) {
  int ret = 0;
  std::string result;
  char buffer[4096];
  FILE *pipe = popen(cmd, "r");
  if (!pipe) {
    SIMM_ASSERT(false, "ExecBashCmd popen failed");
  }
  while (fgets(buffer, 4096, pipe) != nullptr) {
    result += std::string(buffer);
  }
  ret = pclose(pipe);
  return std::make_pair(ret, result);
}

/**
 * @brief Get the amount of free memory available for use.
 *
 * /proc/meminfo will be checked, which can be inaccurate for Kubernetes scenarios where the file will give information
 * about the node on which the container runs, but not the memory limit for the container itself.
 */
int64_t GetMemoryFreeToUse(bool hugepage) {
  std::ifstream ins("/proc/meminfo");
  if (ins) {
    std::string line, item;
    size_t num;
    size_t hugepage_size = 2 * 1024 * 1024;  // default hugepage_size=2MB
    [[maybe_unused]] size_t total = 0, free_to_use = 0;
    [[maybe_unused]] size_t hugepage_total, hugepage_free_to_use = 0;
    while (getline(ins, line)) {
      std::stringstream ss(line);
      ss >> item >> num;
      if (item == "MemTotal:") {
        total = num * 1024;  // kB to Byte
	      continue;
      }
      if (item == "MemFree:") {
        free_to_use = num * 1024;  // kB to Byte
	      continue;
      }
      if (item == "MemAvailable:") {
	      continue;
      }
      if (item == "HugePages_Total:") {
        hugepage_total = num;  // pages num
	      continue;
      }
      if (item == "HugePages_Free:") {
        hugepage_free_to_use = num;
	      continue;
      }
      if (item == "Hugepagesize:") {
        hugepage_size = num * 1024;
	      continue;
      }
    }
    if (hugepage) {
      hugepage_total *= hugepage_size;
      hugepage_free_to_use *= hugepage_size;
    }
    ins.close();
    return !hugepage ? free_to_use : hugepage_free_to_use;
  } else {
    return -1;
  }
}
}  // namespace utils
}  // namespace simm