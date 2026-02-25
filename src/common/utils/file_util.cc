#include "common/utils/file_util.h"

#include <acl/libacl.h>
#include <dirent.h>
#include <glob.h>
#include <grp.h>
#include <linux/limits.h>
#include <pwd.h>
#include <sys/acl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/base/assert.h"
#include "common/errcode/errcode_def.h"
#include "common/logging/logging.h"
#include "common/utils/string_util.h"
#include "common/utils/time_util.h"

namespace simm {
namespace utils {

std::string GetFilename(const std::string &path) {
  std::string path_tmp = path;
  if (path_tmp[path_tmp.length() - 1] == '/') {
    path_tmp.pop_back();
  }
  std::string::size_type pos = path_tmp.find_last_of('/') + 1;
  std::string filename = path_tmp.substr(pos, path_tmp.length() - pos);
  std::string filename_without_postfix = filename.substr(0, filename.rfind('.'));
  return filename_without_postfix;
}

std::string GetParentDirname(const std::string &path) {
  std::string path_tmp = path;
  std::string::size_type pos = path_tmp.find_last_of('/') + 1;
  std::string parent_path = path_tmp.substr(0, pos);
  while (parent_path[parent_path.length() - 1] == '/') {
    parent_path.pop_back();
  }
  return GetFilename(parent_path);
}

std::string GetDirPath(const std::string &path) {
  std::string path_tmp = path;
  std::string::size_type pos = path_tmp.find_last_of('/') + 1;
  std::string parent_path = path_tmp.substr(0, pos);
  while (parent_path[parent_path.length() - 1] == '/') {
    parent_path.pop_back();
  }
  return parent_path;
}

bool IsFileReadable(const std::string &file_path) {
  try {
    // attempt to open the file in read mode
    std::ifstream file(file_path);
    return file.good();
  } catch (const std::exception &ex) {
    fprintf(stderr, "IsFileReadable exception %s : %s\n", ex.what(), ERR_STR(errno).data());
    return false;
  }
}

bool IsFilePermissionDenied(const std::string file_path) {
  if (IsFileReadable(file_path))
    return false;
  int ecode = errno;
  return ecode == 13;  // [Errno 13] Permission denied
}

bool IsDirExist(const std::string &dir) {
  struct stat info;
  int ret = stat(dir.c_str(), &info);
  if (ret == 0 && info.st_mode & S_IFDIR) {
    return true;
  }
  return false;
}

bool RemoveDir(const std::string &dir) {
  try {
    auto path = std::filesystem::path(dir);
    bool ret = std::filesystem::remove(path);
    return ret;
  } catch (const std::exception &ex) {
    fprintf(stderr, "RemoveDir exception %s : %s\n", ex.what(), ERR_STR(errno).data());
    return false;
  }
}

std::pair<int, std::vector<std::string>> FindFilesByWildcard(const std::string &wildcard) {
  glob_t glob_result;
  std::vector<std::string> filenames;
  memset(&glob_result, 0, sizeof(glob_result));

  int ret = glob(wildcard.c_str(), GLOB_TILDE, NULL, &glob_result);
  if (ret == 0) {
    for (size_t i = 0; i < glob_result.gl_pathc; ++i) {
      filenames.push_back(std::string(glob_result.gl_pathv[i]));
    }
  } else {
    ret = -1;
  }
  return std::make_pair(ret, filenames);
}

std::pair<int, std::vector<std::string>> TraverseDirectoryFiles(const std::string &path) {
  try {
    std::filesystem::path dir_path(path);
    std::vector<std::string> files;
    int ret = -1;
    // check if the dir exists
    if (std::filesystem::exists(dir_path) && std::filesystem::is_directory(dir_path)) {
      // Iterate over the files in the dir
      for (const auto &entry : std::filesystem::directory_iterator(dir_path)) {
        if (std::filesystem::is_regular_file(entry.path())) {
          files.push_back(entry.path());
        }
      }
      ret = 0;
    } else {
      ret = -1;
    }
    return std::make_pair(ret, files);
  } catch (const std::exception &ex) {
    fprintf(stderr, "TraverseDirectoryFiles exception %s : %s\n", ex.what(), ERR_STR(errno).data());
    return std::make_pair<int, std::vector<std::string>>(-1, {});
  }
}

std::pair<int, std::vector<std::string>> TraverseDirectoryDirs(const std::string &path) {
  try {
    std::filesystem::path dir_path(path);
    std::vector<std::string> dirs;
    int ret = -1;
    // check if the dir exists
    if (std::filesystem::exists(dir_path) && std::filesystem::is_directory(dir_path)) {
      // Iterate over the dirs in the dir
      for (const auto &entry : std::filesystem::directory_iterator(dir_path)) {
        if (std::filesystem::is_directory(entry.path())) {
          dirs.push_back(entry.path());
        }
      }
      ret = 0;
    } else {
      ret = -1;
    }
    return std::make_pair(ret, dirs);
  } catch (const std::exception &ex) {
    fprintf(stderr, "TraverseDirectoryDirs exception %s : %s\n", ex.what(), ERR_STR(errno).data());
    return std::make_pair<int, std::vector<std::string>>(-1, {});
  }
}

std::string GetFileCreatedTime(const char *file_path) {
  struct stat fileStat;
  if (stat(file_path, &fileStat) == 0) {
    std::time_t create_time_t = fileStat.st_ctime;
    auto time_info = std::localtime(&create_time_t);
    char buffer[80];
    std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", time_info);
    std::string create_time(buffer);
    return create_time;
  } else {
    std::cerr << "Failed to retrivee file information for " << file_path << std::endl;
    return "";
  }
}

size_t GetFileCreatedTimestamp(const char *file_path) {
  struct stat fileStat;
  size_t ret = 0;
  if (stat(file_path, &fileStat) == 0) {
    std::time_t create_time_t = fileStat.st_ctime;
    ret = (size_t)create_time_t * 1000L * 1000L * 1000L;
    return ret;
  } else {
    std::cerr << "Failed to retrivee file information for " << file_path << std::endl;
    return 0;
  }
}

long GetFileModifiedTimeNanosec(const char *file_path) {
  long ret = 0;
  struct stat file_info;
  ret = stat(file_path, &file_info);
  if (ret != 0) {
    return ret;
  }
  ret = (long)file_info.st_mtime;
  ret *= 1000L * 1000L * 1000L;  // nanosec format
#ifdef _STATBUF_ST_NSEC
  ret += file_info.st_mtim.tv_nsec;
#endif
  return ret;
}

std::string GetAbsolutePath(const char *path) {
  std::string fs_path;
  if (path[0] == '/') {
    fs_path = path;
  } else {
    char cwd[PATH_MAX];
    auto cwd_ptr = getcwd(cwd, sizeof(cwd));
    if (!cwd_ptr) {
      fprintf(stderr, "getcwd error %s\n", path);
      return std::string(path);
    }
    fs_path = std::string(cwd_ptr) + "/" + std::string(path);
  }
  return fs_path;
}

std::pair<int, std::string> RemoveFileOrDir(const char *path) {
  struct stat infos;
  int ret = stat(path, &infos);
  std::string err_msg = "";
  if (ret != 0) {
    ret = -1;
    err_msg = FormatString("Fail to Stat %s: %s", path, ERR_STR(errno));
    return std::make_pair(ret, err_msg);
  }
  if (infos.st_mode & S_IFREG) {
    ret = remove(path);
    if (ret != 0) {
      err_msg = FormatString("Fail to Remove %s: %s", path, ERR_STR(errno));
    }
  } else if (infos.st_mode & S_IFDIR) {
    auto result = TraverseDirectoryFiles(path);
    if (result.first != 0) {
      ret = result.first;
      err_msg = FormatString("Fail to TraverseDirectoryFiles %s: ", path);
    } else {
      for (auto &entry : result.second) {
        ret = remove(entry.c_str());
        if (ret != 0) {
          err_msg += FormatString("Fail to Remove %s: %s ", path, ERR_STR(errno));
        }
      }
    }
  }
  return std::make_pair(ret, err_msg);
}

/**
 * @brief check if the file or directory has ACL_MASK.
 *
 * @param path
 * @return If the ACL has no ACL_MASK, return false. Otherwise, return true
 */
bool HasAclMask(const std::string &path) {
  acl_t acl = acl_get_file(path.c_str(), ACL_TYPE_ACCESS);
  if (acl == nullptr) {
    LOG_ERROR("Failed to acl_get_file for {}: {}", path, ERR_STR(errno));
    return false;
  }
  int entry_id = ACL_FIRST_ENTRY;
  acl_entry_t entry;
  while (acl_get_entry(acl, entry_id, &entry) == 1) {
    acl_tag_t tag;
    acl_get_tag_type(entry, &tag);
    if (tag == ACL_MASK) {
      acl_free(acl);
      return true;
    }
    entry_id = ACL_NEXT_ENTRY;
  }
  acl_free(acl);
  return false;
}

bool HasSetUserAcl(const std::string &path, const std::string &username) {
  acl_t acl = acl_get_file(path.c_str(), ACL_TYPE_ACCESS);
  if (acl == nullptr) {
    LOG_ERROR("Failed to acl_get_file for {}: {}", path, ERR_STR(errno));
    return false;
  }
  int entry_id = ACL_FIRST_ENTRY;
  acl_entry_t entry;
  while (acl_get_entry(acl, entry_id, &entry) == 1) {
    acl_permset_t permset;
    acl_get_permset(entry, &permset);
    acl_tag_t tag;
    acl_get_tag_type(entry, &tag);
    if (tag == ACL_USER) {
      uid_t *entry_user_id = (uid_t *)acl_get_qualifier(entry);
      if (entry_user_id) {
        struct passwd *pwd = getpwuid(*entry_user_id);
        if (pwd && std::string(pwd->pw_name) == username) {
          acl_free(acl);
          LOG_DEBUG("User %s has been set ACL permission for the dir = %s.", username.c_str(), path.c_str());
          return true;
        }
      }
    }
    entry_id = ACL_NEXT_ENTRY;
  }

  acl_free(acl);
  return false;
}

std::string GetPathGroupName(const std::string &path) {
  struct stat file_info;
  int ret = stat(path.c_str(), &file_info);
  SIMM_ASSERT(ret == 0, "Failed to stat %s, err_msg = %s", path.c_str(), ERR_STR(errno));
  struct group grp_info;
  struct group *result;
  // int64_t bufsize_init = sysconf(_SC_GETGR_R_SIZE_MAX);
  size_t bufsize = 16384;  // bufsize_init == -1 ? 16384 : (size_t)bufsize_init;
  char *buf = (char *)malloc(bufsize);
  if (buf == NULL) {
    SIMM_ASSERT(false, "Failed to malloc in GetPathGroupName() for %s, ERR_STR = %s", path.c_str(), ERR_STR(errno));
  }
  ret = getgrgid_r(file_info.st_gid, &grp_info, buf, bufsize, &result);
  if (ret != 0 || result == NULL) {
    free(buf);
    SIMM_ASSERT(false,
                "Failed to getgrgid_r for %s's gid = %u, ret = %d, err_msg = %s",
                path.c_str(),
                file_info.st_gid,
                ret,
                ERR_STR(errno));
  }
  char *group_name = grp_info.gr_name;
  SIMM_ASSERT(group_name != nullptr, "the group_name of %s's gid %u is empty", path.c_str(), file_info.st_gid);
  free(buf);
  return group_name;
}

bool HasUserAclPermission(const std::string &path, const std::string &username, acl_perm_t perm) {
  std::string perm_str;
  if (perm == ACL_READ) {
    perm_str = "read";
  } else if (perm == ACL_WRITE) {
    perm_str = "write";
  } else if (perm == ACL_EXECUTE) {
    perm_str = "execute";
  } else {
    perm_str = "unknown";
  }
  acl_t acl = acl_get_file(path.c_str(), ACL_TYPE_ACCESS);
  if (acl == NULL) {
    LOG_ERROR("Failed to acl_get_file for {}: {}", path, ERR_STR(errno));
    return false;
  }
  int entry_id = ACL_FIRST_ENTRY;
  bool has_permission = false;
  acl_entry_t entry;
  while (acl_get_entry(acl, entry_id, &entry) == 1) {
    acl_permset_t permset;
    acl_get_permset(entry, &permset);
    acl_tag_t tag;
    acl_get_tag_type(entry, &tag);
    if (tag == ACL_USER && acl_get_perm(permset, perm) == 1) {
      uid_t *entry_user_id = (uid_t *)acl_get_qualifier(entry);
      if (entry_user_id) {
        struct passwd *pwd = getpwuid(*entry_user_id);
        if (pwd && std::string(pwd->pw_name) == username) {
          // acl_free(acl);
          LOG_DEBUG(
              "User %s has %s permission for the dir = %s (ACL).", username.c_str(), perm_str.c_str(), path.c_str());
          has_permission = true;
        }
      }
    }
    if (tag == ACL_MASK && acl_get_perm(permset, perm) != 1) {
      has_permission = false;
      break;
    }
    entry_id = ACL_NEXT_ENTRY;
  }
  acl_free(acl);
  LOG_DEBUG(
      "User %s does not have %s permission for the  dir = %s "
      "(ACL).",
      username.c_str(),
      perm_str.c_str(),
      path.c_str());
  return has_permission;
}

acl_entry_t find_entry(acl_t acl, acl_tag_t type, uid_t uid) {
  acl_entry_t ent;
  acl_tag_t e_type;
  uid_t *e_uid_p;
  if (acl_get_entry(acl, ACL_FIRST_ENTRY, &ent) != 1) {
    return NULL;
  }
  for (;;) {
    acl_get_tag_type(ent, &e_type);
    if (type == e_type) {
      if (uid != ACL_UNDEFINED_ID) {
        e_uid_p = (uid_t *)acl_get_qualifier(ent);
        if (e_uid_p == NULL) {
          if (acl_get_entry(acl, ACL_NEXT_ENTRY, &ent) != 1) {
            return NULL;
          }
          continue;
        }
        if (*e_uid_p == uid) {
          acl_free(e_uid_p);
          return ent;
        }
        acl_free(e_uid_p);
      } else {
        return ent;
      }
    }
    if (acl_get_entry(acl, ACL_NEXT_ENTRY, &ent) != 1) {
      return NULL;
    }
  }
}

bool IsSymbolicLink(const std::string &path) {
  struct stat file_stat;
  if (lstat(path.c_str(), &file_stat) == 0) {
    if (S_ISLNK(file_stat.st_mode)) {
      return true;
    }
    return false;
  }
  LOG_ERROR("failed to stat {}: {}", path, ERR_STR(errno));
  return false;
}

std::string GetSymLinkTarget(const std::string &path) {
  char res[PATH_MAX];
  auto len = readlink(path.c_str(), res, PATH_MAX - 1);
  if (len == -1) {
    return path;
  }
  res[len] = '\0';
  return std::string(res);
}

std::string LexicallyNormal(const std::string &path) {
  if (path.empty())
    return path;
  std::vector<std::string> dirs;
  bool first_slash = path.front() == '/';
  size_t last_slash_pos = 0, pos = 0;
  if (first_slash) {
    for (pos = 1; pos < path.length(); pos++) {
      if (path[pos] == '/') {
        if (pos > last_slash_pos + 1) {
          std::string dir = path.substr(last_slash_pos + 1, pos - last_slash_pos - 1);
          dirs.push_back(dir);
        }
        last_slash_pos = pos;
        continue;
      }
    }
    if (pos > last_slash_pos + 1) {
      std::string dir = path.substr(last_slash_pos + 1, pos - last_slash_pos - 1);
      dirs.push_back(dir);
    }
  } else {
    auto p = path.find('/');
    if (p == std::string::npos) {
      dirs.push_back(path);
    } else {
      std::string dir = path.substr(0, p);
      dirs.push_back(dir);
      last_slash_pos = p;
      for (pos = p + 1; pos < path.length(); pos++) {
        if (path[pos] == '/') {
          if (pos > last_slash_pos + 1) {
            std::string dir = path.substr(last_slash_pos + 1, pos - last_slash_pos - 1);
            dirs.push_back(dir);
          }
          last_slash_pos = pos;
          continue;
        }
      }
      if (pos > last_slash_pos + 1) {
        std::string dir = path.substr(last_slash_pos + 1, pos - last_slash_pos - 1);
        dirs.push_back(dir);
      }
    }
  }
  std::vector<std::string> abs_dirs;
  for (size_t i = 0; i < dirs.size(); i++) {
    if (dirs[i] == ".") {
      continue;
    }
    if (dirs[i] == "..") {
      if (!abs_dirs.empty() && abs_dirs.back() != "..")
        abs_dirs.pop_back();
      else
        abs_dirs.push_back(dirs[i]);
      continue;
    }
    abs_dirs.push_back(dirs[i]);
  }
  std::string res = first_slash ? "/" : "";
  for (size_t i = 0; i < abs_dirs.size(); i++) {
    res.append(abs_dirs[i]);
    if (i != abs_dirs.size() - 1)
      res.append("/");
  }
  return res;
}

std::string FormatAbsolutePath(const std::string &path) {
  std::string ret = path;
  std::vector<std::string> items = SplitString(ret, "/");
  ret = "";
  for (auto &item : items) {
    if (item.size()) {
      ret += "/" + item;
    }
  }
  return ret;
}

std::string GetParrentPath(const std::string &path) {
  std::string path_tmp = FormatAbsolutePath(path);
  std::string::size_type pos = path_tmp.find_last_of('/');
  std::string parrent_path = path_tmp.substr(0, pos);
  return parrent_path;
}

std::vector<std::string> GetUniqDirpathes(const std::vector<std::string> &dirs) {
  std::vector<std::string> ret;
  std::unordered_set<std::string> prefixes;
  std::vector<std::string> tmp = dirs;
  std::sort(tmp.begin(), tmp.end(), [](std::string &a, std::string &b) { return a.size() > b.size(); });
  for (std::string path : tmp) {
    path = FormatAbsolutePath(path);
    while (prefixes.find(path) == prefixes.end()) {
      prefixes.insert(path);
      ret.push_back(path);
      path = GetParrentPath(path);
      if (std::count(path.begin(), path.end(), '/') <= 1)
        break;
    }
  }
  return ret;
}

ssize_t GetFileSize(const char *file_path) {
  ssize_t file_size = -1;
  struct stat file_info;
  int ret = stat(file_path, &file_info);
  if (ret == 0) {
    file_size = file_info.st_size;
  }
  return file_size;
}

std::pair<int, std::vector<std::string>> ListDirectoryFiles(const std::string &path) {
  int ret = -1;
  std::vector<std::string> files;
  DIR *dir = opendir(path.c_str());
  if (dir == nullptr) {
    LOG_ERROR("ListDirectoryFiles open dir {} failed: {}", path, ERR_STR(errno));
    return std::make_pair(ret, files);
  }
  struct dirent *ent;
  while (true) {
    ent = readdir(dir);
    if (ent == nullptr)
      break;
    if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) {
      continue;
    }
    bool regular_file = false;
    if (ent->d_type != DT_UNKNOWN) {
      regular_file = ent->d_type == DT_REG;
    } else {
      std::string abspath = path + "/" + ent->d_name;
      struct stat st;
      if (stat(abspath.c_str(), &st) == 0) {
        regular_file = S_ISREG(st.st_mode);
      }
    }
    if (regular_file)
      files.push_back(ent->d_name);
  }
  closedir(dir);
  return std::make_pair(0, files);
}

}  // namespace utils
}  // namespace simm
