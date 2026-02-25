#pragma once

#include <sys/acl.h>

#include <map>
#include <string>
#include <vector>

namespace simm {
namespace utils {

enum file_permission_type : int { ANY = 0, GROUP_OR_OTHER, USER, GROUP, OTHER };

std::string GetFilename(const std::string &path);
std::string GetParentDirname(const std::string &path);
std::string GetDirPath(const std::string &path);
bool IsFileReadable(const std::string &file_path);
bool IsFilePermissionDenied(const std::string file_path);
bool IsDirExist(const std::string &dir);
std::pair<int, std::vector<std::string>> FindFilesByWildcard(const std::string &wildcard);
std::pair<int, std::vector<std::string>> TraverseDirectoryFiles(const std::string &path);
std::pair<int, std::vector<std::string>> TraverseDirectoryDirs(const std::string &path);
std::string GetFileCreatedTime(const char *file_path);
size_t GetFileCreatedTimestamp(const char *file_path);
long GetFileModifiedTimeNanosec(const char *file_path);
std::string GetAbsolutePath(const char *path);
std::pair<int, std::string> RemoveFileOrDir(const char *path);
bool RemoveDir(const std::string &dir);
/**
 * @brief setfacl to a path for a specified user.
 *
 * @param path            the path name to setfacl.
 * @param username        the username to setfacl.
 * @param read_enable     0 means disable readable permission when force==1, and
 * rollback user readable permission to gourps's or others's permission when
 * force==0; 1 means enable user readable permission.
 * @param write_enable    0 means disable readable permission when force==1, and
 * rollback user writable permission to gourps's or others's permission when
 * force==0; 1 means enable writable permission.
 * @param execute_enble   0 means disable readable permission when force==1, and
 * rollback user executable permission to gourps's or others's permission when
 * force==0; 1 means enable executable permission.
 * @param force   0 means rollback user permission to
 * gourps's or others's permission; 1 means disable permission.
 * @return std::pair<int, std::string>  Returns an ErrorCode and error msg.
 *
 */
std::pair<int, std::string> setFileACLPerm(const std::string &path,
                                           const std::string &username,
                                           bool read_enable,
                                           bool write_enable,
                                           bool execute_enble,
                                           bool force = 0);
std::pair<int, std::string> setDirACLPermRecurse(const std::string &dir_path,
                                                 const std::string &username,
                                                 bool read_enable,
                                                 bool write_enable,
                                                 bool execute_enble);
/**
 * @brief enable user executable facl to a path for a specified user. This
 * function will setfacl recursively across the directory. if the path is a
 * file, it will set the facl starting from its parent directory.
 *
 * @param path            the path name to setfacl.
 * @param username        the username to setfacl.
 * @return std::pair<int, std::string>  Returns an ErrorCode and error msg.
 * @note
 *         1. If the user already have relative permission (e.g. user'group or
 * others has x permission), this function will not change the permission.
 *
 */
std::pair<int, std::string> EnableDirACLUserExecPermRecurse(const std::string &dir_path, const std::string &username);
std::string GetLoggerPath(const std::string &filename);
bool IsSymbolicLink(const std::string &path);
bool IsRealPath(const std::string &path);
std::string GetSymLinkTarget(const std::string &path);
std::string LexicallyNormal(const std::string &path);
std::string GetRealPathOnFileset(const std::string &path,
                                 const std::string &fs_prefix,
                                 const std::string &group,
                                 std::map<std::string, std::string> &symlink_map);
std::string GetRealPathIfSymbolicLink(const std::string &path, const std::string &group = "");
std::string FormatAbsolutePath(const std::string &path);
std::string GetParrentPath(const std::string &path);
// must use absolute path
std::pair<int, std::vector<std::string>> GetUnauthorizedFiles(const std::string &path, const std::string &username);
bool HasAclMask(const std::string &path);
bool HasSetUserAcl(const std::string &path, const std::string &username);
bool HasUserAclPermission(const std::string &path, const std::string &username, acl_perm_t perm);

ssize_t GetFileSize(const char *file_path);
std::pair<int, std::vector<std::string>> ListDirectoryFiles(const std::string &path);

}  // namespace utils
}  // namespace simm
