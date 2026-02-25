#pragma once

#include <iostream>
#include <string_view>

#include "version/version.h"

namespace simm {
namespace common {

class VersionInfo {
 public:
  static uint16_t getMajorVersion() { return SIMM_VERSION_MAJOR; }

  static uint16_t getMinorVersion() { return SIMM_VERSION_MINOR; }

  static uint16_t getPatchVersion() { return SIMM_VERSION_PATCH; }

  static std::string getGitCommitShortHash() { return SIMM_GIT_COMMIT_SHORT_HASH; }

  static std::string_view getGitCommitFullHash() { return SIMM_GIT_COMMIT_FULL_HASH; }

  static std::string_view getBuildTimeStmap() { return SIMM_BUILD_TIME_STAMP; }

  static std::string_view getBuildType() { return SIMM_BUILD_TYPE; }

  static void printWholeVersionInfo() {
    std::cout << "SiMM Version          : v" << getMajorVersion() << "." << getMinorVersion() << "."
              << getPatchVersion() << std::endl;
    std::cout << "Build Type            : " << getBuildType() << std::endl;
    std::cout << "Build TimeStamp       : " << getBuildTimeStmap() << std::endl;
    std::cout << "Git Commit Short Hash : " << getGitCommitShortHash() << std::endl;
    std::cout << "Git Commit Full Hash  : " << getGitCommitFullHash() << std::endl;
  }
};

}  // namespace common
}  // namespace simm