#include <cstddef>
#include <iostream>
#include <tuple>
#include <vector>
#include <string>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "common/base/common_types.h"
#include "common/logging/logging.h"
#include "common/utils/k8s_util.h"
#include "common/utils/string_util.h"

namespace simm {
namespace utils {

TEST(GetHeadlessServiceNamespaceTest, GetHeadlessServiceNamespace) {
  auto service_ns = simm::utils::GetHeadlessServiceNamespace("hhu");
  std::cout << "GetHeadlessServiceNamespace = " << service_ns << std::endl;
}

TEST(GetPodIpsOfHeadlessServiceTest, GetPodIpsOfDataService) {
  auto result = simm::utils::GetPodIpsOfHeadlessService("hhu", "simm-data-svc", "simm-dataserver-port");
  EXPECT_EQ(result.first, 0);
  std::cout << "GetPodIpsOfDataService for ns = hhu"
            << " with total num = " << result.second.size() << ":" << std::endl;
  for (const auto& pod_name_ip : result.second) {
    std::cout << "Pod Name: " << pod_name_ip.first
              << ", Pod IP: " << pod_name_ip.second << std::endl;
  }
  std::cout << std::endl;
}

TEST(GetPodIpsOfHeadlessServiceTest, GetPodIpsOfCMService) {
  auto result = simm::utils::GetPodIpsOfHeadlessService(
      "hhu", "simm-clustermanager-svc", "simm-clusermanager-port");
  EXPECT_EQ(result.first, 0);
  EXPECT_EQ(result.second.size(), 1UL);
  std::cout << "GetPodIpsOfMasterService for ns = hhu"
            << " with total num = " << result.second.size() << ":" << std::endl;
  for (const auto& pod_name_ip : result.second) {
    std::cout << "Pod Name: " << pod_name_ip.first
              << ", Pod IP: " << pod_name_ip.second << std::endl;
  }
  std::cout << std::endl;
}

}   // namespace common
}   // namespace simm


int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
