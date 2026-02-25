#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <rpc/rpc.h>

#include "simm/simm_common.h"
#include "simm/simm_kv.h"

#include "client/clnt_messenger.h"
#include "common/base/memory.h"

DECLARE_string(cm_primary_node_ip);
DECLARE_int32(cm_inter_port);

namespace simm {
namespace clnt {

constexpr size_t kOneMB = 1 << 20;

using simm::common::MemBlock;

class ClientMessengerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto ret = ClientMessenger::Instance().Init();
    ASSERT_EQ(ret, CommonErr::OK);
  }

  void TearDown() override {}
};

TEST_F(ClientMessengerTest, TestGetServerAddress) {
  std::string key = "test";
  // auto addr = ClientMessenger::Instance().GetServerAddress(key);
  // std::cout << addr << std::endl;
}

}  // namespace clnt
}  // namespace simm

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
