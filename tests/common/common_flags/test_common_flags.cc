#include <gflags/gflags.h>
#include <gtest/gtest.h>

DECLARE_string(product_name);
DECLARE_string(product_version);
DECLARE_bool(buildinfo);

TEST(ErrcodeCommonFlagsTest, CommonFlags) {
    EXPECT_EQ(FLAGS_product_name, "SiMM");
    EXPECT_EQ(FLAGS_product_version, "v0.2.0");
    EXPECT_TRUE(!FLAGS_buildinfo);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
