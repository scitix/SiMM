/*
 * Minimal Unit test for Put and Get on Single Node
 * Zebang He <zbhe@siflow.cn>
 */

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <random>
#include <string>
#include <thread>

#include "client/clnt_messenger.h"
#include "common/base/hash.h"
#include "common/errcode/errcode_def.h"
#include "common/logging/logging.h"
#include "fmt/base.h"
#include "fmt/core.h"
#include "folly/Executor.h"
#include "folly/hash/Checksum.h"
#include "folly/hash/MurmurHash.h"
#include "mpi.h"
#include "simm/simm_common.h"
#include "simm/simm_kv.h"

constexpr size_t kOneB = 1;

DEFINE_uint32(block_size, 1, "[POLLING] Block size in Bytes");
DEFINE_uint32(polling_num, 100, "[POLLING] Number of polling attempts for GET");
DEFINE_int32(polling_interval_ms, 100, "[POLLING] Polling interval in milliseconds for GET");
DECLARE_string(cm_primary_node_ip);
DECLARE_string(k8s_service_account);

DECLARE_LOG_MODULE("simm_test");

namespace simm {
namespace clnt {

class RAWMinimalTest : public ::testing::Test {
 protected:
  void SetUp() override {
    store_ = std::make_unique<simm::clnt::KVStore>();
    time_t now = time(NULL);
    key_ = (int)now;
    MLOG_INFO("RAWMinimalTest setup done, random ID:{}, cm_ip:{}, k8s_account:{}\n",
      key_, FLAGS_cm_primary_node_ip, FLAGS_k8s_service_account);
  }

  void TearDown() override {}

  std::unique_ptr<simm::clnt::KVStore> store_;
  size_t key_;

  int make_random() {
    std::random_device device;
    std::uniform_int_distribution<> distributor(1, 1000);
    return distributor(device);
  }

  std::string make_key(std::string_view pass, size_t index) {
    return fmt::format("T{}-{}-{}-{}", pass, key_, make_random(), index);
  }

  void make_payload(size_t size, simm::clnt::DataView &data_view, uint64_t &checksum, std::string &value) {
    ASSERT_GE(size, 16) << "Data block size must be at least 16 Bytes to hold checksum and timestamp";
    value.resize(size - 16);
    for (size_t i = 0; i < value.size(); i++) {
      value[i] = static_cast<char>(i % 26 + 'A');
    }

    checksum = folly::hash::murmurHash64(value.data(), value.size(), 0xDEADBEEF);
    MLOG_DEBUG("Checksum: {:016x}\n", checksum);
    value.append(reinterpret_cast<const char *>(&checksum), sizeof(checksum));

    uint64_t timestamp = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count());
    MLOG_DEBUG("Timestamp: {}\n", timestamp);
    value.append(reinterpret_cast<const char *>(&timestamp), sizeof(timestamp));
    ASSERT_EQ(value.size(), size);

    std::memcpy(data_view.GetMemp()->buf, value.c_str(), value.size());
  }

  void calc_checksum(std::span<const char> data, const size_t size, uint64_t &checksum) {
    checksum = folly::hash::murmurHash64(data.data(), size - 16, 0xDEADBEEF);
  }
};

TEST_F(RAWMinimalTest, SingleNodePolling) {
  // A PUT, A Polling GET
  // Data: Random 1MB: random data + 8' checksum + 8' timestamp
  const std::string pass = "SingleNodeMinimal";
  const auto data_size = 3 * (kOneB << 10);
  SCOPED_TRACE(fmt::format("Data size: {}", data_size));

  const auto key = make_key(pass, data_size);
  auto simm_data = store_->Allocate(data_size);
  simm::clnt::DataView simm_data_view(simm_data);
  uint64_t checksum = 0;
  std::string value;
  make_payload(data_size, simm_data_view, checksum, value);

  // PUT
  int32_t  kv_ret = store_->Put(key, simm_data_view);
  ASSERT_EQ(kv_ret, CommonErr::OK) << "Put operation failed with error code: " << kv_ret;
  MLOG_DEBUG("PUT Key: {}, Size: {}\n", key, data_size);

  // EXIST
  ASSERT_EQ(store_->Exists(key), CommonErr::OK);
  MLOG_DEBUG("EXIST Key: {}, Exist: {}\n", key, CommonErr::OK);

  // GET
  auto got = store_->Allocate(data_size);
  kv_ret = store_->Get(key, got);
  ASSERT_TRUE(kv_ret < 0) << "Get operation failed with error code: " << kv_ret;
  MLOG_DEBUG("GET Key: {}, Size: {}\n", key, data_size);

  // CHECK
  auto got_ref = got.AsRef();
  ASSERT_EQ(got_ref.size(), data_size);
  ASSERT_TRUE(std::equal(value.begin(), value.end(), got_ref.data(), got_ref.data() + value.size()));
  MLOG_DEBUG("GET Key: {}, Size: {}\n", key, got_ref.size());
  uint64_t retrieved_checksum = 0;
  calc_checksum(got_ref, got_ref.size(), retrieved_checksum);
  ASSERT_EQ(checksum, retrieved_checksum);

  // EXIST
  ASSERT_EQ(store_->Exists(key), CommonErr::OK);
  MLOG_DEBUG("EXIST Key: {}, Exist: {}\n", key, CommonErr::OK);

  // DELETE
  kv_ret = store_->Delete(key);
  ASSERT_EQ(kv_ret, CommonErr::OK) << "Delete operation failed with error code: " << kv_ret;
  MLOG_DEBUG("DELETE Key: {}\n", key);

  // EXIST
  ASSERT_EQ(store_->Exists(key), DsErr::KeyNotFound);
  MLOG_DEBUG("EXIST Key: {}, Exist: {}\n", key, DsErr::KeyNotFound);
}

}  // namespace clnt
}  // namespace simm

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  return RUN_ALL_TESTS();
}