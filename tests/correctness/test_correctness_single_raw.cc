/*
 * Unit test for Read_After_Write on Single Node / Multi Nodes
 * Zebang He <zbhe@siflow.cn>
 */

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <latch>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "boost/progress.hpp"
#include "client/clnt_messenger.h"
#include "common/base/hash.h"
#include "common/errcode/errcode_def.h"
#include "common/logging/logging.h"
#include "fmt/base.h"
#include "fmt/core.h"
#include "folly/Executor.h"
#include "folly/Fingerprint.h"
#include "folly/hash/Checksum.h"
#include "folly/hash/MurmurHash.h"
#include "folly/init/Init.h"
#include "mpi.h"
#include "simm/simm_common.h"
#include "simm/simm_kv.h"

constexpr size_t kOneMB = 1 << 20;
constexpr size_t kOneKB = 1 << 10;
constexpr size_t kOneB = 1;

constexpr auto keyLengths = {16 * kOneB, 32 * kOneB, 64 * kOneB, 128 * kOneB, 256 * kOneB};
constexpr auto dataSizes = {16 * kOneB,
                            100 * kOneB,
                            500 * kOneB,
                            1 * kOneKB,
                            1900 * kOneB,
                            2200 * kOneB,
                            4 * kOneKB,
                            8700 * kOneB,
                            130 * kOneKB,
                            256 * kOneKB,
                            440 * kOneKB,
                            1 * kOneMB,
                            1500 * kOneKB,
                            2 * kOneMB,
                            3 * kOneMB};

DEFINE_string(config, "", "Configuration file path (TBD)");
DEFINE_bool(verbose, false, "Enable verbose logging (TBD)");
DEFINE_int32(polling_interval_ms, 100, "[POLLING] Polling interval in milliseconds for GET");
DEFINE_uint32(polling_num, 50'000, "[POLLING] Number of polling for GET pressure test");
DEFINE_uint32(polling_retries, 1000, "[POLLING] Number of retries for GET polling when async / multi-threaded");
DEFINE_int32(startup_wait_ms, 50, "[POLLING] Wait time in milliseconds for system startup");
DEFINE_uint32(block_size, 3 * kOneMB, "[POLLING] Block size in Bytes");
DEFINE_uint32(key_num, 50'000, "[POLLING] Number of different keys to test");
// DEFINE_uint32(thread_num, 3000, "[POLLING] Threads number for multi-threaded GET");
DECLARE_string(cm_primary_node_ip);
DECLARE_string(k8s_service_account);

DECLARE_LOG_MODULE("simm_test");

bool g_mpi_initialized = false;
bool g_mpi_finalized = false;
int g_mpi_world_rank = -1;
int g_mpi_world_size = -1;

namespace simm {
namespace clnt {

class RAWCorrectionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    store_ = std::make_unique<simm::clnt::KVStore>();
    if (!store_) {
      FAIL() << "Store is not initialized";
    }
    time_t now = time(NULL);
    key_ = (int)now;
    MLOG_INFO("RAWCorrectionTest setup done, random ID:{}, cm_ip:{}, k8s_account:{}\n",
      key_, FLAGS_cm_primary_node_ip, FLAGS_k8s_service_account)
  }

  void TearDown() override {}

  std::unique_ptr<simm::clnt::KVStore> store_;
  size_t key_;

  std::string make_random(size_t length = 4) {
    const char charset[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    std::string result;
    result.resize(length);
    std::random_device device;
    std::uniform_int_distribution<> distributor(0, sizeof(charset) - 2);
    for (size_t i = 0; i < length; i++) {
      result[i] = charset[distributor(device)];
    }
    return result;
  }

  std::string make_key(std::string_view pass, size_t index, size_t length = 0) {
    if (length == 0) {
      return fmt::format("T{}-{}-{}-{}", pass, index, key_, make_random());
    } else {
      return make_random(length);
    }
  }

  void calc_checksum(std::span<const char> data, const size_t size, uint64_t &checksum) {
    checksum = folly::hash::murmurHash64(data.data(), size - 16, 0xDEADBEEF);
  }

  void make_payload(size_t size, simm::clnt::DataView &data_view, uint64_t &checksum, std::string &value) {
    ASSERT_GE(size, 16) << "Data block size must be at least 16 Bytes to hold checksum and timestamp";
    value.resize(size - 16);
    for (size_t i = 0; i < value.size(); i++) {
      value[i] = static_cast<char>(i % 26 + 'A');
    }

    calc_checksum(value, size, checksum);
    value.append(reinterpret_cast<const char *>(&checksum), sizeof(checksum));

    uint64_t timestamp = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count());
    MLOG_DEBUG("Timestamp: {}\n", timestamp);
    value.append(reinterpret_cast<const char *>(&timestamp), sizeof(timestamp));
    ASSERT_EQ(value.size(), size);

    std::memcpy(data_view.GetMemp()->buf, value.c_str(), value.size());
  }
};

class SingleNodeRAWCorrectionTest : public RAWCorrectionTest {
 protected:
  void SetUp() override {
    if (g_mpi_world_size > 1 && g_mpi_world_rank != 0) {
      MLOG_DEBUG(fmt::format("Rank #{}: Skipping test on multi-node environment", g_mpi_world_rank));
      GTEST_SKIP() << fmt::format("Rank #{}: Skipping test on multi-node environment", g_mpi_world_rank);
    }
    RAWCorrectionTest::SetUp();
  }
  void TearDown() override {
    if (g_mpi_world_size != -1) {
      MPI_Barrier(MPI_COMM_WORLD);
    }
  }
};

class MultiNodeRAWCorrectionTest : public RAWCorrectionTest {
 protected:
  void SetUp() override {
    if (g_mpi_world_size < 2) {
      MLOG_WARN("Skipping test on single-node environment");
      GTEST_SKIP() << "Skipping test on single-node environment";
    }
    RAWCorrectionTest::SetUp();
  }
  void TearDown() override {
    if (g_mpi_world_size != -1) {
      MPI_Barrier(MPI_COMM_WORLD);
    }
  }
};

TEST_F(SingleNodeRAWCorrectionTest, T1_0_Minimal) {
  // T1-0, PUT + EXIST + GET + EXIST + DELETE + EXIST
  const std::string pass = "SingleNodeMinimal";
  boost::progress_display progress(dataSizes.size());
  for (const auto data_size : dataSizes) {
    ++progress;
    SCOPED_TRACE(fmt::format("Data size: {}", data_size));

    const auto key = make_key(pass, data_size);
    auto simm_data = store_->Allocate(data_size);
    simm::clnt::DataView simm_data_view(simm_data);
    uint64_t checksum = 0;
    std::string value;
    make_payload(data_size, simm_data_view, checksum, value);

    // PUT
    int32_t kv_ret = store_->Put(key, simm_data_view);
    ASSERT_EQ(kv_ret, CommonErr::OK) << "Put operation failed with error code: " << kv_ret;
    MLOG_DEBUG("PUT Key: {}, Size: {}\n", key, data_size);

    // EXIST
    ASSERT_EQ(store_->Exists(key), CommonErr::OK);
    MLOG_DEBUG("EXIST Key: {}, Exist: {}\n", key, CommonErr::OK);

    // GET
    auto got = store_->Allocate(data_size);
    kv_ret = store_->Get(key, got);
    ASSERT_TRUE(kv_ret >= 0) << "Get operation failed with error code: " << kv_ret;
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

    // NOT EXIST
    ASSERT_EQ(store_->Exists(key), DsErr::KeyNotFound);
    MLOG_DEBUG("EXIST Key: {}, Exist: {}\n", key, DsErr::KeyNotFound);
  }
}

TEST_F(SingleNodeRAWCorrectionTest, T1_1_SingleNodePolling) {
  // T1-1, A PUT, A Polling GET
  // Key size: 10B~40B, Data: 10B~3MB
  const std::string pass = "SingleNodePolling";
  boost::progress_display progress(dataSizes.size() * keyLengths.size());
  for (const auto key_length : keyLengths) {
    for (const auto data_size : dataSizes) {
      SCOPED_TRACE(fmt::format("Key length: {}, Data size: {}", key_length, data_size));
      ++progress;

      const auto key = make_key(pass, 0, key_length);
      auto simm_data = store_->Allocate(data_size);
      simm::clnt::DataView simm_data_view(simm_data);
      uint64_t checksum = 0;
      std::string value;
      make_payload(data_size, simm_data_view, checksum, value);

      // 2 threads
      auto jobs = std::vector<std::thread>{};
      jobs.reserve(2);

      // semaphore to sync PUT and GET
      std::atomic<int> put_stat = -1;

      jobs.emplace_back([&]() {
        try {
          auto kv_ret = store_->Put(key, simm_data_view);
          put_stat.store(kv_ret, std::memory_order_release);
          ASSERT_EQ(kv_ret, CommonErr::OK) << "Put operation failed with error code: " << kv_ret;
          MLOG_INFO("PUT Key: {}, Size: {}\n", key, data_size);
        } catch (const std::exception &e) {
          FAIL() << "Put operation failed with exception: " << e.what();
          put_stat.store(-2, std::memory_order_release);
        }
      });

      bool get_success = false;
      auto got = std::make_unique<simm::clnt::Data>(store_->Allocate(data_size));
      jobs.emplace_back([&]() {
        while (put_stat.load(std::memory_order_acquire) == -1) {
          std::this_thread::sleep_for(std::chrono::milliseconds(FLAGS_startup_wait_ms));
        }  // TODO: TIMEOUT
        ASSERT_EQ(put_stat.load(std::memory_order_acquire), CommonErr::OK)
            << "PUT operation failed, cannot proceed to GET";
        // Polling GET until success
        for (size_t i = 0; i < FLAGS_polling_retries; i++) {
          auto kv_ret = store_->Get(key, *got);
          std::this_thread::sleep_for(std::chrono::milliseconds(FLAGS_polling_interval_ms));
          if (kv_ret < 0) {
            MLOG_DEBUG("GET Key: {} failed with error code: {}, retrying... ({})\n", key, kv_ret, i);
            continue;
          } else {
            get_success = true;
            MLOG_INFO("GET Key: {} succeed after {} retries\n", key, i);
            break;
          }
        }
      });

      for (auto &job : jobs) {
        if (job.joinable()) {
          job.join();
        }
      }

      ASSERT_TRUE(get_success);
      auto got_ref = got->AsRef();
      ASSERT_EQ(got_ref.size(), data_size);
      ASSERT_TRUE(std::equal(value.begin(), value.end(), got_ref.data(), got_ref.data() + value.size()));
      MLOG_DEBUG("GET Key: {}, Size: {}\n", key, got_ref.size());

      uint64_t retrieved_checksum = 0;
      calc_checksum(got_ref, got_ref.size(), retrieved_checksum);

      MLOG_DEBUG("GET Key: {}, Checksum: {:016x}\n", key, retrieved_checksum);

      ASSERT_EQ(checksum, retrieved_checksum);
    }
  }
}

TEST_F(SingleNodeRAWCorrectionTest, T1_1A_SingleNodePolling_Async) {
  // T1-1A, A PUT, A Polling GET (Async)
  // Key size: 10B~40B, Data: 10B~3MB
  const std::string pass = "SingleNodePollingAsync";
  boost::progress_display progress(dataSizes.size() * keyLengths.size());

  for (const auto key_length : keyLengths) {
    for (const auto data_size : dataSizes) {
      ++progress;
      SCOPED_TRACE(fmt::format("Key length: {}, Data size: {}", key_length, data_size));

      const auto key = make_key(pass, 0, key_length);
      auto simm_data = store_->Allocate(data_size);
      simm::clnt::DataView simm_data_view(simm_data);
      uint64_t checksum = 0;
      std::string value;
      make_payload(data_size, simm_data_view, checksum, value);
      try {
        auto kv_ret = store_->Put(key, simm_data_view);
        ASSERT_EQ(kv_ret, CommonErr::OK) << "Put operation failed with error code: " << kv_ret;
        MLOG_INFO("PUT Key: {}, Size: {}\n", key, data_size);
      } catch (const std::exception &e) {
        FAIL() << "Put operation failed with exception: " << e.what();
      }

      std::vector<std::unique_ptr<simm::clnt::Data>> gots;
      gots.reserve(FLAGS_polling_num);

      auto success_count_ptr = std::make_shared<std::atomic<size_t>>(0);
      auto finished_latch_ptr = std::make_shared<std::latch>(FLAGS_polling_num);

      for (auto i = 0u; i < FLAGS_polling_num; ++i) {
        gots.push_back(std::make_unique<simm::clnt::Data>(store_->Allocate(data_size)));
        store_->AsyncGet(key, *gots[i], [success_count_ptr, finished_latch_ptr](int err) {
          if (err == CommonErr::OK) {
            success_count_ptr->fetch_add(1u, std::memory_order_relaxed);
          } else {
            MLOG_ERROR("Async GET failed with error code: {}", err);
          }
          finished_latch_ptr->count_down();
        });
      }

      finished_latch_ptr->wait();
      ASSERT_EQ(success_count_ptr->load(), FLAGS_polling_num)
          << "Async GET operation failed with success count: " << success_count_ptr->load();

      for (const auto &got : gots) {
        auto got_ref = got->AsRef();
        ASSERT_EQ(got_ref.size(), data_size);
        ASSERT_TRUE(std::equal(value.begin(), value.end(), got_ref.data(), got_ref.data() + value.size()));
        MLOG_DEBUG("GET Key: {}, Size: {}\n", key, got_ref.size());

        uint64_t retrieved_checksum = 0;
        calc_checksum(got_ref, got_ref.size(), retrieved_checksum);

        MLOG_DEBUG("GET Key: {}, Checksum: {:016x}\n", key, retrieved_checksum);

        ASSERT_EQ(checksum, retrieved_checksum);
      }
    }
  }
}

TEST_F(MultiNodeRAWCorrectionTest, T1_2_MultiNodePolling) {
  // T1-2 A PUT, A Polling GET, Multi Threads (jthread)
  // Data: Random 1MB: random data + 8' checksum + 8' timestamp
  const std::string pass = "MultiNodePolling";
  boost::progress_display progress(g_mpi_world_size * dataSizes.size() * keyLengths.size());

  for (int main_rank = 0; main_rank < g_mpi_world_size; ++main_rank) {
    for (const auto key_length : keyLengths) {
      for (const auto data_size : dataSizes) {
        SCOPED_TRACE(fmt::format(
            "[#{:2}/#{:2}]Key length: {}, Data size: {}", g_mpi_world_rank, main_rank, key_length, data_size));
        MPI_Barrier(MPI_COMM_WORLD);

        if (g_mpi_world_rank == main_rank) {
          ++progress;
          auto key = make_key(pass, main_rank, key_length);
          // Rank 0: PUT

          auto simm_data = store_->Allocate(data_size);
          simm::clnt::DataView simm_data_view(simm_data);
          uint64_t checksum = 0;
          std::string value;
          make_payload(data_size, simm_data_view, checksum, value);
          try {
            auto kv_ret = store_->Put(key, simm_data_view);
            if (kv_ret != CommonErr::OK) {
              MPI_Abort(MPI_COMM_WORLD, kv_ret);
              FAIL() << "Put operation failed with error code: " << kv_ret;
            } else {
              MLOG_INFO("Bcast: checksum {},PUT Key: {}, Size: {}\n", checksum, key, data_size);
              // Broadcast checksum to other ranks
              ASSERT_EQ(MPI_Bcast(&checksum, 1, MPI_UINT64_T, main_rank, MPI_COMM_WORLD), MPI_SUCCESS)
                  << "MPI_Bcast failed sending checksum";
              ASSERT_EQ(MPI_Bcast(const_cast<char *>(key.c_str()), key.size() + 1, MPI_CHAR, main_rank, MPI_COMM_WORLD),
                        MPI_SUCCESS)
                  << "MPI_Bcast failed sending key";
            }
          } catch (const std::exception &e) {
            FAIL() << "Put operation failed with exception: " << e.what();
          }
        } else {
          // Other Ranks: GET
          uint64_t checksum = 0;
          // Receive checksum
          ASSERT_EQ(MPI_Bcast(&checksum, 1, MPI_UINT64_T, main_rank, MPI_COMM_WORLD), MPI_SUCCESS)
              << "MPI_Bcast failed receiving checksum";

          char key_buffer[512 + 20];  // Assuming reasonable key length limit
          ASSERT_EQ(MPI_Bcast(key_buffer, sizeof(key_buffer), MPI_CHAR, main_rank, MPI_COMM_WORLD), MPI_SUCCESS)
              << "MPI_Bcast failed receiving key";
          auto key = std::string(key_buffer);
          MLOG_DEBUG("Rank {} received checksum: {:016x}\nKey: {}\n", g_mpi_world_rank, checksum, key);

          // Polling GET until success
          auto got = std::make_unique<simm::clnt::Data>(store_->Allocate(data_size));
          bool get_success = false;

          for (size_t i = 0; i < FLAGS_polling_num; i++) {
            auto kv_ret = store_->Get(key, *got);
            if (kv_ret < 0) {
              MLOG_DEBUG("GET Key: {} failed with error code: {}, retrying... ({})\n", key, kv_ret, i);
              continue;
            } else {
              get_success = true;
              if (i > 0)
                MLOG_WARN("GET Key: {} succeed after {} retries\n", key, i);
              break;
            }
          }

          ASSERT_TRUE(get_success);
          auto got_ref = got->AsRef();
          ASSERT_EQ(got_ref.size(), data_size);
          MLOG_DEBUG("GET Key: {}, Size: {}\n", key, got_ref.size());

          uint64_t retrieved_checksum = 0;
          calc_checksum(got_ref, got_ref.size(), retrieved_checksum);

          MLOG_INFO("GET Key: {}, Checksum: {:016x}\n", key, retrieved_checksum);

          ASSERT_EQ(checksum, retrieved_checksum);
        }
      }
    }
  }
}

TEST_F(SingleNodeRAWCorrectionTest, T1_3_SingleNodeMultiThreadPolling) {
  // A multi-threaded Polling GET, data block: 3KB, 1.5MB, 3MB
  // Data: Random 1MB: random data + 8' checksum + 8' timestamp
  const std::string pass = "SingleNodeMultiThreadPolling";
  const std::vector<size_t> block_sizes = {3 * kOneKB, 1500 * kOneKB, 3 * kOneMB};
  std::vector<std::string> keys;
  std::vector<uint64_t> checksums;
  const size_t N = FLAGS_polling_num;  // Number of get
  boost::progress_display progress(N);

  for (size_t i = 0; i < block_sizes.size(); i++) {
    // For each block size, do a PUT
    auto key = make_key(pass, i);
    keys.push_back(key);
    const size_t block_size = block_sizes[i];

    auto simm_data = store_->Allocate(block_size);
    simm::clnt::DataView simm_data_view(simm_data);
    uint64_t checksum = 0;
    std::string value;
    make_payload(block_size, simm_data_view, checksum, value);
    checksums.push_back(checksum);

    auto kv_ret = store_->Put(key, simm_data_view);
    if (kv_ret != CommonErr::OK) {
      FAIL() << "Put operation failed with error code: " << kv_ret;
    } else {
      MLOG_DEBUG("PUT Key: {}, Size: {}\n", key, block_size);
    }
  }

  for (size_t i = 0; i < block_sizes.size(); i++) {
    // Polling GET for 100,000 times, once per time
    std::atomic<size_t> success_count = 0;
    SCOPED_TRACE(fmt::format("Block size: {}, GET", block_sizes[i]));
#pragma omp parallel for
    for (size_t j = 0; j < N; j++) {
#pragma omp critical
      { ++progress; }
      const size_t block_size = block_sizes[i];
      const auto &key = keys[i];
      const auto &checksum = checksums[i];

      auto got = std::make_unique<simm::clnt::Data>(store_->Allocate(block_size));
      auto kv_ret = store_->Get(key, *got);

      if (kv_ret >= 0) {
        auto got_ref = got->AsRef();
        if (got_ref.size() == block_size) {
          uint64_t retrieved_checksum = *reinterpret_cast<const uint64_t *>(got_ref.data() + block_size - 16);
          if (checksum == retrieved_checksum) {
            success_count.fetch_add(1, std::memory_order_relaxed);
            // MLOG_DEBUG("[Thread #{:5},{}] Success.\t {:5}/{:5}, {:2}%",
            //            block_size / kOneKB,
            //            j,
            //            success_count.load(),
            //            N,
            //            (success_count.load() * 100 / N));
          } else {
            MLOG_ERROR("[Thread #{}] Checksum mismatch: {:016x} != {:016x}", j, checksum, retrieved_checksum);
          }
        } else {
          MLOG_ERROR("[Thread #{}] Data mismatch.", j);
        }
      } else {
        MLOG_ERROR("[Thread #{}] GET failed: {}", j, kv_ret);
      }
    }
    MLOG_DEBUG("Succeed count: {} out of {}\n", success_count.load(), N);
    ASSERT_EQ(success_count.load(), N);
  }
}

TEST_F(MultiNodeRAWCorrectionTest, T1_4_MultiNodeMultiThreadPolling) {
  // A multi-threaded Polling GET, data block: 3KB, 1.5MB, 3MB
  // Data: Random 1MB: random data + 8' checksum + 8' timestamp
  const std::string pass = "MultiNodeMultiThreadPolling";
  const std::vector<size_t> block_sizes = {3 * kOneKB, 1500 * kOneKB, 3 * kOneMB};
  const size_t N = FLAGS_polling_num;  // Number of get

  for (size_t i = 0; i < block_sizes.size(); i++) {
    auto key = make_key(pass, i);
    const size_t block_size = block_sizes[i];
    MLOG_INFO("Testing block size: {} Bytes\n", block_size);

    auto simm_data = store_->Allocate(block_size);
    simm::clnt::DataView simm_data_view(simm_data);
    uint64_t checksum = 0;
    std::string value;
    make_payload(block_size, simm_data_view, checksum, value);

    auto kv_ret = store_->Put(key, simm_data_view);
    if (kv_ret != CommonErr::OK) {
      FAIL() << "Put operation failed with error code: " << kv_ret;
    } else {
      MLOG_DEBUG("PUT Key: {}, Size: {}\n", key, block_size);
    }

    // Polling GET for 100,000 times, once per time
    std::atomic<size_t> success_count = 0;
    MPI_Barrier(MPI_COMM_WORLD);
    boost::progress_display progress(N);
#pragma omp parallel for
    for (size_t j = 0; j < N; j++) {
#pragma omp critical
      {
        if (g_mpi_world_rank == 0) {
          ++progress;
        }
      }
      auto got = std::make_unique<simm::clnt::Data>(store_->Allocate(block_size));
      auto kv_ret = store_->Get(key, *got);

      if (kv_ret >= 0) {
        auto got_ref = got->AsRef();
        if (got_ref.size() == block_size &&
            std::equal(value.begin(), value.end(), got_ref.data(), got_ref.data() + value.size())) {
          uint64_t retrieved_checksum = *reinterpret_cast<const uint64_t *>(got_ref.data() + block_size - 16);
          if (checksum == retrieved_checksum) {
            success_count.fetch_add(1, std::memory_order_relaxed);
            // MLOG_DEBUG("[Thread #{:5},{}] Success.\t {:5}/{:5}, {:2}%",
            //            block_size / kOneKB,
            //            j,
            //            success_count.load(),
            //            N,
            //            (success_count.load() * 100 / N));
          } else {
            MLOG_ERROR("[Thread #{}] Checksum mismatch: {:016x} != {:016x}", j, checksum, retrieved_checksum);
          }
        } else {
          MLOG_ERROR("[Thread #{}] Data mismatch.", j);
        }
      } else {
        MLOG_ERROR("[Thread #{}] GET failed.", j);
      }
    }
    MLOG_DEBUG(
        "GET Key: {} with size {} finished. Succeed count: {} out of {}\n", key, block_size, success_count.load(), N);
    ASSERT_EQ(success_count.load(), N);
  }
}

TEST_F(SingleNodeRAWCorrectionTest, T1_5_NotExists) {
  // T1-5 GET a non-exist key
  const std::string pass = "NotExists";
  const auto key = make_key(pass, 0);
  const auto t = FLAGS_polling_num;
  boost::progress_display progress(t);
  const auto expected_err = ClntErr::ClntGetObjectFailed;

  auto got = std::make_unique<simm::clnt::Data>(store_->Allocate(FLAGS_block_size * kOneB));
  auto total_duration = 0;
  MLOG_INFO("Testing non-exist key: {} for {} times.\n", key, t);

  auto simm_data = store_->Allocate(FLAGS_block_size * kOneB);
  simm::clnt::DataView simm_data_view(simm_data);
  uint64_t checksum = 0;
  std::string value;
  make_payload(FLAGS_block_size * kOneB, simm_data_view, checksum, value);

#pragma omp parallel for
  for (auto i = 0u; i <= t; i++) {
#pragma omp critical
    { ++progress; }
    // PUT
    auto kv_ret = store_->Put(key, simm_data_view);
    if (kv_ret != CommonErr::OK) {
      MLOG_ERROR("Put operation failed with error code: {}\n", kv_ret);
    } else {
      MLOG_DEBUG("PUT Key: {}, Size: {}\n", key, FLAGS_block_size * kOneB);
    }

    // EXIST
    MLOG_DEBUG("Exist: {}, ret: {}", key, store_->Exists(key));

    // DELETE
    kv_ret = store_->Delete(key);
    if (kv_ret != CommonErr::OK) {
      MLOG_ERROR("Delete operation failed with error code: {}\n", kv_ret);
    } else {
      MLOG_DEBUG("DELETE Key: {}\n", key);
    }

    // EXIST
    MLOG_DEBUG("Exist: {}, ret: {}", key, store_->Exists(key));

    // GET not exists key should fail immediately
    auto start_time = std::chrono::high_resolution_clock::now();
    kv_ret = store_->Get(key, *got);
    if (kv_ret >= 0) {
      MLOG_ERROR("GET a non-exist key should not succeed.");
    } else if (kv_ret == expected_err) {
      MLOG_DEBUG("GET Key: {} failed with error code: {}, as expected.\n", key, kv_ret);
    } else {
      MLOG_ERROR("GET Key failed with code: {}, but expected {}", kv_ret, expected_err)
    }
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    total_duration += duration;
    MLOG_DEBUG("GET NotFound Key: took {} ms.\n", key, duration);
  }
  MLOG_INFO("GET NotFound Key: average {} ms.\n", key, total_duration, t, total_duration / t);
}

TEST_F(MultiNodeRAWCorrectionTest, T1_6_DuplicateKey) {
  // T1-6 PUT a duplicate key, A put with duplicate fail, other threads get the first key.
  const std::string pass = "DuplicateKey";
  if (g_mpi_world_rank == 0) {
    const auto key = make_key(pass, 0);
    // Broadcast key
    ASSERT_EQ(MPI_Bcast(const_cast<char *>(key.c_str()), key.size() + 1, MPI_CHAR, 0, MPI_COMM_WORLD), MPI_SUCCESS)
        << "MPI_Bcast failed sending key";

    auto simm_data = store_->Allocate(FLAGS_block_size * kOneB);
    simm::clnt::DataView simm_data_view(simm_data);
    uint64_t checksum = 0;
    std::string value;
    make_payload(FLAGS_block_size * kOneB, simm_data_view, checksum, value);

    auto kv_ret = store_->Put(key, simm_data_view);
    if (kv_ret != CommonErr::OK) {
      MPI_Abort(MPI_COMM_WORLD, kv_ret);
      FAIL() << "First Put operation failed with error code: " << kv_ret;
    } else {
      MLOG_DEBUG("First PUT Key: {}, Size: {}\n", key, FLAGS_block_size * kOneB);
    }

    // EXIST
    ASSERT_EQ(store_->Exists(key), CommonErr::OK);

    // Broadcast checksum
    ASSERT_EQ(MPI_Bcast(&checksum, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD), MPI_SUCCESS) << "MPI_Bcast checksum failed";

#ifdef NO_OVERWRITE
    try {
      auto kv_ret = store_->Put(key, simm_data_view);
      if (kv_ret == CommonErr::OK) {
        FAIL() << "Duplicate Put operation should not succeed.";
      } else if (kv_ret == expected_err) {
        MLOG_DEBUG("Duplicate PUT Key: {} failed as expected with error code: {}\n", key, kv_ret);
      } else {
        FAIL() << "Duplicate Put operation failed with error code: " << kv_ret << ", but expected "
               << static_cast<int>(expected_err);
      }
    } catch (const std::exception &e) {
      FAIL() << "Duplicate Put operation failed with exception: " << e.what();
    }
#else
    // Overwrite is allowed, so the second PUT should succeed
    try {
      auto kv_ret = store_->Put(key, simm_data_view);
      if (kv_ret != CommonErr::OK) {
        FAIL() << "Second Put operation failed with error code: " << kv_ret;
      } else {
        MLOG_DEBUG("Second PUT Key: {}, Size: {}\n", key, FLAGS_block_size * kOneB);
      }
    } catch (const std::exception &e) {
      FAIL() << "Second Put operation failed with exception: " << e.what();
    }
#endif
  } else {
    // Receive Key
    char key_buffer[512 + 20];  // Assuming reasonable key length limit
    ASSERT_EQ(MPI_Bcast(key_buffer, sizeof(key_buffer), MPI_CHAR, 0, MPI_COMM_WORLD), MPI_SUCCESS)
        << "MPI_Bcast failed receiving key";
    auto key = std::string(key_buffer);

    // Receive checksum
    uint64_t checksum = 0;
    ASSERT_EQ(MPI_Bcast(&checksum, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD), MPI_SUCCESS)
        << "MPI_Bcast failed receiving checksum";

    MLOG_DEBUG("Rank {} received checksum: {:016x}\nKey: {}\n", g_mpi_world_rank, checksum, key);

    // Polling GET until success
    auto got = std::make_unique<simm::clnt::Data>(store_->Allocate(FLAGS_block_size * kOneB));

    boost::progress_display progress(FLAGS_polling_num);
    for (size_t j = 0; j < FLAGS_polling_num; j++) {
      { ++progress; }
      auto kv_ret = store_->Get(key, *got);
      ASSERT_TRUE(kv_ret >= 0);
      MLOG_DEBUG("GET Key: {} succeed\n", key, j);
      auto got_ref = got->AsRef();
      uint64_t retrieved_checksum = 0;
      calc_checksum(got_ref, got_ref.size(), retrieved_checksum);
      ASSERT_EQ(checksum, retrieved_checksum);
    }

    auto got_ref = got->AsRef();
    ASSERT_EQ(got_ref.size(), FLAGS_block_size * kOneB);
    MLOG_DEBUG("GET Key: {}, Size: {}\n", key, got_ref.size());

    uint64_t retrieved_checksum = 0;
    calc_checksum(got_ref, got_ref.size(), retrieved_checksum);

    MLOG_DEBUG("GET Key: {}, Checksum: {:016x}\n", key, retrieved_checksum);

    ASSERT_EQ(checksum, retrieved_checksum);
  }
}

TEST_F(SingleNodeRAWCorrectionTest, T1_7_SingleNodePutGetDeleteLoop) {
  // T1-7 A PUT, A GET, A DELETE, Loop 100'000 times
  // Same key with diffrent data block
  // GTEST_SKIP() << "DELETE API is not ready yet";
  const std::string pass = "SingleNodePutGetDeleteLoop";
  const size_t loop_num = FLAGS_polling_num;
  boost::progress_display progress(loop_num);
  auto key = make_key(pass, 0);

  auto simm_data = store_->Allocate(FLAGS_block_size * kOneB);
  simm::clnt::DataView simm_data_view(simm_data);
  uint64_t checksum = 0;
  std::string value;

  for (size_t i = 0; i < loop_num; i++) {
    ++progress;

    make_payload(FLAGS_block_size * kOneB, simm_data_view, checksum, value);
    MLOG_DEBUG("[PGD-Loop {}/{}] Checksum: {:016x}\n", i + 1, loop_num, checksum);

    // PUT
    try {
      auto kv_ret = store_->Put(key, simm_data_view);
      if (kv_ret != 0) {
        FAIL() << "Put operation failed with error code: " << kv_ret;
      } else {
        MLOG_DEBUG("[{}/{}] PUT Key: {}, Size: {}\n", i + 1, loop_num, key, FLAGS_block_size * kOneB);
      }
    } catch (const std::exception &e) {
      FAIL() << "Put operation failed with exception: " << e.what();
    }

    // GET
    auto got = std::make_unique<simm::clnt::Data>(store_->Allocate(FLAGS_block_size * kOneB));
    auto kv_ret = store_->Get(key, *got);
    if (kv_ret < 0) {
      FAIL() << "GET Key: {} failed with error code: " << kv_ret;
    } else {
      auto got_ref = got->AsRef();
      ASSERT_EQ(got_ref.size(), FLAGS_block_size * kOneB);
      ASSERT_TRUE(std::equal(value.begin(), value.end(), got_ref.data(), got_ref.data() + value.size()));
      MLOG_DEBUG("[{}/{}] GET Key: {}, Size: {}\n", i + 1, loop_num, key, got_ref.size());

      uint64_t retrieved_checksum = 0;
      calc_checksum(got_ref, got_ref.size(), retrieved_checksum);

      MLOG_DEBUG("[{}/{}] GET Key: {}, Checksum: {:016x}\n", i + 1, loop_num, key, retrieved_checksum);

      ASSERT_EQ(checksum, retrieved_checksum);
    }

    // DELETE
    kv_ret = store_->Delete(key);
    if (kv_ret != 0) {
      FAIL() << "DELETE Key: {} failed with error code: " << kv_ret;
    } else {
      MLOG_DEBUG("[{}/{}] DELETE Key: {}\n", i + 1, loop_num, key);
    }
  }
}

TEST_F(MultiNodeRAWCorrectionTest, T1_8_MultiNodePutGetDeleteLoop) {
  // T1-8 A PUT, A GET, A DELETE, Loop 100'000 times
  // Different keys with diffrent data block
  // GTEST_SKIP() << "DELETE API is not ready yet";
  const std::string pass = "MultiNodePutGetDeleteLoop";
  const size_t loop_num = FLAGS_polling_num;
  boost::progress_display progress(loop_num);
  std::atomic<bool> early_exit_triggered(false);

#pragma omp parallel for
  for (size_t i = 1; i <= loop_num; i++) {
#pragma omp critical
    { ++progress; }

    if (early_exit_triggered.load(std::memory_order_relaxed)) {
      continue;
    }

    auto simm_data = store_->Allocate(FLAGS_block_size * kOneB);
    simm::clnt::DataView simm_data_view(simm_data);
    uint64_t checksum = 0;
    std::string value;
    auto key = make_key(pass, g_mpi_world_rank);  // NOTE: Actually different process has different random suffix

    make_payload(FLAGS_block_size * kOneB, simm_data_view, checksum, value);
    MLOG_DEBUG("[PGD-Loop {}/{}] Checksum: {:016x}\n", i, loop_num, checksum);

    // PUT
    try {
      auto kv_ret = store_->Put(key, simm_data_view);
      if (kv_ret != 0) {
        MLOG_ERROR("Put operation failed with error code: {}\n", kv_ret);
        early_exit_triggered.store(true, std::memory_order_relaxed);
        continue;
      } else {
        MLOG_DEBUG("[{}/{}] PUT Key: {}, Size: {}\n", i + 1, loop_num, key, FLAGS_block_size * kOneB);
      }
    } catch (const std::exception &e) {
      MLOG_ERROR("Put operation failed with exception: {}\n", e.what());
      early_exit_triggered.store(true, std::memory_order_relaxed);
      continue;
    }

    // GET
    auto got = std::make_unique<simm::clnt::Data>(store_->Allocate(FLAGS_block_size * kOneB));
    auto kv_ret = store_->Get(key, *got);
    if (kv_ret < 0) {
      MLOG_ERROR("GET Key: {} failed with error code: ", key, kv_ret);
      early_exit_triggered.store(true, std::memory_order_relaxed);
      continue;
    } else {
      auto got_ref = got->AsRef();
      // ASSERT_EQ(got_ref.size(), FLAGS_block_size * kOneB);
      // ASSERT_TRUE(std::equal(value.begin(), value.end(), got_ref.data(), got_ref.data() + value.size()));
      if (got_ref.size() != FLAGS_block_size * kOneB ||
          !std::equal(value.begin(), value.end(), got_ref.data(), got_ref.data() + value.size())) {
        MLOG_ERROR("Data mismatch on GET Key: {}", key);
        early_exit_triggered.store(true, std::memory_order_relaxed);
        continue;
      }
      MLOG_DEBUG("[{}/{}] GET Key: {}, Size: {}\n", i + 1, loop_num, key, got_ref.size());

      uint64_t retrieved_checksum = 0;
      calc_checksum(got_ref, got_ref.size(), retrieved_checksum);

      MLOG_DEBUG("[{}/{}] GET Key: {}, Checksum: {:016x}\n", i + 1, loop_num, key, retrieved_checksum);

      // ASSERT_EQ(checksum, retrieved_checksum);
      if (checksum != retrieved_checksum) {
        MLOG_ERROR("Checksum mismatch on GET Key: {}, {:016x} != {:016x}", key, checksum, retrieved_checksum);
        early_exit_triggered.store(true, std::memory_order_relaxed);
        continue;
      }
    }

    // DELETE
    kv_ret = store_->Delete(key);
    if (kv_ret != 0) {
      MLOG_ERROR("DELETE Key: {} failed with error code: {}\n", key, kv_ret);
      early_exit_triggered.store(true, std::memory_order_relaxed);
      continue;
    } else {
      MLOG_DEBUG("[{}/{}] DELETE Key: {}\n", i + 1, loop_num, key);
    }
  }

  ASSERT_FALSE(early_exit_triggered.load(std::memory_order_relaxed));
}

TEST_F(SingleNodeRAWCorrectionTest, T1_9_SingleNodeMultiKeys) {
  // T1-9 PUT 100'000 different keys then GET them
  const std::string pass = "SingleNodeMultiKeys";
  const size_t key_num = FLAGS_key_num;  // Default 200,000
  boost::progress_display progress(key_num);
  std::vector<std::string> keys;
  keys.reserve(key_num);
  std::vector<std::uint64_t> checksums;
  checksums.reserve(key_num);

  auto simm_data = store_->Allocate(FLAGS_block_size * kOneB);  // Default 1MB
  simm::clnt::DataView simm_data_view(simm_data);
  uint64_t checksum = 0;
  std::string value;

  // Total: 200,000 * 3MB = 600GB data

  for (size_t i = 0; i < key_num; i++) {
    SCOPED_TRACE(fmt::format("Generating & Putting Key {}/{}", i + 1, key_num));
    ++progress;
    // TODO: Random key/data length
    keys.push_back(make_key(pass, i));
    make_payload(FLAGS_block_size * kOneB, simm_data_view, checksum, value);
    checksums.push_back(checksum);

    // PUT
    try {
      auto kv_ret = store_->Put(keys.back(), simm_data_view);
      if (kv_ret != 0) {
        FAIL() << "Put operation failed with error code: " << kv_ret;
      }
    } catch (const std::exception &e) {
      FAIL() << "Put operation failed with exception: " << e.what();
    }
  }

  for (size_t i = 0; i < key_num; i++) {
    // GET
    SCOPED_TRACE(fmt::format("Getting Key {}/{}", i + 1, key_num));
    ++progress;
    auto got = std::make_unique<simm::clnt::Data>(store_->Allocate(FLAGS_block_size * kOneB));
    auto kv_ret = store_->Get(keys[i], *got);
    if (kv_ret < 0) {
      FAIL() << "GET Key: {} failed with error code: " << kv_ret;
    } else {
      auto got_ref = got->AsRef();
      ASSERT_EQ(got_ref.size(), FLAGS_block_size * kOneB);
      MLOG_DEBUG("[{}/{}] GET Key: {}, Size: {}\n", i + 1, key_num, keys[i], got_ref.size());

      uint64_t retrieved_checksum = 0;
      calc_checksum(got_ref, got_ref.size(), retrieved_checksum);

      MLOG_DEBUG("[{}/{}] GET Key: {}, Checksum: {:016x}\n", i + 1, key_num, keys[i], retrieved_checksum);

      ASSERT_EQ(checksums[i], retrieved_checksum);
    }
  }
}

TEST_F(SingleNodeRAWCorrectionTest, T1_10_CornerCases) {
  // T1-10 Corner cases for giant blocks
  // Data: (1KB, 1MB, 8MB, 32MB, 500MB) +/- 5B
  const std::string pass = "CornerCases";
  const std::vector<size_t> corner_sizes = {
      1 * kOneKB, 1 * kOneMB, 3 * kOneKB, 32 * kOneKB, 4 * kOneMB - 1, 4 * kOneMB};
  const size_t jitter = 5;  // +/- bytes

  for (const auto base_size : corner_sizes) {
    for (const auto delta : {-jitter, 0ul, jitter}) {
      const size_t data_size = std::min(base_size + delta, 4 * kOneMB);  // Limit max size to 4MB for test env
      SCOPED_TRACE(fmt::format("Data size: {}", data_size));
      MLOG_INFO("Testing data size: {} Bytes", data_size);
      auto key = make_key(pass, data_size);

      auto simm_data = store_->Allocate(data_size);
      simm::clnt::DataView simm_data_view(simm_data);
      uint64_t checksum = 0;
      std::string value;
      make_payload(data_size, simm_data_view, checksum, value);
      MLOG_INFO("with checksum {:016x}\n", checksum);

      // PUT
      try {
        auto kv_ret = store_->Put(key, simm_data_view);
        if (kv_ret != 0) {
          FAIL() << "Put operation failed with error code: " << kv_ret;
        } else {
          MLOG_DEBUG("PUT Key: {}, Size: {}\n", key, data_size);
        }
      } catch (const std::exception &e) {
        FAIL() << "Put operation failed with exception: " << e.what();
      }

      // GET
      auto got = std::make_unique<simm::clnt::Data>(store_->Allocate(data_size));
      auto kv_ret = store_->Get(key, *got);
      if (kv_ret < 0) {
        FAIL() << "GET Key: {} failed with error code: " << kv_ret;
      } else {
        auto got_ref = got->AsRef();
        ASSERT_EQ(got_ref.size(), data_size);
        ASSERT_TRUE(std::equal(value.begin(), value.end(), got_ref.data(), got_ref.data() + value.size()));
        MLOG_DEBUG("GET Key: {}, Size: {}\n", key, got_ref.size());
        uint64_t retrieved_checksum = 0;
        calc_checksum(got_ref, got_ref.size(), retrieved_checksum);
        ASSERT_EQ(checksum, retrieved_checksum);
      }
    }
  }
}

TEST_F(MultiNodeRAWCorrectionTest, T1_11_MultiNodePolling) {
  // T1-11, PUT + Polling GET
  // Key size: 10B~40B, Data: 10B~3MB
  const std::string pass = "MultiNodePolling";
  boost::progress_display progress(keyLengths.size() * dataSizes.size());
  for (const auto key_length : keyLengths) {
    for (const auto data_size : dataSizes) {
      SCOPED_TRACE(fmt::format("Key lengh: {}, Data size: {}", key_length, data_size));
      ++progress;

      const auto key = make_key(pass, 0, key_length);
      auto simm_data = store_->Allocate(data_size);
      simm::clnt::DataView simm_data_view(simm_data);
      uint64_t checksum = 0;
      std::string value;
      make_payload(data_size, simm_data_view, checksum, value);

      // 2 threads
      auto jobs = std::vector<std::thread>{};
      jobs.reserve(2);

      // semaphore of PUT success
      std::atomic<bool> put_success = false;

      jobs.emplace_back([&]() {
        try {
          auto kv_ret = store_->Put(key, simm_data_view);
          ASSERT_EQ(kv_ret, CommonErr::OK) << "Put operation failed with error code: " << kv_ret;
          MLOG_INFO("PUT Key: {}, Size: {}\n", key, data_size);
          put_success.store(true, std::memory_order_release);
        } catch (const std::exception &e) {
          FAIL() << "Put operation failed with exception: " << e.what();
        }
      });

      bool get_success = false;
      auto got = std::make_unique<simm::clnt::Data>(store_->Allocate(data_size));
      jobs.emplace_back([&]() {
        while (!put_success.load(std::memory_order_acquire)) {
          std::this_thread::sleep_for(std::chrono::milliseconds(FLAGS_startup_wait_ms));
        }  // TODO: TIMEOUT
        // Polling GET until success
        for (size_t i = 0; i < FLAGS_polling_retries; i++) {
          auto kv_ret = store_->Get(key, *got);
          std::this_thread::sleep_for(std::chrono::milliseconds(FLAGS_polling_interval_ms));
          if (kv_ret < 0) {
            MLOG_DEBUG("GET Key: {} failed with error code: {}, retrying... ({})\n", key, kv_ret, i);
            continue;
          } else {
            get_success = true;
            MLOG_INFO("GET Key: {} succeed after {} retries\n", key, i);
            break;
          }
        }
      });

      for (auto &job : jobs) {
        if (job.joinable()) {
          job.join();
        }
      }

      ASSERT_TRUE(get_success);
      auto got_ref = got->AsRef();
      ASSERT_EQ(got_ref.size(), data_size);
      ASSERT_TRUE(std::equal(value.begin(), value.end(), got_ref.data(), got_ref.data() + value.size()));
      MLOG_DEBUG("GET Key: {}, Size: {}\n", key, got_ref.size());

      uint64_t retrieved_checksum = 0;
      calc_checksum(got_ref, got_ref.size(), retrieved_checksum);

      MLOG_DEBUG("GET Key: {}, Checksum: {:016x}\n", key, retrieved_checksum);

      ASSERT_EQ(checksum, retrieved_checksum);
    }
  }
}

TEST_F(MultiNodeRAWCorrectionTest, T1_12_MultiNodeNotExists) {
  // T1-12 A PUT DELETE, BCast -->, Other Ranks GET a non-exist key
  // Data: Random 1MB: random data + 8' checksum + 8' timestamp
  const std::string pass = "MultiNodeNotExists";

  for (const auto main_rank : {0, 1}) {
    for (const auto key_length : keyLengths) {
      for (const auto data_size : dataSizes) {
        SCOPED_TRACE(fmt::format(
            "[#{:2}/#{:2}]Key length: {}, Data size: {}", g_mpi_world_rank, main_rank, key_length, data_size));

        if (g_mpi_world_rank == main_rank) {
          auto key = make_key(pass, main_rank, key_length);
          // Rank 0: PUT
          auto simm_data = store_->Allocate(data_size);
          simm::clnt::DataView simm_data_view(simm_data);
          uint64_t checksum = 0;
          std::string value;
          make_payload(data_size, simm_data_view, checksum, value);
          std::this_thread::sleep_for(std::chrono::milliseconds(FLAGS_startup_wait_ms));
          try {
            auto kv_ret = store_->Put(key, simm_data_view);
            ASSERT_EQ(kv_ret, CommonErr::OK);
            kv_ret = store_->Delete(key);
            ASSERT_EQ(kv_ret, CommonErr::OK);
            MLOG_DEBUG("Bcast: Key: {}\n", key);
            ASSERT_EQ(MPI_Bcast(const_cast<char *>(key.c_str()), key.size() + 1, MPI_CHAR, main_rank, MPI_COMM_WORLD),
                      MPI_SUCCESS)
                << "MPI_Bcast failed sending key";
          } catch (const std::exception &e) {
            FAIL() << "Put operation failed with exception: " << e.what();
          }
        } else {
          // Other Ranks: GET
          char key_buffer[512 + 20];  // Assuming reasonable key length limit
          ASSERT_EQ(MPI_Bcast(key_buffer, sizeof(key_buffer), MPI_CHAR, main_rank, MPI_COMM_WORLD), MPI_SUCCESS)
              << "MPI_Bcast failed receiving key";
          auto key = std::string(key_buffer);
          MLOG_DEBUG("Rank {} received Key: {}\n", g_mpi_world_rank, key);

          // Polling GET
          auto start_time = std::chrono::high_resolution_clock::now();
          auto got = std::make_unique<simm::clnt::Data>(store_->Allocate(data_size));
          auto kv_ret = store_->Get(key, *got);
          auto end_time = std::chrono::high_resolution_clock::now();
          ASSERT_EQ(kv_ret, ClntErr::ClntGetObjectFailed);
          auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
          MLOG_DEBUG("GET NotFound Key: {} took {} ms.\n", key, duration);
        }
      }
    }
  }
}
}  // namespace clnt
}  // namespace simm

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  folly::Init init(&argc, &argv);

  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &g_mpi_world_rank);
  MLOG_INFO("MPI World Rank: {}\n", g_mpi_world_rank);
  MPI_Comm_size(MPI_COMM_WORLD, &g_mpi_world_size);
  MLOG_INFO("MPI World Size: {}\n", g_mpi_world_size);

  // add silent mode in gtest for non-zero rank
  if (g_mpi_world_rank != 0) {
    ::testing::GTEST_FLAG(color) = "no";
  }

  int result = RUN_ALL_TESTS();
  MPI_Finalize();
  return result;
}
