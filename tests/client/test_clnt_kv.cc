#include <atomic>
#include <random>
#include <thread>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <folly/synchronization/Baton.h>

#include "common/errcode/errcode_def.h"
#include "fmt/core.h"
#include "simm/simm_common.h"
#include "simm/simm_kv.h"

DECLARE_string(cm_primary_node_ip);

constexpr size_t kOneMB = 1 << 20;
constexpr size_t kBufferSize = 1 << 29;

namespace simm {
namespace clnt {

class LibsimmKV : public ::testing::Test {
 protected:
  void SetUp() override {
    store_ = std::make_unique<KVStore>();
    data_ = std::make_unique<Data>(store_->Allocate(kOneMB));
    buffer_ = std::make_unique<char[]>(kBufferSize);
    auto buffer_addr = buffer_.get();
    mr_ = RegisterMr(reinterpret_cast<uintptr_t>(buffer_addr), kBufferSize);

    std::random_device device;
    std::uniform_int_distribution<> distributor(1, 1000);
    key_ = distributor(device);
  }

  void TearDown() override {
    store_.reset();
    data_.reset();
  }

  std::unique_ptr<KVStore> store_{nullptr};
  std::unique_ptr<Data> data_{nullptr};
  std::unique_ptr<char[]> buffer_{nullptr};
  std::optional<MrExt> mr_;
  size_t key_{0};

  std::string make_key(std::string_view pass, size_t index) { return fmt::format("T{}-{}-{}", pass, key_, index); }
};

TEST_F(LibsimmKV, PutOneGetOne) {
  constexpr size_t kRounds = 100;
  auto data = data_->AsRef();

  for (auto i = 0ul; i < kRounds; ++i) {
    auto key = make_key("PutOneGetOne", i);
    std::copy(key.begin(), key.end(), data.begin());
    ASSERT_TRUE(int(store_->Put(key, *data_)) == 0);

    //   ASSERT_TRUE(store_->Exists(key));

    auto got = std::make_unique<Data>(store_->Allocate(kOneMB));
    ASSERT_TRUE(int(store_->Get(key, *got)) > 0);
    auto got_ = got->AsRef();
    ASSERT_TRUE(std::equal(key.begin(), key.end(), got_.data(), got_.data() + key.size()));
  }
}

TEST_F(LibsimmKV, PutOneOverrideGetOne) {
  constexpr size_t kRounds = 100;
  auto data = data_->AsRef();

  for (auto i = 0ul; i < kRounds; ++i) {
    auto key = make_key("PutOneOverrideGetOne", i);
    for (auto j = 0ul; j < kRounds; ++j) {
      auto data_str = fmt::format("{}-{}", key, j);
      std::copy(data_str.begin(), data_str.end(), data.begin());
      ASSERT_TRUE(int(store_->Put(key, *data_)) == 0);

      //   ASSERT_TRUE(store_->Exists(key));

      auto got = std::make_unique<Data>(store_->Allocate(kOneMB));
      ASSERT_TRUE(int(store_->Get(key, *got)) > 0);
      auto got_ = got->AsRef();
      ASSERT_TRUE(std::equal(data_str.begin(), data_str.end(), got_.data(), got_.data() + data_str.size()));
    }
  }
}

TEST_F(LibsimmKV, PutManyGetMany) {
  constexpr size_t kManyGetPut = 1000;

  auto data = data_->AsRef();
  for (auto i = 0ul; i < kManyGetPut; ++i) {
    auto key = make_key("PutManyGetMany", i);
    std::copy(key.begin(), key.end(), data.begin());
    ASSERT_TRUE(int(store_->Put(key, *data_)) == 0);
  }

  for (auto i = 0ul; i < kManyGetPut; ++i) {
    auto key = make_key("PutManyGetMany", i);

    // ASSERT_TRUE(store_->Exists(key));

    auto got = std::make_unique<Data>(store_->Allocate(kOneMB));
    ASSERT_TRUE(int(store_->Get(key, *got)) > 0);
    auto got_ = got->AsRef();
    ASSERT_TRUE(std::equal(key.begin(), key.end(), got_.data(), got_.data() + key.size()));
  }
}

TEST_F(LibsimmKV, MPutManyMGetMany) {
  constexpr size_t kManyGetPut = 1000;

  std::vector<std::string> keys;
  std::vector<DataView> inputs;
  std::vector<std::unique_ptr<Data>> holder;
  for (auto i = 0ul; i < kManyGetPut; ++i) {
    auto input = std::make_unique<Data>(store_->Allocate(kOneMB));
    auto data = input->AsRef();
    auto key = make_key("MPutManyMGetMany", i);
    std::copy(key.begin(), key.end(), data.begin());
    keys.emplace_back(key);
    inputs.emplace_back(DataView(*input));
    holder.emplace_back(std::move(input));
  }
  auto put_rets = store_->MPut(keys, inputs);
  for (auto ret : put_rets) {
    ASSERT_TRUE(ret == 0);
  }

  // Check exists
  auto exist_rets = store_->MExists(keys);
  for (auto ret : exist_rets) {
    ASSERT_TRUE(ret == 0);
  }

  std::vector<DataView> outputs;
  std::vector<std::unique_ptr<Data>> output_holder;
  for (auto i = 0ul; i < kManyGetPut; ++i) {
    auto got = std::make_unique<Data>(store_->Allocate(kOneMB));
    outputs.emplace_back(DataView(*got));
    output_holder.emplace_back(std::move(got));
  }
  auto get_rets = store_->MGet(keys, outputs);
  for (auto ret : get_rets) {
    ASSERT_TRUE(ret > 0);
  }
  for (auto i = 0ul; i < kManyGetPut; ++i) {
    auto key = make_key("MPutManyMGetMany", i);
    auto got = outputs[i];
    auto got_ = got.AsRef();
    //std::cout << "key: " << key << ", value: " << got_.data() << std::endl;
    ASSERT_TRUE(std::equal(key.begin(), key.end(), got_.data(), got_.data() + key.size()));
  }
}

TEST_F(LibsimmKV, MPutManyMGetManyMr) {
  constexpr size_t kManyGetPut = 256;

  std::vector<std::string> keys;
  std::vector<DataView> inputs;
  for (auto i = 0ul; i < kManyGetPut; ++i) {
    auto input_addr = reinterpret_cast<uintptr_t>(buffer_.get()) + i * kOneMB;
    auto input = DataView(input_addr, kOneMB, mr_.value());
    auto key = make_key("MPutManyMGetMany", i);
    std::copy(key.begin(), key.end(), reinterpret_cast<char*>(input_addr));
    keys.emplace_back(key);
    inputs.emplace_back(input);
  }
  auto put_rets = store_->MPut(keys, inputs);
  for (auto ret : put_rets) {
    ASSERT_TRUE(ret == 0);
  }

  // Check exists
  auto exist_rets = store_->MExists(keys);
  for (auto ret : exist_rets) {
    ASSERT_TRUE(ret == 0);
  }

  std::vector<DataView> outputs;
  for (auto i = 0ul; i < kManyGetPut; ++i) {
    auto got_addr = reinterpret_cast<uintptr_t>(buffer_.get()) + i * kOneMB;
    auto got = DataView(got_addr, kOneMB, mr_.value());
    outputs.emplace_back(got);
  }
  auto get_rets = store_->MGet(keys, outputs);
  for (auto ret : get_rets) {
    ASSERT_TRUE(ret > 0);
  }
  for (auto i = 0ul; i < kManyGetPut; ++i) {
    auto key = make_key("MPutManyMGetMany", i);
    auto got = outputs[i];
    auto got_ = got.AsRef();
    //std::cout << "key: " << key << ", value: " << got_.data() << std::endl;
    ASSERT_TRUE(std::equal(key.begin(), key.end(), got_.data(), got_.data() + key.size()));
  }
}

TEST_F(LibsimmKV, PutGetInParallel) {
  constexpr size_t kPutGetInParallel = 1000;

  auto jobs = std::vector<std::jthread>{};
  jobs.reserve(kPutGetInParallel);

  auto put_worker = [&](auto &&key, auto &&data) {
    auto data_ = data->AsRef();
    std::copy(key.begin(), key.end(), data_.begin());
    ASSERT_TRUE(int(store_->Put(key, *data)) == 0);
  };
  for (auto i = 0ul; i < kPutGetInParallel; ++i) {
    auto key = make_key("PutGetInParallel", i);
    auto data = std::make_unique<Data>(store_->Allocate(kOneMB));
    auto thread = std::jthread(put_worker, std::move(key), std::move(data));
    jobs.emplace_back(std::move(thread));
  }

  jobs.clear();

  auto get_worker = [&](auto &&key, auto &&got) {
    // ASSERT_TRUE(store_->Exists(key));
    ASSERT_TRUE(int(store_->Get(key, *got)) > 0);
    auto got_ = got->AsRef();
    ASSERT_TRUE(std::equal(key.begin(), key.end(), got_.data(), got_.data() + key.size()));
  };

  for (auto i = 0ul; i < kPutGetInParallel; ++i) {
    auto key = make_key("PutGetInParallel", i);
    auto got = std::make_unique<Data>(store_->Allocate(kOneMB));

    // get_worker(std::move(key), std::move(got));
    auto thread = std::jthread(get_worker, std::move(key), std::move(got));
    jobs.emplace_back(std::move(thread));
  }
}

TEST_F(LibsimmKV, OutOfRange) {
  auto data = data_->AsRef();
  auto key = make_key("OutOfRange", 0);
  std::copy(key.begin(), key.end(), data.begin());

  // key too long
  key.append(400, 'k');
  ASSERT_EQ(int(store_->Put(key, *data_)), CommonErr::InvalidArgument);
  ASSERT_EQ(int(store_->Get(key, *data_)), CommonErr::InvalidArgument);
  ASSERT_EQ(int(store_->Exists(key)), CommonErr::InvalidArgument);
  ASSERT_EQ(int(store_->Delete(key)), CommonErr::InvalidArgument);
  ASSERT_EQ(int(store_->AsyncPut(key, *data_, nullptr)), CommonErr::InvalidArgument);
  ASSERT_EQ(int(store_->AsyncGet(key, *data_, nullptr)), CommonErr::InvalidArgument);
  ASSERT_EQ(int(store_->AsyncExists(key, nullptr)), CommonErr::InvalidArgument);
  ASSERT_EQ(int(store_->AsyncDelete(key, nullptr)), CommonErr::InvalidArgument);
  const size_t batch_count = 140;
  std::vector<std::string> key_vec;
  std::vector<simm::clnt::DataView> dv_vec;
  for (size_t i = 0; i < batch_count; i++) {
    key_vec.emplace_back(make_key("OutOfRange", 0));
    dv_vec.emplace_back(*data_);
  }
  // overwrite with one invalid length key
  key_vec.at(batch_count/2) = key;
  std::vector<int16_t> mput_rets = store_->MPut(key_vec, dv_vec);
  for (auto ret : mput_rets) {
    ASSERT_EQ(ret, CommonErr::InvalidArgument);
  }
  std::vector<int32_t> mget_rets = store_->MGet(key_vec, dv_vec);
  for (auto ret : mget_rets) {
    ASSERT_EQ(ret, CommonErr::InvalidArgument);
  }

  key_vec.at(batch_count/2) = make_key("OutOfRange", 0);
  dv_vec.clear();
  mput_rets.clear();
  // value too long
  key = make_key("OutOfRange", 1);
  constexpr size_t allocSize = 4 * kOneMB + 1;
  data_ = std::make_unique<Data>(store_->Allocate(allocSize));
  for (size_t i = 0; i < batch_count; i++) {
    dv_vec.emplace_back(*data_);
  }
  ASSERT_EQ(store_->Put(key, *data_), CommonErr::InvalidArgument);
  ASSERT_EQ(int(store_->AsyncPut(key, *data_, nullptr)), CommonErr::InvalidArgument);
  mput_rets = store_->MPut(key_vec, dv_vec);
  for (auto ret : mput_rets) {
    ASSERT_EQ(ret, CommonErr::InvalidArgument);
  }
}

TEST_F(LibsimmKV, PutOneGetOneAsync) {
  auto key = make_key("PutOneGetOneA", 0);
  auto data = data_->AsRef();
  std::copy(key.begin(), key.end(), data.begin());

  folly::Baton<> done_flag;
  auto must_success = [&done_flag](int err) {
    done_flag.post();
    ASSERT_TRUE(err == CommonErr::OK);
  };
  auto must_success_aget = [&done_flag](int err) {
    done_flag.post();
    // err here means value length got from dataserver
    ASSERT_TRUE(err > 0);
  };
  ASSERT_EQ(store_->AsyncPut(key, *data_, must_success), CommonErr::OK);
  done_flag.wait();
  done_flag.reset();

  ASSERT_EQ(store_->AsyncExists(key, must_success), CommonErr::OK);
  done_flag.wait();
  done_flag.reset();

  auto got = std::make_unique<Data>(store_->Allocate(kOneMB));
  ASSERT_EQ((store_->AsyncGet(key, *got, must_success_aget)), CommonErr::OK);
  done_flag.wait();
  done_flag.reset();

  auto got_ = got->AsRef();
  ASSERT_TRUE(std::equal(key.begin(), key.end(), got_.data(), got_.data() + key.size()));
}

TEST_F(LibsimmKV, PutManyGetManyAsync) {
  GTEST_SKIP() << "[BUG] : Skip this case due to unknown failure & coredump, need fix";
  constexpr size_t kManyGetPutAsync = 1000;

  folly::Baton<> done_flag;
  auto must_success = [&done_flag](int err) {
    done_flag.post();
    ASSERT_TRUE(!err);
  };
  for (auto i = 0ul; i < kManyGetPutAsync; ++i) {
    auto put = std::make_unique<Data>(store_->Allocate(kOneMB));
    auto put_ = put->AsRef();
    auto key = make_key("PutManyGetManyA", i);
    std::copy(key.begin(), key.end(), put_.begin());
    ASSERT_EQ(store_->AsyncPut(key, *put, must_success), CommonErr::OK);
    done_flag.wait();
    done_flag.reset();
  }

  std::atomic<int> remain_cnt{kManyGetPutAsync};
  std::vector<std::shared_ptr<Data>> gots;
  for (auto i = 0ul; i < kManyGetPutAsync; ++i) {
    auto got = std::make_shared<Data>(store_->Allocate(kOneMB));
    gots.emplace_back(got);
  }
  std::vector<std::string> results;
  for (auto i = 0ul; i < kManyGetPutAsync; ++i) {
    auto key = make_key("PutManyGetManyA", i);

    auto got = gots[i];
    auto get_callback = [&remain_cnt, &done_flag, &results, &i, &key, got](int err) {
      // counter--
      auto got_ = got->AsRef();
      std::string str(got_.data(), key.size());
      results.emplace_back(str);
      if (remain_cnt.fetch_sub(1) == 1) {
        // wait for last req done
        done_flag.post();
      }
      // err here means value length got from dataserver
      ASSERT_TRUE(err > 0);
    };
    ASSERT_TRUE((store_->AsyncGet(key, *got, get_callback)) == CommonErr::OK);
  }
  done_flag.wait();
  done_flag.reset();
  for (auto i = 0ul; i < kManyGetPutAsync; ++i) {
    auto key = make_key("PutManyGetManyA", i);
    auto got = results[i];
    ASSERT_TRUE(std::equal(key.begin(), key.end(), got.data(), got.data() + key.size()));
  }
}

}  // namespace clnt
}  // namespace simm

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
