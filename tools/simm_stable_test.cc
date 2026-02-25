/*
 * Copyright (c) 2026 Scitix Tech PTE. LTD.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <latch>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include <gflags/gflags.h>

#include <folly/Random.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/init/Init.h>

#include "common/errcode/errcode_def.h"
#include "common/logging/logging.h"
#include "simm/simm_common.h"
#include "simm/simm_kv.h"

using steady_clock_t = std::chrono::steady_clock;
using nano_ts = std::chrono::nanoseconds;

constexpr size_t ONE_KB = 1024;
constexpr size_t ONE_MB = 1024 * ONE_KB;
constexpr size_t KEY_LEN_LIMIT = 256;
constexpr size_t VAL_LEN_LIMIT = 4 * ONE_MB;
constexpr size_t THREAD_NUM_LIMIT = 256;
constexpr size_t IODEPTH_LIMIT = 2048;

DEFINE_uint32(keylimit, 256, "user key lenght limit, [1B, limit]");
DEFINE_uint32(vallimit, 4 * ONE_MB, "user val lenght limit, [1B, limit]");
DEFINE_uint32(threads, 32, "threads number to call kv api concurrently");
DEFINE_uint32(time, 60, "tests run time in seconds");
DEFINE_uint32(iodepth, 64, "iodepth for async io in every work thread");
DEFINE_uint32(delratio, 20, "possibility to delete kv in one sync/async task, default is 20%");
DEFINE_uint32(oputratio, 5, "possibility to do overwrite put operation, default is 0%, namely disabled");
DEFINE_string(iomode, "sync", "sync or async, other modes are invalid");
DEFINE_bool(fixed_kvsize, false, "fixed key and value size by 'keylimit' 'vallimit', or not(use random sizes)");
DEFINE_bool(batch_mode, false, "use batch mode apis or not");  // now only cover mget/mput apis
DEFINE_uint32(batch_size, 140, "requests number in batch apis, only work when batch_mode is true");

DECLARE_string(cm_primary_node_ip);  // specify primary cm ip for tests

DECLARE_LOG_MODULE("simm_client");

// FIXME(ytji) :
// 1. cover \ and "
// 2. cover special symbols, e.g. ￥《 》 ...
static const char charset[] =
    "abcdefghijklmnopqrstuvwxyz"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "0123456789"
    "~.!@#$%^&*([{}])_-+=/><,?:;'";
static constexpr size_t charset_size = sizeof(charset) - 1;

std::string convert_to_readable_size(uint64_t bytes_num) {
  static const char *units[] = {"B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"};
  double size = static_cast<double>(bytes_num);
  int unit_index = 0;

  while (size >= 1024.0 && unit_index < (int)(sizeof(units) / sizeof(units[0])) - 1) {
    size /= 1024.0;
    ++unit_index;
  }

  std::ostringstream oss;
  oss << std::fixed << std::setprecision(2) << size << " " << units[unit_index];
  return oss.str();
}

struct ThreadStats {
  void add([[maybe_unused]] nano_ts d) {
    // no latency metrics needed in stable test
  }

  void merge(const ThreadStats &o) {
    get_ += o.get_;
    get_fails_ += o.get_fails_;
    get_succs_ += o.get_succs_;
    mget_ += o.mget_;
    mget_fails_ += o.mget_fails_;
    mget_succs_ += o.mget_succs_;
    put_ += o.put_;
    put_fails_ += o.put_fails_;
    put_succs_ += o.put_succs_;
    mput_ += o.mput_;
    mput_fails_ += o.mput_fails_;
    mput_succs_ += o.mput_succs_;
    overwrite_put_ += o.overwrite_put_;
    overwrite_put_fails_ += o.overwrite_put_fails_;
    overwrite_put_succs_ += o.overwrite_put_succs_;
    exists_ += o.exists_;
    exists_fails_ += o.exists_fails_;
    exists_succs_ += o.exists_succs_;
    mexists_ += o.mexists_;
    mexists_fails_ += o.mexists_fails_;
    mexists_succs_ += o.mexists_succs_;
    del_ += o.del_;
    del_fails_ += o.del_fails_;
    del_succs_ += o.del_succs_;
    data_match_ += o.data_match_;
    data_mismatch_ += o.data_mismatch_;
    put_size_bytes += o.put_size_bytes;
    get_size_bytes += o.get_size_bytes;
  }

  uint64_t get_{0};
  uint64_t get_fails_{0};
  uint64_t get_succs_{0};
  uint64_t mget_{0};
  uint64_t mget_fails_{0};
  uint64_t mget_succs_{0};
  uint64_t put_{0};
  uint64_t put_fails_{0};
  uint64_t put_succs_{0};
  uint64_t mput_{0};
  uint64_t mput_fails_{0};
  uint64_t mput_succs_{0};
  uint64_t overwrite_put_{0};
  uint64_t overwrite_put_fails_{0};
  uint64_t overwrite_put_succs_{0};
  uint64_t exists_{0};
  uint64_t exists_fails_{0};
  uint64_t exists_succs_{0};
  uint64_t mexists_{0};
  uint64_t mexists_fails_{0};
  uint64_t mexists_succs_{0};
  uint64_t del_{0};
  uint64_t del_fails_{0};
  uint64_t del_succs_{0};
  uint64_t data_match_{0};
  uint64_t data_mismatch_{0};
  uint64_t put_size_bytes{0};
  uint64_t get_size_bytes{0};
};

void print_stats(const ThreadStats &s, uint32_t threads) {
  std::cout << "++++++++++++++++++++++++++++ STABLE TEST REPORT +++++++++++++++++++++++++\n"
            << "ThreadID      : " << threads << "\n"
            << "GetCnt        : " << s.get_ << "\n"
            << "GetFails      : " << s.get_fails_ << "\n"
            << "GetSuccs      : " << s.get_succs_ << "\n"
            << "MGetCnt       : " << s.mget_ << "\n"
            << "MGetFails     : " << s.mget_fails_ << "\n"
            << "MGetSuccs     : " << s.mget_succs_ << "\n"
            << "PutCnt        : " << s.put_ << "\n"
            << "PutFails      : " << s.put_fails_ << "\n"
            << "PutSuccs      : " << s.put_succs_ << "\n"
            << "MPutCnt       : " << s.mput_ << "\n"
            << "MPutFails     : " << s.mput_fails_ << "\n"
            << "MPutSuccs     : " << s.mput_succs_ << "\n"
            << "OPutCnt       : " << s.overwrite_put_ << "\n"
            << "OPutFails     : " << s.overwrite_put_fails_ << "\n"
            << "OPutSuccs     : " << s.overwrite_put_succs_ << "\n"
            << "ExistCnt      : " << s.exists_ << "\n"
            << "ExistFails    : " << s.exists_fails_ << "\n"
            << "ExistSuccs    : " << s.exists_succs_ << "\n"
            << "MExistCnt     : " << s.mexists_ << "\n"
            << "MExistFails   : " << s.mexists_fails_ << "\n"
            << "MExistSuccs   : " << s.mexists_succs_ << "\n"
            << "DelCnt        : " << s.del_ << "\n"
            << "DelFails      : " << s.del_fails_ << "\n"
            << "DelSuccs      : " << s.del_succs_ << "\n"
            << "DataMatch     : " << s.data_match_ << "\n"
            << "DataMismatch  : " << s.data_mismatch_ << "\n"
            << "PutSize       : " << convert_to_readable_size(s.put_size_bytes) << "\n"
            << "GetSize       : " << convert_to_readable_size(s.get_size_bytes) << "\n"
            << std::endl;
}

void usage() {
  std::cout
      << "simm_stable_test --iomode=[sync|async] --keylimit=100 --vallimit=8192 --threads=32 --iodepth=128 --time=3600"
      << std::endl;
}

void check_args() {
  bool args_valid{true};

  if (FLAGS_keylimit < 1 || FLAGS_keylimit > KEY_LEN_LIMIT) {
    MLOG_ERROR("Invalid key size upper limit : {}B, should in [1B, {}B]", FLAGS_keylimit, KEY_LEN_LIMIT);
    args_valid = false;
  } else if (FLAGS_vallimit < 1 || FLAGS_vallimit > VAL_LEN_LIMIT) {
    MLOG_ERROR("Invalid val size upper limit : {}B, should in [1B, {}B]", FLAGS_vallimit, VAL_LEN_LIMIT);
    args_valid = false;
  } else if (FLAGS_threads < 1 || FLAGS_threads > THREAD_NUM_LIMIT) {
    MLOG_ERROR("Invalid threads number : {}, should in [1, {}]", FLAGS_threads, THREAD_NUM_LIMIT);
    args_valid = false;
  } else if (FLAGS_iodepth < 1 || FLAGS_iodepth > IODEPTH_LIMIT) {
    MLOG_ERROR("Invalid iodepth : {}, should in [1, {}]", FLAGS_iodepth, IODEPTH_LIMIT);
    args_valid = false;
  } else if (FLAGS_iomode != "sync" && FLAGS_iomode != "async") {
    MLOG_ERROR("Invalid iomode type : {}, should be sync or async", FLAGS_iomode);
    args_valid = false;
  }

  if (!args_valid) {
    usage();
    std::exit(EINVAL);
  }
}

// used to generate key or value with random contents
std::string generate_random_string(uint32_t sz_limit, bool is_key = true) {
  // thread_local std::mt19937_64 rng{std::random_device{}()};
  // std::uniform_int_distribution<uint64_t> dist(0, UINT64_MAX);
  std::string s;
  uint32_t pos = 0;
  // uint32_t limit = folly::Random::rand32(1, sz_limit + 1);
  s.resize(sz_limit);
  if (is_key) {
    while (pos < sz_limit) {
      // uint64_t r = dist(rng);
      uint64_t r = folly::Random::rand64();
      for (int j = 0; j < 10 && pos < sz_limit; ++j) {
        s[pos++] = charset[r % charset_size];
        r /= charset_size;
      }
    }
  } else {  // generate value
    const size_t step = sizeof(uint64_t);
    while (pos + step <= sz_limit) {
      uint64_t r = folly::Random::rand64();
      std::memcpy(&s[pos], &r, step);
      pos += step;
    }
    if (pos < sz_limit) {
      uint64_t r = folly::Random::rand64();
      std::memcpy(&s[pos], &r, sz_limit - pos);
    }
  }
  return s;
}

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  folly::Init init(&argc, &argv);

#ifdef NDEBUG
  simm::logging::LogConfig clnt_log_config = simm::logging::LogConfig{"/tmp/simm_clnt.log", "INFO"};
#else
  simm::logging::LogConfig clnt_log_config = simm::logging::LogConfig{"/tmp/simm_clnt.log", "DEBUG"};
#endif
  simm::logging::LoggerManager::Instance().UpdateConfig("simm_client", clnt_log_config);

  check_args();

  MLOG_INFO("++++++++++++++++++++++++++++++++++++++++ Start SIMM_STABLE_TEST ++++++++++++++++++++++++++++++++++++++++");
  MLOG_INFO(
      "Test Args: key_size_limit:{}B | val_size_limit:{}B | threads:{} | iomode:{} | fixed_kv:{} | batchmode:{} | "
      "batchsize:{}",
      FLAGS_keylimit,
      FLAGS_vallimit,
      FLAGS_threads,
      FLAGS_iomode,
      FLAGS_fixed_kvsize ? "T" : "F",
      FLAGS_batch_mode ? "T" : "F",
      FLAGS_batch_size);

  auto test_binary_start_ts = steady_clock_t::now();

  // +++++++++++++++++++++++++++++++++++
  // Phase : env init
  // +++++++++++++++++++++++++++++++++++
  auto simm_kvstore = std::make_unique<simm::clnt::KVStore>();
  if (simm_kvstore == nullptr) {
    MLOG_ERROR("Make unique_ptr for simm KVStore failed, no memory!");
    std::exit(ENOMEM);
  }

  // test thread pool init
  folly::CPUThreadPoolExecutor thread_pool(FLAGS_threads);

  // +++++++++++++++++++++++++++++++++++
  // Phase : test
  // +++++++++++++++++++++++++++++++++++
  std::atomic<bool> stop_threads{false};
  std::vector<ThreadStats> test_stats_vec(FLAGS_threads);
  std::latch kvio_latch{FLAGS_threads};

  auto sync_io_task = [&](uint32_t tid) -> void {
    auto &thread_stat = test_stats_vec[tid];
    while (!stop_threads.load()) {
      // generate key & value for current task
      size_t k_sz = FLAGS_fixed_kvsize ? FLAGS_keylimit : folly::Random::rand32(1, FLAGS_keylimit + 1);
      size_t v_sz = FLAGS_fixed_kvsize ? FLAGS_vallimit : folly::Random::rand32(1, FLAGS_vallimit + 1);
      std::vector<std::string> key_vec;
      std::vector<simm::clnt::Data> data_vec;
      std::vector<simm::clnt::DataView> dataview_vec;
      size_t kv_cnt = FLAGS_batch_mode ? FLAGS_batch_size : 1;
      for (size_t i = 0; i < kv_cnt; i++) {
        auto k = generate_random_string(k_sz);
        key_vec.emplace_back(k);
        auto v = simm_kvstore->Allocate(v_sz);
        auto random_value_str = generate_random_string(v_sz, false);
        std::memcpy(v.AsRef().data(), random_value_str.c_str(), v_sz);
        simm::clnt::DataView dv(v);
        dataview_vec.emplace_back(dv);
        data_vec.emplace_back(std::move(v));
      }

      if (FLAGS_batch_mode) {  // batch mode apis
        auto bput_rets = simm_kvstore->MPut(key_vec, dataview_vec);
        bool bput_succeed = true;
        uint32_t idx = 0;
        for (const auto ret : bput_rets) {
          if (ret == CommonErr::OK) {
            thread_stat.put_succs_++;
            thread_stat.put_size_bytes += v_sz;
          } else {
            thread_stat.put_fails_++;
            bput_succeed = false;
            MLOG_ERROR("Failed to put key({}) in sync mput IO, kv_ret:{}", key_vec.at(idx), ret);
          }
          idx++;
        }
        thread_stat.put_ += FLAGS_batch_size;
        thread_stat.mput_++;
        if (bput_succeed) {
          thread_stat.mput_succs_++;
        } else {
          thread_stat.mput_fails_++;
        }

        // FIXME(ytji) : skip MExists test for api is unavailable now
        for (const auto &k : key_vec) {
          auto exist_ret = simm_kvstore->Exists(k);
          thread_stat.exists_++;
          if (exist_ret == CommonErr::OK) {
            thread_stat.exists_succs_++;
          } else {
            thread_stat.exists_fails_++;
            MLOG_ERROR("Failed to lookup key({}) in sync exists IO, kv_ret:{}", k, exist_ret);
          }
        }

        // FIXME(ytji) : skip overwrite IO in batch scenarios

        std::vector<simm::clnt::Data> mget_data_vec;
        std::vector<simm::clnt::DataView> mget_dataview_vec;
        for (size_t i = 0; i < kv_cnt; i++) {
          auto mget_v = simm_kvstore->Allocate(v_sz);
          simm::clnt::DataView mget_dv(mget_v);
          mget_dataview_vec.emplace_back(mget_dv);
          mget_data_vec.emplace_back(std::move(mget_v));
        }
        auto bget_rets = simm_kvstore->MGet(key_vec, mget_dataview_vec);
        bool bget_succeed = true;
        idx = 0;
        for (const auto ret : bget_rets) {
          if (ret >= 0) {
            thread_stat.get_succs_++;
            thread_stat.get_size_bytes += v_sz;
            // do mem check for every succeed get op
            if (std::memcmp(data_vec.at(idx).AsRef().data(), mget_dataview_vec.at(idx).AsRef().data(), v_sz) != 0) {
              thread_stat.data_mismatch_++;
              MLOG_ERROR("Data mismatch for key({}) in sync mget IO", key_vec.at(idx));
            } else {
              thread_stat.data_match_++;
            }
          } else {
            thread_stat.get_fails_++;
            bget_succeed = false;
            MLOG_ERROR("Failed to get key({}) in sync mget IO, kv_ret:{}", key_vec.at(idx), ret);
          }
          idx++;
        }
        thread_stat.get_ += FLAGS_batch_size;
        thread_stat.mget_++;
        if (bget_succeed) {
          thread_stat.mget_succs_++;
        } else {
          thread_stat.mget_fails_++;
        }

        if (folly::Random::rand32(0, 100) < FLAGS_delratio) {
          // FIXME(ytji) : skip MDelete test for api is unavailable now
          for (const auto &k : key_vec) {
            auto del_ret = simm_kvstore->Delete(k);
            thread_stat.del_++;
            if (del_ret != 0) {
              thread_stat.del_fails_++;
              MLOG_ERROR("Failed to delete key({}) in sync delete IO, kv_ret:{}", k, del_ret);
            } else {
              thread_stat.del_succs_++;
            }
          }
        }
      } else {
        int32_t kv_ret = simm_kvstore->Put(key_vec.at(0), dataview_vec.at(0));
        thread_stat.put_++;
        if (kv_ret != 0) {
          thread_stat.put_fails_++;
          MLOG_ERROR("Failed to put key({}) in sync put IO, kv_ret:{}", key_vec.at(0), kv_ret);
          continue;
        } else {
          thread_stat.put_succs_++;
          thread_stat.put_size_bytes += v_sz;
        }

        kv_ret = simm_kvstore->Exists(key_vec.at(0));
        thread_stat.exists_++;
        if (kv_ret != 0) {
          thread_stat.exists_fails_++;
          MLOG_ERROR("Failed to lookup key({}) in sync exists IO, kv_ret:{}", key_vec.at(0), kv_ret);
          continue;
        } else {
          thread_stat.exists_succs_++;
        }

        if (folly::Random::rand32(0, 100) < FLAGS_oputratio) {
          size_t overwrite_v_sz = folly::Random::rand32(1, FLAGS_vallimit + 1);
          auto overwrite_v = simm_kvstore->Allocate(overwrite_v_sz);
          auto random_overwrite_value_str = generate_random_string(overwrite_v_sz, false);
          std::memcpy(overwrite_v.AsRef().data(), random_overwrite_value_str.c_str(), overwrite_v_sz);
          simm::clnt::DataView overwrite_dv(overwrite_v);
          // overwrite put
          kv_ret = simm_kvstore->Put(key_vec.at(0), overwrite_dv);
          thread_stat.overwrite_put_++;
          if (kv_ret != 0) {
            thread_stat.overwrite_put_fails_++;
            MLOG_ERROR("Failed to overwrite key({}) in sync put IO, kv_ret:{}", key_vec.at(0), kv_ret);
            continue;
          } else {
            thread_stat.overwrite_put_succs_++;
            thread_stat.put_size_bytes += overwrite_v_sz;
            // move overwrite kv size and data buffer for later data check
            v_sz = overwrite_v_sz;
            data_vec.at(0) = std::move(overwrite_v);
          }
        }

        auto get_v = simm_kvstore->Allocate(v_sz);
        simm::clnt::DataView get_dv(get_v);
        kv_ret = simm_kvstore->Get(key_vec.at(0), get_dv);
        thread_stat.get_++;
        // Get() will return kv length or error codes( < 0)
        if (kv_ret <= 0) {
          thread_stat.get_fails_++;
          MLOG_ERROR("Failed to get key({}) in sync get IO, kv_ret:{}", key_vec.at(0), kv_ret);
          continue;
        } else {
          thread_stat.get_succs_++;
          thread_stat.get_size_bytes += v_sz;
        }

        if (std::memcmp(data_vec.at(0).AsRef().data(), get_v.AsRef().data(), v_sz) != 0) {
          thread_stat.data_mismatch_++;
          MLOG_ERROR("Data mismatch for key({}) in sync_io_task", key_vec.at(0));
        } else {
          thread_stat.data_match_++;
        }

        if (folly::Random::rand32(0, 100) < FLAGS_delratio) {
          kv_ret = simm_kvstore->Delete(key_vec.at(0));
          thread_stat.del_++;
          if (kv_ret != 0) {
            thread_stat.del_fails_++;
            MLOG_ERROR("Failed to delete key({}) in sync delete IO, kv_ret:{}", key_vec.at(0), kv_ret);
          } else {
            thread_stat.del_succs_++;
          }
        }
      }  // else non-batch mode apis
    }
    kvio_latch.count_down();
    MLOG_INFO("Thead{} for sync io mode exits", tid);
  };
  [[maybe_unused]] auto async_io_task = [&]() -> void {
    // TODO(ytji): to be added
  };

  auto test_ts_end = test_binary_start_ts + std::chrono::seconds(FLAGS_time);
  auto io_test_start_ts = steady_clock_t::now();

  for (uint32_t i = 0; i < FLAGS_threads; ++i) {
    if (FLAGS_iomode == "sync") {
      MLOG_INFO("Add sync io task to thread pool, thread_id:{}", i);
      thread_pool.add([i, &sync_io_task] { sync_io_task(i); });
    } else if (FLAGS_iomode == "async") {
      // TODO
    }
  }

  while (!kvio_latch.try_wait() && steady_clock_t::now() < test_ts_end) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  stop_threads.store(true);
  kvio_latch.wait();
  MLOG_INFO("All kvio threads jobs finished");

  auto io_test_elapsed = std::chrono::duration_cast<std::chrono::seconds>(steady_clock_t::now() - io_test_start_ts);

  MLOG_INFO("Test run time({}s) used up, all worker threads stopped, IO test duration({}s)",
            FLAGS_time,
            static_cast<int>(io_test_elapsed.count()));

  // +++++++++++++++++++++++++++++++++++
  // Phase : stats aggregation and print
  // +++++++++++++++++++++++++++++++++++
  ThreadStats aggregated_stats;
  [[maybe_unused]] uint32_t tid = 1;
  for (const auto &stat_entry : test_stats_vec) {
    aggregated_stats.merge(stat_entry);
    // print_stats(stat_entry, tid);
    // tid++;
  }
  print_stats(aggregated_stats, 0);

  return 0;
}
