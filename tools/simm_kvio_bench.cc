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

#include <errno.h>
#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <latch>
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include <gflags/gflags.h>

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/init/Init.h>

#include "common/errcode/errcode_def.h"
#include "common/logging/logging.h"
#include "simm/simm_common.h"
#include "simm/simm_kv.h"

using steady_clock_t = std::chrono::steady_clock;
using nano_ts = std::chrono::nanoseconds;

DEFINE_uint32(key_size, 20, "Key string length, default is 20 Bytes");
DEFINE_uint32(val_size, 8909, "Value length, default is 8.7 KB");
DEFINE_uint32(threads, 32, "threads number to call kv api concurrently");
DEFINE_uint32(run_time, 600, "tests run time in seconds");
DEFINE_uint32(key_num, 100000, "pre-prepare distinct key strings for every kvio worker thread");
DEFINE_uint32(key_num_prepare_get, 1000, "number of key-values pre-filled for get mode tests");
DEFINE_uint32(main_process_sleep_interval, 1000, "test main process sleep interval(in ms)");
DEFINE_string(mode, "get", "test modes : get, put, mget, mput, del, mixed....");
DEFINE_uint32(batchsize, 140, "requests number in one mget/mput calling");
DEFINE_bool(summary, true, "Only do summary stats print when whole test finishes");

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

struct ThreadStats {
  void add(nano_ts d) {
    uint32_t x = static_cast<uint32_t>(d.count());
    lat_samples_ns_.push_back(x);
    sum_lat_ns_ += x;
  }

  void merge(const ThreadStats &o) {
    ops_ += o.ops_;
    puts_ += o.puts_;
    gets_ += o.gets_;
    mputs_ += o.mputs_;
    mgets_ += o.mgets_;
    succs_ += o.succs_;
    fails_ += o.fails_;
    sum_lat_ns_ += o.sum_lat_ns_;
    lat_samples_ns_.insert(lat_samples_ns_.end(), o.lat_samples_ns_.begin(), o.lat_samples_ns_.end());
  }

  void reset() {
    ops_ = 0;
    puts_ = 0;
    gets_ = 0;
    mputs_ = 0;
    mgets_ = 0;
    succs_ = 0;
    fails_ = 0;
    sum_lat_ns_ = 0.0;
    lat_samples_ns_.clear();
  }

  uint64_t ops_{0};
  uint64_t puts_{0};
  uint64_t gets_{0};
  uint64_t mputs_{0};
  uint64_t mgets_{0};
  uint64_t succs_{0};
  uint64_t fails_{0};
  long double sum_lat_ns_{0.0};
  std::vector<uint32_t> lat_samples_ns_;
};

static std::string print_now_ts() {
  using namespace std::chrono;
  auto now = system_clock::now();
  auto ms = duration_cast<milliseconds>(now.time_since_epoch()) % 1000;

  std::time_t t = system_clock::to_time_t(now);
  struct tm tm_buf;
  localtime_r(&t, &tm_buf);  // POSIX thread-safe

  char time_buf[64];
  std::strftime(time_buf, sizeof(time_buf), "%Y-%m-%d %H:%M:%S", &tm_buf);

  char out[80];
  std::snprintf(out, sizeof(out), "%s.%03lld", time_buf, static_cast<long long>(ms.count()));
  return std::string(out);
}

static void print_stats(const ThreadStats &s,
                        uint64_t value_size,
                        uint32_t threads,
                        int run_time = 0,
                        bool summary_report = true,
                        bool batch_mode = false) {
  auto pct = [&](double p) {
    if (s.lat_samples_ns_.empty()) {
      return 0.0;
    }
    auto v = s.lat_samples_ns_;
    std::nth_element(v.begin(), v.begin() + static_cast<size_t>(p * (v.size() - 1)), v.end());
    return static_cast<double>(v.at(static_cast<size_t>(p * (v.size() - 1))) / 1e3);  // us
  };

  double mean_us = s.ops_ ? static_cast<double>(s.sum_lat_ns_ / s.ops_) / 1e3 : 0.0;
  double secs = static_cast<double>(run_time == 0 ? s.sum_lat_ns_ / 1e9 : run_time);  // accumulated IO seconds
  double ops_s = s.ops_ / secs;
  double Gbps = (ops_s * (batch_mode ? FLAGS_batchsize : 1) * value_size * 8.0) / (1024 * 1024 * 1024);

  if (summary_report) {
    std::cout << "+++++++++++++++++++++++++++++++++++++++++++++++++++++\n"
              << "ThreadID      : " << threads << "\n"
              << "IOPSCnt       : " << s.ops_ << "\n"
              << "GetCnt        : " << s.gets_ << "\n"
              << "PutCnt        : " << s.puts_ << "\n"
              << "MGetCnt       : " << s.mgets_ << "\n"
              << "MPutCnt       : " << s.mputs_ << "\n"
              << "SuccCnnt      : " << s.succs_ << "\n"
              << "failsCnt      : " << s.fails_ << "\n"
              << "IOPS          : " << ops_s << " ops/s\n"
              << "Thurput       : " << Gbps << " Gb/s\n"
              << "Latency(mean) : " << mean_us << " us\n"
              << "Latency(P50)  : " << pct(0.50) << " us\n"
              << "Latency(P95)  : " << pct(0.95) << " us\n"
              << "Latency(P99)  : " << pct(0.99) << " us\n"
              << "Latency(P999) : " << pct(0.999) << " us\n"
              << std::endl;
  } else {
    // periodic printing, e.g. 1s
    std::cout << "[" << print_now_ts() << "]"
              << "[" << FLAGS_mode << "]"
              << "[" << FLAGS_threads << "]"
              << " IOPSCnt:" << s.ops_ << " IOPS:" << ops_s << " Thurput(GB/s):" << (Gbps / 8)
              << " Lat(mean)(us):" << mean_us << " Lat(P50)(us):" << pct(0.50) << " Lat(P95)(us):" << pct(0.95)
              << " Lat(P99)(us):" << pct(0.99) << " Lat(P999)(us):" << pct(0.999) << std::endl;
  }
}

void usage() {
  std::cout << "simm_kvio_bench --mode=[get|put] --key_size=40 --val_size=4096 --threads=64 --run_time=180"
            << std::endl;
}

void check_args() {
  bool args_valid{true};

  if (FLAGS_mode != "get" && FLAGS_mode != "put") {
    MLOG_ERROR("Invalid test mode : {}", FLAGS_mode);
    args_valid = false;
  }

  if (!args_valid) {
    usage();
    std::exit(EINVAL);
  }
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

  MLOG_INFO("++++++++++++++++++++++++++++ Start SIMM_KVIO_BENCH ++++++++++++++++++++++++++++");
  MLOG_INFO("Test Args: key_size:{}B | val_size:{}B | threads:{} | run_time:{}s | mode:{}",
            FLAGS_key_size,
            FLAGS_val_size,
            FLAGS_threads,
            FLAGS_run_time,
            FLAGS_mode);

  auto test_binary_start_ts = steady_clock_t::now();

  // +++++++++++++++++++++++++++++++++++
  // Phase : env init
  // +++++++++++++++++++++++++++++++++++
  auto simm_kvstore = std::make_unique<simm::clnt::KVStore>();
  if (simm_kvstore == nullptr) {
    MLOG_ERROR("Make unique_ptr for simm KVStore failed, no memory!");
    std::exit(ENOMEM);
  }

  // needn't to init ClientMessenger Instance and MempoolBase Instance,
  // kvstore will init them internally

  folly::CPUThreadPoolExecutor thread_pool(FLAGS_threads);

  // +++++++++++++++++++++++++++++++++++
  // Phase : keys & values preparation
  // +++++++++++++++++++++++++++++++++++
  std::latch keys_preparation_latch{FLAGS_threads};
  std::vector<std::vector<std::string>> keys_vec(FLAGS_threads);
  // if it is pure get test, it's enought to prepare 1000 keys in advance
  FLAGS_key_num = FLAGS_mode == "get" ? FLAGS_key_num_prepare_get : FLAGS_key_num;
  for (uint32_t i = 0; i < FLAGS_threads; ++i) {
    MLOG_INFO("Start to reserve key string vector slots({}) for worker thread({})", FLAGS_key_num, i);
    keys_vec.at(i).reserve(FLAGS_key_num);
  }
  auto keys_preparation_worker = [&](uint32_t tid, uint32_t key_sz) {
    thread_local std::mt19937_64 rng{std::random_device{}()};
    std::uniform_int_distribution<uint64_t> dist(0, UINT64_MAX);
    for (uint32_t cnt = 0; cnt < FLAGS_key_num; ++cnt) {
      std::string s;
      s.resize(key_sz);
      uint32_t pos = 0;
      while (pos < key_sz) {
        uint64_t r = dist(rng);
        for (int j = 0; j < 10 && pos < key_sz; ++j) {
          s[pos++] = charset[r % charset_size];
          r /= charset_size;
        }
      }
      keys_vec.at(tid).emplace_back(std::move(s));
    }
    MLOG_INFO("Finish to reserve key string vector slots({}) for worker thread({})", FLAGS_key_num, tid);
    keys_preparation_latch.count_down();
  };
  for (uint32_t i = 0; i < FLAGS_threads; ++i) {
    thread_pool.add([i, &keys_preparation_worker] { keys_preparation_worker(i, FLAGS_key_size); });
  }

  keys_preparation_latch.wait();
  MLOG_INFO("Finish key preparation jobs for all threads");

  // +++++++++++++++++++++++++++++++++++
  // Phase : test
  // +++++++++++++++++++++++++++++++++++
  std::atomic<bool> stop_threads{false};
  std::vector<ThreadStats> test_stats_vec(FLAGS_threads);
  std::latch kvio_latch{FLAGS_threads};
  auto kvio_worker = [&](uint32_t tid, const std::string &test_mode = "put") {
    auto &thread_stat = test_stats_vec.at(tid);
    uint32_t key_idx = 0;
    auto simm_data = simm_kvstore->Allocate(FLAGS_val_size);
    simm::clnt::DataView simm_data_view(simm_data);
    std::string put_data_str(FLAGS_val_size, '0' + tid);
    std::memcpy(simm_data_view.AsRef().data(), put_data_str.c_str(), put_data_str.size());
    if (test_mode == "put") {
      while (!stop_threads.load() && key_idx < FLAGS_key_num) {
        auto kv_op_start = steady_clock_t::now();
        // sync call
        auto kv_ret = simm_kvstore->Put(keys_vec.at(tid).at(key_idx), simm_data_view);
        auto kv_op_elapsed = std::chrono::duration_cast<nano_ts>(steady_clock_t::now() - kv_op_start);
        thread_stat.add(kv_op_elapsed);
        thread_stat.ops_++;
        thread_stat.puts_++;
        if (kv_ret != 0) {
          thread_stat.fails_++;
          MLOG_ERROR("Failed Put op in thread({}), kv_ret:{}", tid, kv_ret);
        } else {
          thread_stat.succs_++;
        }
        key_idx++;
      }
    } else if (test_mode == "mput") {
      while (!stop_threads.load() && key_idx < FLAGS_key_num) {
        std::vector<std::string> key_vec;
        std::vector<simm::clnt::DataView> put_dv_vec;
        for (uint32_t n = 0; n < FLAGS_batchsize && key_idx < FLAGS_key_num; n++, key_idx++) {
          key_vec.emplace_back(keys_vec.at(tid).at(key_idx));
          put_dv_vec.emplace_back(simm_data_view);
        }
        auto kv_op_start = steady_clock_t::now();
        // sync call
        auto kv_ret = simm_kvstore->MPut(key_vec, put_dv_vec);
        auto kv_op_elapsed = std::chrono::duration_cast<nano_ts>(steady_clock_t::now() - kv_op_start);
        thread_stat.add(kv_op_elapsed);
        thread_stat.ops_++;
        thread_stat.mputs_++;
        bool mput_succ = true;
        for (uint32_t n = 0; n < kv_ret.size(); n++) {
          if (kv_ret.at(n) != 0) {
            MLOG_ERROR("Failed MPut op({}/{}) in thread({}), kv_ret:{}", n + 1, FLAGS_batchsize, tid, kv_ret.at(n));
            mput_succ = false;
          }
        }
        if (mput_succ) {
          thread_stat.succs_++;
        } else {
          thread_stat.fails_++;
        }
      }
    } else if (test_mode == "get") {
      // put in some data for get ops
      while (key_idx < FLAGS_key_num) {
        auto kv_ret = simm_kvstore->Put(keys_vec.at(tid).at(key_idx), simm_data_view);
        if (kv_ret != 0) {
          thread_stat.fails_++;
          MLOG_ERROR(
              "Failed to put key({}) for get test in thread({}), kv_ret:{}", keys_vec.at(tid).at(key_idx), tid, kv_ret);
        } else {
          thread_stat.succs_++;
        }
        thread_stat.puts_++;
        key_idx++;
      }
      MLOG_INFO("Pre-put actions done, start get tests...");
      while (!stop_threads.load()) {
        key_idx = key_idx >= FLAGS_key_num ? 0 : key_idx;
        // CAUTION: get IO need to allocate MR from transport mempool, that's one performance loss point
        // auto kv_op_start = steady_clock_t::now();
        auto simm_get_data = simm_kvstore->Allocate(FLAGS_val_size);
        simm::clnt::DataView simm_get_data_view(simm_get_data);
        // sync call
        auto kv_op_start = steady_clock_t::now();
        auto kv_ret = simm_kvstore->Get(keys_vec.at(tid).at(key_idx), simm_get_data_view);
        auto kv_op_elapsed = std::chrono::duration_cast<nano_ts>(steady_clock_t::now() - kv_op_start);
        thread_stat.add(kv_op_elapsed);
        thread_stat.ops_++;
        thread_stat.gets_++;
        if (kv_ret != 0) {
          thread_stat.fails_++;
          MLOG_ERROR("Failed Get op in thread({}), kv_ret:{}", tid, kv_ret);
        } else {
          thread_stat.succs_++;
        }
        key_idx++;
      }
    } else if (test_mode == "mget") {
      // put in some data for get ops
      while (key_idx < FLAGS_key_num) {
        auto kv_ret = simm_kvstore->Put(keys_vec.at(tid).at(key_idx), simm_data_view);
        if (kv_ret != 0) {
          thread_stat.fails_++;
          MLOG_ERROR(
              "Failed to put key({}) for get test in thread({}), kv_ret:{}", keys_vec.at(tid).at(key_idx), tid, kv_ret);
        } else {
          thread_stat.succs_++;
        }
        thread_stat.puts_++;
        key_idx++;
      }
      MLOG_INFO("Pre-put actions done, start get tests...");
      while (!stop_threads.load()) {
        std::vector<std::string> key_vec;
        std::vector<simm::clnt::DataView> get_dv_vec;
        for (uint32_t n = 0; n < FLAGS_batchsize; n++, key_idx++) {
          key_idx = key_idx >= FLAGS_key_num ? 0 : key_idx;
          key_vec.emplace_back(keys_vec.at(tid).at(key_idx));
          auto simm_get_data = simm_kvstore->Allocate(FLAGS_val_size);
          get_dv_vec.emplace_back(simm_get_data);
        }
        auto kv_op_start = steady_clock_t::now();
        // sync call
        auto kv_ret = simm_kvstore->MGet(key_vec, get_dv_vec);
        auto kv_op_elapsed = std::chrono::duration_cast<nano_ts>(steady_clock_t::now() - kv_op_start);
        thread_stat.add(kv_op_elapsed);
        thread_stat.ops_++;
        thread_stat.mgets_++;
        bool mget_succ = true;
        for (uint32_t n = 0; n < kv_ret.size(); n++) {
          if (kv_ret.at(n) <= 0) {
            MLOG_ERROR("Failed MGet op({}/{}) in thread({}), kv_ret:{}", n + 1, FLAGS_batchsize, tid, kv_ret.at(n));
            mget_succ = false;
          }
        }
        if (mget_succ) {
          thread_stat.succs_++;
        } else {
          thread_stat.fails_++;
        }
      }
    } else {
      MLOG_ERROR("Unsupported test mode:{}, exit kvio work thread:{}", test_mode, tid);
    }
    kvio_latch.count_down();
  };

  auto test_ts_end = test_binary_start_ts + std::chrono::seconds(FLAGS_run_time);
  auto io_test_start_ts = steady_clock_t::now();

  for (uint32_t i = 0; i < FLAGS_threads; ++i) {
    thread_pool.add([i, &kvio_worker] { kvio_worker(i, FLAGS_mode); });
  }

  while (!kvio_latch.try_wait() && steady_clock_t::now() < test_ts_end) {
    std::this_thread::sleep_for(std::chrono::milliseconds(FLAGS_main_process_sleep_interval));
    if (!FLAGS_summary) {
      ThreadStats aggregated_stats;
      for (auto &stat_entry : test_stats_vec) {
        aggregated_stats.merge(stat_entry);
        // reset every stats object for next round
        stat_entry.reset();
      }
      print_stats(aggregated_stats,
                  FLAGS_val_size,
                  0,
                  static_cast<int>(FLAGS_main_process_sleep_interval / 1000),
                  FLAGS_summary,
                  (FLAGS_mode == "mget" || FLAGS_mode == "mput"));
    }
  }

  stop_threads.store(true);
  kvio_latch.wait();
  MLOG_INFO("All kvio threads jobs finished");

  auto io_test_elapsed = std::chrono::duration_cast<std::chrono::seconds>(steady_clock_t::now() - io_test_start_ts);

  MLOG_INFO("Test run time({}s) used up, all worker threads stopped, IO test duration({}s)",
            FLAGS_run_time,
            static_cast<int>(io_test_elapsed.count()));

  // +++++++++++++++++++++++++++++++++++
  // Phase : stats aggregation and print
  // +++++++++++++++++++++++++++++++++++
  if (FLAGS_summary) {
    ThreadStats aggregated_stats;
    uint32_t tid = 1;
    for (const auto &stat_entry : test_stats_vec) {
      aggregated_stats.merge(stat_entry);
      print_stats(stat_entry, FLAGS_val_size, tid);
      tid++;
    }
    print_stats(aggregated_stats, FLAGS_val_size, 0, static_cast<int>(io_test_elapsed.count()));
  }

  return 0;
}
