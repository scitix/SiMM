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
#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <gflags/gflags.h>

#include <folly/Random.h>
#include <folly/init/Init.h>

#include "common/errcode/errcode_def.h"
#include "common/logging/logging.h"
#include "simm/simm_common.h"
#include "simm/simm_kv.h"
#include "simm_stable_test_utils.h"

using steady_clock_t = std::chrono::steady_clock;
using micro_ts = std::chrono::microseconds;

DECLARE_string(cm_primary_node_ip);

namespace {

constexpr size_t ONE_KB = 1024;
constexpr size_t ONE_MB = 1024 * ONE_KB;
constexpr size_t KEY_LEN_LIMIT = 256;
constexpr size_t VAL_LEN_LIMIT = 4 * ONE_MB;
constexpr size_t THREAD_NUM_LIMIT = 256;
constexpr size_t IODEPTH_LIMIT = 2048;
constexpr uint32_t KEYSPACE_LIMIT = 1'000'000;

DEFINE_uint32(keylimit, 256, "user key length limit, [1B, limit]");
DEFINE_uint32(vallimit, 4 * ONE_MB, "user value length limit, [1B, limit]");
DEFINE_uint32(threads, 32, "threads number to call kv api concurrently");
DEFINE_uint32(time, 60, "test run time in seconds");
DEFINE_uint32(iodepth, 64, "iodepth for async io in every work thread");
DEFINE_uint32(delratio, 10, "delete ratio in workload mix, percent");
DEFINE_uint32(oputratio, 15, "overwrite put ratio when choosing put on an existing key, percent");
DEFINE_uint32(getratio, 45, "get ratio in workload mix, percent");
DEFINE_uint32(putratio, 35, "put ratio in workload mix, percent");
DEFINE_uint32(existsratio, 10, "exists ratio in workload mix, percent");
DEFINE_string(iomode, "sync", "sync or async");
DEFINE_bool(fixed_kvsize, false, "fixed key and value size by keylimit and vallimit");
DEFINE_bool(batch_mode, false, "use batch mode apis or not, only supported in sync mode");
DEFINE_uint32(batch_size, 140, "requests number in batch apis when batch_mode is true");
DEFINE_uint32(keyspace_per_thread,
              10000,
              "logical keyspace size owned by each worker thread; reused to build long-running stable workload");
DEFINE_uint32(report_interval_inSecs, 10, "periodic report interval in seconds");
DEFINE_bool(strict_verify_exists,
            false,
            "if true, treat async/sync exists miss as failure; if false, count expected miss as success");

DECLARE_LOG_MODULE("simm_client");

std::atomic<bool> g_stop_requested{false};

void StopSignalHandler(int /*sig*/) {
  g_stop_requested.store(true, std::memory_order_release);
}

std::string convert_to_readable_size(uint64_t bytes_num) {
  static const char *units[] = {"B", "KB", "MB", "GB", "TB", "PB", "EB"};
  double size = static_cast<double>(bytes_num);
  size_t unit_index = 0;
  while (size >= 1024.0 && unit_index + 1 < std::size(units)) {
    size /= 1024.0;
    ++unit_index;
  }

  std::ostringstream oss;
  oss << std::fixed << std::setprecision(2) << size << " " << units[unit_index];
  return oss.str();
}

struct LatencyStats {
  static constexpr std::array<uint64_t, 15> kBucketUpperUs =
      {50, 100, 200, 500, 1000, 2000, 5000, 10000, 20000, 50000, 100000, 200000, 500000, 1000000, UINT64_MAX};

  void add(micro_ts d) {
    const auto us = static_cast<uint64_t>(std::max<int64_t>(0, d.count()));
    ++count_;
    total_us_ += us;
    min_us_ = std::min(min_us_, us);
    max_us_ = std::max(max_us_, us);
    for (size_t i = 0; i < kBucketUpperUs.size(); ++i) {
      if (us <= kBucketUpperUs[i]) {
        ++bucket_counts_[i];
        break;
      }
    }
  }

  void merge(const LatencyStats &o) {
    count_ += o.count_;
    total_us_ += o.total_us_;
    min_us_ = std::min(min_us_, o.min_us_);
    max_us_ = std::max(max_us_, o.max_us_);
    for (size_t i = 0; i < bucket_counts_.size(); ++i) {
      bucket_counts_[i] += o.bucket_counts_[i];
    }
  }

  double avg_us() const { return count_ == 0 ? 0.0 : static_cast<double>(total_us_) / static_cast<double>(count_); }

  uint64_t percentile(double pct) const {
    if (count_ == 0) {
      return 0;
    }
    const uint64_t target = static_cast<uint64_t>(std::ceil(static_cast<double>(count_) * pct));
    uint64_t seen = 0;
    for (size_t i = 0; i < bucket_counts_.size(); ++i) {
      seen += bucket_counts_[i];
      if (seen >= target) {
        return kBucketUpperUs[i];
      }
    }
    return max_us_;
  }

  uint64_t count_{0};
  uint64_t total_us_{0};
  uint64_t min_us_{std::numeric_limits<uint64_t>::max()};
  uint64_t max_us_{0};
  std::array<uint64_t, kBucketUpperUs.size()> bucket_counts_{};
};

struct ThreadStats {
  void merge(const ThreadStats &o) {
    put_ += o.put_;
    put_fails_ += o.put_fails_;
    put_succs_ += o.put_succs_;
    overwrite_put_ += o.overwrite_put_;
    overwrite_put_fails_ += o.overwrite_put_fails_;
    overwrite_put_succs_ += o.overwrite_put_succs_;
    get_ += o.get_;
    get_fails_ += o.get_fails_;
    get_succs_ += o.get_succs_;
    exists_ += o.exists_;
    exists_fails_ += o.exists_fails_;
    exists_succs_ += o.exists_succs_;
    del_ += o.del_;
    del_fails_ += o.del_fails_;
    del_succs_ += o.del_succs_;
    mput_ += o.mput_;
    mput_fails_ += o.mput_fails_;
    mput_succs_ += o.mput_succs_;
    mget_ += o.mget_;
    mget_fails_ += o.mget_fails_;
    mget_succs_ += o.mget_succs_;
    data_match_ += o.data_match_;
    data_mismatch_ += o.data_mismatch_;
    expected_miss_ += o.expected_miss_;
    submit_fails_ += o.submit_fails_;
    put_size_bytes += o.put_size_bytes;
    get_size_bytes += o.get_size_bytes;
    latency_.merge(o.latency_);
  }

  uint64_t total_ops() const { return put_ + overwrite_put_ + get_ + exists_ + del_ + mput_ + mget_; }

  uint64_t total_failures() const {
    return put_fails_ + overwrite_put_fails_ + get_fails_ + exists_fails_ + del_fails_ + mput_fails_ + mget_fails_ +
           submit_fails_ + data_mismatch_;
  }

  uint64_t put_{0};
  uint64_t put_fails_{0};
  uint64_t put_succs_{0};
  uint64_t overwrite_put_{0};
  uint64_t overwrite_put_fails_{0};
  uint64_t overwrite_put_succs_{0};
  uint64_t get_{0};
  uint64_t get_fails_{0};
  uint64_t get_succs_{0};
  uint64_t exists_{0};
  uint64_t exists_fails_{0};
  uint64_t exists_succs_{0};
  uint64_t del_{0};
  uint64_t del_fails_{0};
  uint64_t del_succs_{0};
  uint64_t mput_{0};
  uint64_t mput_fails_{0};
  uint64_t mput_succs_{0};
  uint64_t mget_{0};
  uint64_t mget_fails_{0};
  uint64_t mget_succs_{0};
  uint64_t data_match_{0};
  uint64_t data_mismatch_{0};
  uint64_t expected_miss_{0};
  uint64_t submit_fails_{0};
  uint64_t put_size_bytes{0};
  uint64_t get_size_bytes{0};
  LatencyStats latency_{};
};

struct ValueSpec {
  bool exists{false};
  uint32_t size{0};
  uint64_t seed{0};
};

enum class OpType {
  Put,
  Get,
  Exists,
  Delete,
};

struct WorkerRuntime {
  explicit WorkerRuntime(uint32_t tid, size_t keyspace, size_t key_len_limit)
      : slots(keyspace), keys(simm::tools::stable_test::BuildWorkerKeyspace(tid, keyspace, key_len_limit)) {}

  mutable std::mutex mutex;
  std::condition_variable cv;
  ThreadStats stats;
  std::vector<ValueSpec> slots;
  std::vector<std::string> keys;
  size_t live_keys{0};
  size_t inflight{0};
};

struct PendingSingleOp {
  OpType op{OpType::Put};
  bool overwrite{false};
  size_t slot{0};
  std::string key;
  ValueSpec previous_spec;
  ValueSpec target_spec;
  std::shared_ptr<simm::clnt::Data> data_holder;
  steady_clock_t::time_point start_ts;
};

uint32_t choose_size(uint32_t limit) {
  return FLAGS_fixed_kvsize ? limit : folly::Random::rand32(1, limit + 1);
}

void FillBufferFromSeed(std::span<char> buf, uint64_t seed) {
  uint64_t state = seed ^ 0x9E3779B97F4A7C15ULL;
  for (size_t pos = 0; pos < buf.size(); ++pos) {
    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    buf[pos] = static_cast<char>(state & 0xFFU);
  }
}

bool VerifyBufferFromSeed(std::span<const char> buf, uint64_t seed) {
  uint64_t state = seed ^ 0x9E3779B97F4A7C15ULL;
  for (size_t pos = 0; pos < buf.size(); ++pos) {
    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    if (buf[pos] != static_cast<char>(state & 0xFFU)) {
      return false;
    }
  }
  return true;
}

void RecordLatency(ThreadStats &stats, steady_clock_t::time_point start_ts) {
  stats.latency_.add(std::chrono::duration_cast<micro_ts>(steady_clock_t::now() - start_ts));
}

void print_stats(const ThreadStats &s, uint32_t threads, uint64_t elapsed_secs, bool is_delta) {
  const double qps = elapsed_secs == 0 ? 0.0 : static_cast<double>(s.total_ops()) / static_cast<double>(elapsed_secs);
  const double throughput_mb = elapsed_secs == 0 ? 0.0
                                                 : static_cast<double>(s.put_size_bytes + s.get_size_bytes) /
                                                       static_cast<double>(ONE_MB) / static_cast<double>(elapsed_secs);

  std::cout << "++++++++++++++++++++++++++++ " << (is_delta ? "STABLE TEST DELTA" : "STABLE TEST REPORT")
            << " +++++++++++++++++++++++++\n"
            << "Threads         : " << threads << "\n"
            << "ElapsedSecs     : " << elapsed_secs << "\n"
            << "TotalOps        : " << s.total_ops() << "\n"
            << "Failures        : " << s.total_failures() << "\n"
            << "SubmitFails     : " << s.submit_fails_ << "\n"
            << "PutCnt          : " << s.put_ << "\n"
            << "PutFails        : " << s.put_fails_ << "\n"
            << "PutSuccs        : " << s.put_succs_ << "\n"
            << "OverwriteCnt    : " << s.overwrite_put_ << "\n"
            << "OverwriteFails  : " << s.overwrite_put_fails_ << "\n"
            << "OverwriteSuccs  : " << s.overwrite_put_succs_ << "\n"
            << "GetCnt          : " << s.get_ << "\n"
            << "GetFails        : " << s.get_fails_ << "\n"
            << "GetSuccs        : " << s.get_succs_ << "\n"
            << "ExistsCnt       : " << s.exists_ << "\n"
            << "ExistsFails     : " << s.exists_fails_ << "\n"
            << "ExistsSuccs     : " << s.exists_succs_ << "\n"
            << "DeleteCnt       : " << s.del_ << "\n"
            << "DeleteFails     : " << s.del_fails_ << "\n"
            << "DeleteSuccs     : " << s.del_succs_ << "\n"
            << "MPutCnt         : " << s.mput_ << "\n"
            << "MPutFails       : " << s.mput_fails_ << "\n"
            << "MPutSuccs       : " << s.mput_succs_ << "\n"
            << "MGetCnt         : " << s.mget_ << "\n"
            << "MGetFails       : " << s.mget_fails_ << "\n"
            << "MGetSuccs       : " << s.mget_succs_ << "\n"
            << "DataMatch       : " << s.data_match_ << "\n"
            << "DataMismatch    : " << s.data_mismatch_ << "\n"
            << "ExpectedMiss    : " << s.expected_miss_ << "\n"
            << "PutSize         : " << convert_to_readable_size(s.put_size_bytes) << "\n"
            << "GetSize         : " << convert_to_readable_size(s.get_size_bytes) << "\n"
            << "QPS             : " << std::fixed << std::setprecision(2) << qps << "\n"
            << "ThroughputMiB/s : " << throughput_mb << "\n"
            << "AvgLatencyUs    : " << s.latency_.avg_us() << "\n"
            << "P50LatencyUs    : " << s.latency_.percentile(0.50) << "\n"
            << "P95LatencyUs    : " << s.latency_.percentile(0.95) << "\n"
            << "P99LatencyUs    : " << s.latency_.percentile(0.99) << "\n"
            << "MaxLatencyUs    : " << s.latency_.max_us_ << "\n"
            << std::endl;
}

void usage() {
  std::cout << "simm_stable_test --iomode=[sync|async] --threads=32 --time=3600 --keyspace_per_thread=10000 "
               "--getratio=45 --putratio=35 --existsratio=10 --delratio=10 --iodepth=128"
            << std::endl;
}

void PrintArgError(const std::string &msg) {
  std::cerr << "[simm_stable_test] invalid args: " << msg << std::endl;
}

void check_args() {
  bool args_valid = true;

  if (FLAGS_cm_primary_node_ip.empty()) {
    MLOG_ERROR("cm_primary_node_ip is empty");
    PrintArgError("cm_primary_node_ip must be set");
    args_valid = false;
  } else if (FLAGS_keylimit < 1 || FLAGS_keylimit > KEY_LEN_LIMIT) {
    MLOG_ERROR("Invalid key size upper limit : {}B, should in [1B, {}B]", FLAGS_keylimit, KEY_LEN_LIMIT);
    PrintArgError("keylimit out of range");
    args_valid = false;
  } else if (FLAGS_vallimit < 1 || FLAGS_vallimit > VAL_LEN_LIMIT) {
    MLOG_ERROR("Invalid val size upper limit : {}B, should in [1B, {}B]", FLAGS_vallimit, VAL_LEN_LIMIT);
    PrintArgError("vallimit out of range");
    args_valid = false;
  } else if (FLAGS_threads < 1 || FLAGS_threads > THREAD_NUM_LIMIT) {
    MLOG_ERROR("Invalid threads number : {}, should in [1, {}]", FLAGS_threads, THREAD_NUM_LIMIT);
    PrintArgError("threads out of range");
    args_valid = false;
  } else if (FLAGS_iodepth < 1 || FLAGS_iodepth > IODEPTH_LIMIT) {
    MLOG_ERROR("Invalid iodepth : {}, should in [1, {}]", FLAGS_iodepth, IODEPTH_LIMIT);
    PrintArgError("iodepth out of range");
    args_valid = false;
  } else if (FLAGS_iomode != "sync" && FLAGS_iomode != "async") {
    MLOG_ERROR("Invalid iomode type : {}, should be sync or async", FLAGS_iomode);
    PrintArgError("iomode must be sync or async");
    args_valid = false;
  } else if (FLAGS_keyspace_per_thread < 1 || FLAGS_keyspace_per_thread > KEYSPACE_LIMIT) {
    MLOG_ERROR("Invalid keyspace_per_thread : {}, should in [1, {}]", FLAGS_keyspace_per_thread, KEYSPACE_LIMIT);
    PrintArgError("keyspace_per_thread out of range");
    args_valid = false;
  } else if (FLAGS_batch_mode && FLAGS_iomode != "sync") {
    MLOG_ERROR("batch_mode currently only supports sync iomode");
    PrintArgError("batch_mode currently only supports sync iomode");
    args_valid = false;
  } else if (FLAGS_batch_mode && FLAGS_batch_size < 1) {
    MLOG_ERROR("batch_size should be >= 1");
    PrintArgError("batch_size should be >= 1");
    args_valid = false;
  } else if (FLAGS_time < 1) {
    MLOG_ERROR("time should be >= 1");
    PrintArgError("time should be >= 1");
    args_valid = false;
  } else if (FLAGS_report_interval_inSecs < 1) {
    MLOG_ERROR("report_interval_inSecs should be >= 1");
    PrintArgError("report_interval_inSecs should be >= 1");
    args_valid = false;
  }

  if (FLAGS_getratio + FLAGS_putratio + FLAGS_existsratio + FLAGS_delratio != 100) {
    MLOG_ERROR("getratio({}) + putratio({}) + existsratio({}) + delratio({}) must equal 100",
               FLAGS_getratio,
               FLAGS_putratio,
               FLAGS_existsratio,
               FLAGS_delratio);
    PrintArgError("getratio + putratio + existsratio + delratio must equal 100; oputratio is part of putratio");
    args_valid = false;
  }

  const auto min_key_len = simm::tools::stable_test::MinimumUniqueKeyLength(
      FLAGS_threads == 0 ? 0 : FLAGS_threads - 1, FLAGS_keyspace_per_thread == 0 ? 0 : FLAGS_keyspace_per_thread - 1);
  if (FLAGS_keylimit < min_key_len) {
    MLOG_ERROR("Invalid key size upper limit : {}B, should be >= {}B to keep per-slot keys unique",
               FLAGS_keylimit,
               min_key_len);
    PrintArgError("keylimit is too small to keep per-slot keys unique");
    args_valid = false;
  }

  if (!args_valid) {
    usage();
    std::exit(EINVAL);
  }
}

bool RunStartupSelfCheck(simm::clnt::KVStore &kvstore) {
  const std::string key =
      "stable_test_canary_" + std::to_string(static_cast<uint64_t>(steady_clock_t::now().time_since_epoch().count()));
  constexpr uint32_t kCanarySize = 128;
  constexpr uint64_t kCanarySeed = 0x13572468ABCDEF01ULL;

  try {
    auto put_data = kvstore.Allocate(kCanarySize);
    FillBufferFromSeed(put_data.AsRef(), kCanarySeed);
    simm::clnt::DataView put_view(put_data);
    const auto put_rc = kvstore.Put(key, put_view);
    if (put_rc != CommonErr::OK) {
      std::cerr << "[simm_stable_test] startup self-check failed: put rc=" << put_rc << std::endl;
      return false;
    }

    const auto exists_rc = kvstore.Exists(key);
    if (exists_rc != CommonErr::OK) {
      std::cerr << "[simm_stable_test] startup self-check failed: exists rc=" << exists_rc << std::endl;
      return false;
    }

    auto get_data = kvstore.Allocate(kCanarySize);
    simm::clnt::DataView get_view(get_data);
    const auto get_rc = kvstore.Get(key, get_view);
    if (get_rc != static_cast<int32_t>(kCanarySize) ||
        !VerifyBufferFromSeed(std::span<const char>(get_data.AsRef().data(), kCanarySize), kCanarySeed)) {
      std::cerr << "[simm_stable_test] startup self-check failed: get rc=" << get_rc << std::endl;
      return false;
    }

    const auto del_rc = kvstore.Delete(key);
    if (del_rc != CommonErr::OK) {
      std::cerr << "[simm_stable_test] startup self-check failed: delete rc=" << del_rc << std::endl;
      return false;
    }
    return true;
  } catch (const std::exception &ex) {
    std::cerr << "[simm_stable_test] startup self-check threw exception: " << ex.what() << std::endl;
    return false;
  }
}

OpType ChooseOperation(const WorkerRuntime &runtime) {
  const uint32_t dice = folly::Random::rand32(0, 100);
  const bool no_live_keys = runtime.live_keys == 0;
  if (no_live_keys) {
    return OpType::Put;
  }
  if (dice < FLAGS_getratio) {
    return OpType::Get;
  }
  if (dice < FLAGS_getratio + FLAGS_putratio) {
    return OpType::Put;
  }
  if (dice < FLAGS_getratio + FLAGS_putratio + FLAGS_existsratio) {
    return OpType::Exists;
  }
  return OpType::Delete;
}

size_t PickExistingSlot(const WorkerRuntime &runtime) {
  size_t start = folly::Random::rand32(0, static_cast<uint32_t>(runtime.slots.size()));
  for (size_t i = 0; i < runtime.slots.size(); ++i) {
    size_t idx = (start + i) % runtime.slots.size();
    if (runtime.slots[idx].exists) {
      return idx;
    }
  }
  return runtime.slots.size();
}

size_t PickPutSlot(const WorkerRuntime &runtime, bool *overwrite) {
  if (runtime.live_keys > 0 && folly::Random::rand32(0, 100) < FLAGS_oputratio) {
    size_t idx = PickExistingSlot(runtime);
    if (idx != runtime.slots.size()) {
      *overwrite = true;
      return idx;
    }
  }

  size_t start = folly::Random::rand32(0, static_cast<uint32_t>(runtime.slots.size()));
  for (size_t i = 0; i < runtime.slots.size(); ++i) {
    size_t idx = (start + i) % runtime.slots.size();
    if (!runtime.slots[idx].exists) {
      *overwrite = false;
      return idx;
    }
  }

  *overwrite = true;
  return start;
}

ThreadStats SnapshotStats(const std::vector<std::shared_ptr<WorkerRuntime>> &workers) {
  ThreadStats aggregated;
  for (const auto &worker : workers) {
    std::lock_guard lock(worker->mutex);
    aggregated.merge(worker->stats);
  }
  return aggregated;
}

void RecordExpectedExistsResult(ThreadStats &stats, bool expected_exists, int rc) {
  ++stats.exists_;
  if (expected_exists) {
    if (rc == CommonErr::OK) {
      ++stats.exists_succs_;
    } else {
      ++stats.exists_fails_;
    }
    return;
  }

  if (FLAGS_strict_verify_exists) {
    if (rc != CommonErr::OK) {
      ++stats.exists_succs_;
      ++stats.expected_miss_;
    } else {
      ++stats.exists_fails_;
    }
  } else {
    ++stats.exists_succs_;
    if (rc != CommonErr::OK) {
      ++stats.expected_miss_;
    }
  }
}

void RunSyncSingleOp(uint32_t tid, simm::clnt::KVStore &kvstore, WorkerRuntime &runtime) {
  PendingSingleOp op;
  {
    std::lock_guard lock(runtime.mutex);
    op.op = ChooseOperation(runtime);
    if (op.op == OpType::Put) {
      op.slot = PickPutSlot(runtime, &op.overwrite);
      op.previous_spec = runtime.slots[op.slot];
      op.target_spec.exists = true;
      op.target_spec.size = choose_size(FLAGS_vallimit);
      op.target_spec.seed = folly::Random::rand64();
    } else if (op.op == OpType::Get || op.op == OpType::Delete) {
      op.slot = PickExistingSlot(runtime);
      if (op.slot == runtime.slots.size()) {
        op.op = OpType::Put;
        op.slot = PickPutSlot(runtime, &op.overwrite);
        op.previous_spec = runtime.slots[op.slot];
        op.target_spec.exists = true;
        op.target_spec.size = choose_size(FLAGS_vallimit);
        op.target_spec.seed = folly::Random::rand64();
      } else {
        op.previous_spec = runtime.slots[op.slot];
      }
    } else {
      op.slot = folly::Random::rand32(0, static_cast<uint32_t>(runtime.slots.size()));
      op.previous_spec = runtime.slots[op.slot];
    }
    op.key = runtime.keys[op.slot];
  }

  op.start_ts = steady_clock_t::now();
  if (op.op == OpType::Put) {
    auto data = kvstore.Allocate(op.target_spec.size);
    FillBufferFromSeed(data.AsRef(), op.target_spec.seed);
    simm::clnt::DataView dv(data);
    const int16_t rc = kvstore.Put(op.key, dv);
    std::lock_guard lock(runtime.mutex);
    if (op.overwrite) {
      ++runtime.stats.overwrite_put_;
    } else {
      ++runtime.stats.put_;
    }
    RecordLatency(runtime.stats, op.start_ts);
    if (rc == CommonErr::OK) {
      if (op.overwrite) {
        ++runtime.stats.overwrite_put_succs_;
      } else {
        ++runtime.stats.put_succs_;
        if (!op.previous_spec.exists) {
          ++runtime.live_keys;
        }
      }
      runtime.stats.put_size_bytes += op.target_spec.size;
      runtime.slots[op.slot] = op.target_spec;
    } else {
      if (op.overwrite) {
        ++runtime.stats.overwrite_put_fails_;
      } else {
        ++runtime.stats.put_fails_;
      }
    }
    return;
  }

  if (op.op == OpType::Get) {
    auto data = kvstore.Allocate(op.previous_spec.size);
    simm::clnt::DataView dv(data);
    const int32_t rc = kvstore.Get(op.key, dv);
    std::lock_guard lock(runtime.mutex);
    ++runtime.stats.get_;
    RecordLatency(runtime.stats, op.start_ts);
    if (rc == static_cast<int32_t>(op.previous_spec.size) &&
        VerifyBufferFromSeed(std::span<const char>(data.AsRef().data(), op.previous_spec.size),
                             op.previous_spec.seed)) {
      ++runtime.stats.get_succs_;
      ++runtime.stats.data_match_;
      runtime.stats.get_size_bytes += op.previous_spec.size;
    } else {
      ++runtime.stats.get_fails_;
      ++runtime.stats.data_mismatch_;
    }
    return;
  }

  if (op.op == OpType::Exists) {
    const int16_t rc = kvstore.Exists(op.key);
    std::lock_guard lock(runtime.mutex);
    RecordLatency(runtime.stats, op.start_ts);
    RecordExpectedExistsResult(runtime.stats, op.previous_spec.exists, rc);
    return;
  }

  const int16_t rc = kvstore.Delete(op.key);
  std::lock_guard lock(runtime.mutex);
  ++runtime.stats.del_;
  RecordLatency(runtime.stats, op.start_ts);
  if (rc == CommonErr::OK) {
    ++runtime.stats.del_succs_;
    if (runtime.slots[op.slot].exists) {
      runtime.slots[op.slot].exists = false;
      runtime.slots[op.slot].size = 0;
      runtime.slots[op.slot].seed = 0;
      if (runtime.live_keys > 0) {
        --runtime.live_keys;
      }
    }
  } else {
    ++runtime.stats.del_fails_;
  }
}

void RunSyncBatchOp(uint32_t tid, simm::clnt::KVStore &kvstore, WorkerRuntime &runtime) {
  const size_t batch_sz = FLAGS_batch_size;
  std::vector<size_t> slots;
  std::vector<std::string> keys;
  std::vector<ValueSpec> specs;
  std::vector<simm::clnt::Data> payloads;
  std::vector<simm::clnt::DataView> put_views;
  std::vector<simm::clnt::Data> get_payloads;
  std::vector<simm::clnt::DataView> get_views;

  slots.reserve(batch_sz);
  keys.reserve(batch_sz);
  specs.reserve(batch_sz);
  payloads.reserve(batch_sz);
  put_views.reserve(batch_sz);
  get_payloads.reserve(batch_sz);
  get_views.reserve(batch_sz);

  {
    std::lock_guard lock(runtime.mutex);
    for (size_t i = 0; i < batch_sz; ++i) {
      bool overwrite = false;
      const auto slot = PickPutSlot(runtime, &overwrite);
      ValueSpec spec;
      spec.exists = true;
      spec.size = choose_size(FLAGS_vallimit);
      spec.seed = folly::Random::rand64();
      slots.push_back(slot);
      keys.push_back(runtime.keys[slot]);
      specs.push_back(spec);
    }
  }

  for (const auto &spec : specs) {
    auto data = kvstore.Allocate(spec.size);
    FillBufferFromSeed(data.AsRef(), spec.seed);
    payloads.emplace_back(std::move(data));
  }
  for (auto &data : payloads) {
    put_views.emplace_back(data);
  }

  auto start_ts = steady_clock_t::now();
  auto put_rets = kvstore.MPut(keys, put_views);
  {
    std::lock_guard lock(runtime.mutex);
    ++runtime.stats.mput_;
    runtime.stats.put_ += batch_sz;
    RecordLatency(runtime.stats, start_ts);
    bool all_ok = true;
    for (size_t i = 0; i < put_rets.size(); ++i) {
      if (put_rets[i] == CommonErr::OK) {
        ++runtime.stats.put_succs_;
        runtime.stats.put_size_bytes += specs[i].size;
        if (!runtime.slots[slots[i]].exists) {
          ++runtime.live_keys;
        }
        runtime.slots[slots[i]] = specs[i];
      } else {
        ++runtime.stats.put_fails_;
        all_ok = false;
      }
    }
    if (all_ok) {
      ++runtime.stats.mput_succs_;
    } else {
      ++runtime.stats.mput_fails_;
    }
  }

  for (const auto &spec : specs) {
    auto data = kvstore.Allocate(spec.size);
    get_payloads.emplace_back(std::move(data));
  }
  for (auto &data : get_payloads) {
    get_views.emplace_back(data);
  }

  start_ts = steady_clock_t::now();
  auto get_rets = kvstore.MGet(keys, get_views);
  {
    std::lock_guard lock(runtime.mutex);
    ++runtime.stats.mget_;
    runtime.stats.get_ += batch_sz;
    RecordLatency(runtime.stats, start_ts);
    bool all_ok = true;
    for (size_t i = 0; i < get_rets.size(); ++i) {
      if (get_rets[i] == static_cast<int32_t>(specs[i].size) &&
          VerifyBufferFromSeed(std::span<const char>(get_payloads[i].AsRef().data(), specs[i].size), specs[i].seed)) {
        ++runtime.stats.get_succs_;
        ++runtime.stats.data_match_;
        runtime.stats.get_size_bytes += specs[i].size;
      } else {
        ++runtime.stats.get_fails_;
        ++runtime.stats.data_mismatch_;
        all_ok = false;
      }
    }
    if (all_ok) {
      ++runtime.stats.mget_succs_;
    } else {
      ++runtime.stats.mget_fails_;
    }
  }
}

void SubmitAsyncSingleOp(uint32_t tid,
                         simm::clnt::KVStore &kvstore,
                         std::shared_ptr<WorkerRuntime> runtime,
                         std::atomic<bool> &stop_threads) {
  PendingSingleOp op;
  {
    std::unique_lock lock(runtime->mutex);
    runtime->cv.wait(
        lock, [&]() { return stop_threads.load(std::memory_order_acquire) || runtime->inflight < FLAGS_iodepth; });
    if (stop_threads.load(std::memory_order_acquire)) {
      return;
    }
    ++runtime->inflight;
    op.op = ChooseOperation(*runtime);
    if (op.op == OpType::Put) {
      op.slot = PickPutSlot(*runtime, &op.overwrite);
      op.previous_spec = runtime->slots[op.slot];
      op.target_spec.exists = true;
      op.target_spec.size = choose_size(FLAGS_vallimit);
      op.target_spec.seed = folly::Random::rand64();
    } else if (op.op == OpType::Get || op.op == OpType::Delete) {
      op.slot = PickExistingSlot(*runtime);
      if (op.slot == runtime->slots.size()) {
        op.op = OpType::Put;
        op.slot = PickPutSlot(*runtime, &op.overwrite);
        op.previous_spec = runtime->slots[op.slot];
        op.target_spec.exists = true;
        op.target_spec.size = choose_size(FLAGS_vallimit);
        op.target_spec.seed = folly::Random::rand64();
      } else {
        op.previous_spec = runtime->slots[op.slot];
      }
    } else {
      op.slot = folly::Random::rand32(0, static_cast<uint32_t>(runtime->slots.size()));
      op.previous_spec = runtime->slots[op.slot];
    }
    op.key = runtime->keys[op.slot];
  }

  op.start_ts = steady_clock_t::now();

  if (op.op == OpType::Put) {
    auto data_holder = std::make_shared<simm::clnt::Data>(kvstore.Allocate(op.target_spec.size));
    FillBufferFromSeed(data_holder->AsRef(), op.target_spec.seed);
    simm::clnt::DataView put_view(*data_holder);
    auto rc = kvstore.AsyncPut(op.key, put_view, [runtime, op, data_holder](int result) {
      std::lock_guard lock(runtime->mutex);
      if (op.overwrite) {
        ++runtime->stats.overwrite_put_;
      } else {
        ++runtime->stats.put_;
      }
      RecordLatency(runtime->stats, op.start_ts);
      if (result == CommonErr::OK) {
        if (op.overwrite) {
          ++runtime->stats.overwrite_put_succs_;
        } else {
          ++runtime->stats.put_succs_;
          if (!op.previous_spec.exists) {
            ++runtime->live_keys;
          }
        }
        runtime->stats.put_size_bytes += op.target_spec.size;
        runtime->slots[op.slot] = op.target_spec;
      } else {
        if (op.overwrite) {
          ++runtime->stats.overwrite_put_fails_;
        } else {
          ++runtime->stats.put_fails_;
        }
      }
      if (runtime->inflight > 0) {
        --runtime->inflight;
      }
      runtime->cv.notify_all();
    });
    if (rc != CommonErr::OK) {
      std::lock_guard lock(runtime->mutex);
      ++runtime->stats.submit_fails_;
      if (runtime->inflight > 0) {
        --runtime->inflight;
      }
      runtime->cv.notify_all();
    }
    return;
  }

  if (op.op == OpType::Get) {
    auto data_holder = std::make_shared<simm::clnt::Data>(kvstore.Allocate(op.previous_spec.size));
    simm::clnt::DataView get_view(*data_holder);
    auto rc = kvstore.AsyncGet(op.key, get_view, [runtime, op, data_holder](int result) {
      std::lock_guard lock(runtime->mutex);
      ++runtime->stats.get_;
      RecordLatency(runtime->stats, op.start_ts);
      if (result == static_cast<int>(op.previous_spec.size) &&
          VerifyBufferFromSeed(std::span<const char>(data_holder->AsRef().data(), op.previous_spec.size),
                               op.previous_spec.seed)) {
        ++runtime->stats.get_succs_;
        ++runtime->stats.data_match_;
        runtime->stats.get_size_bytes += op.previous_spec.size;
      } else {
        ++runtime->stats.get_fails_;
        ++runtime->stats.data_mismatch_;
      }
      if (runtime->inflight > 0) {
        --runtime->inflight;
      }
      runtime->cv.notify_all();
    });
    if (rc != CommonErr::OK) {
      std::lock_guard lock(runtime->mutex);
      ++runtime->stats.submit_fails_;
      if (runtime->inflight > 0) {
        --runtime->inflight;
      }
      runtime->cv.notify_all();
    }
    return;
  }

  if (op.op == OpType::Exists) {
    auto rc = kvstore.AsyncExists(op.key, [runtime, op](int result) {
      std::lock_guard lock(runtime->mutex);
      RecordLatency(runtime->stats, op.start_ts);
      RecordExpectedExistsResult(runtime->stats, op.previous_spec.exists, result);
      if (runtime->inflight > 0) {
        --runtime->inflight;
      }
      runtime->cv.notify_all();
    });
    if (rc != CommonErr::OK) {
      std::lock_guard lock(runtime->mutex);
      ++runtime->stats.submit_fails_;
      if (runtime->inflight > 0) {
        --runtime->inflight;
      }
      runtime->cv.notify_all();
    }
    return;
  }

  auto rc = kvstore.AsyncDelete(op.key, [runtime, op](int result) {
    std::lock_guard lock(runtime->mutex);
    ++runtime->stats.del_;
    RecordLatency(runtime->stats, op.start_ts);
    if (result == CommonErr::OK) {
      ++runtime->stats.del_succs_;
      if (runtime->slots[op.slot].exists) {
        runtime->slots[op.slot].exists = false;
        runtime->slots[op.slot].size = 0;
        runtime->slots[op.slot].seed = 0;
        if (runtime->live_keys > 0) {
          --runtime->live_keys;
        }
      }
    } else {
      ++runtime->stats.del_fails_;
    }
    if (runtime->inflight > 0) {
      --runtime->inflight;
    }
    runtime->cv.notify_all();
  });
  if (rc != CommonErr::OK) {
    std::lock_guard lock(runtime->mutex);
    ++runtime->stats.submit_fails_;
    if (runtime->inflight > 0) {
      --runtime->inflight;
    }
    runtime->cv.notify_all();
  }
}

}  // namespace

int main(int argc, char **argv) {
  gflags::SetUsageMessage(
      "simm_stable_test: long-running stability and workload tool for SiMM client/service validation");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  folly::Init init(&argc, &argv);

#ifdef NDEBUG
  simm::logging::LogConfig clnt_log_config = simm::logging::LogConfig{"/tmp/simm_clnt.log", "INFO"};
#else
  simm::logging::LogConfig clnt_log_config = simm::logging::LogConfig{"/tmp/simm_clnt.log", "DEBUG"};
#endif
  simm::logging::LoggerManager::Instance().UpdateConfig("simm_client", clnt_log_config);

  check_args();
  std::signal(SIGINT, StopSignalHandler);
  std::signal(SIGTERM, StopSignalHandler);

  const char *fixed_kv_mode = FLAGS_fixed_kvsize ? "T" : "F";
  const char *batch_mode = FLAGS_batch_mode ? "T" : "F";

  MLOG_INFO("++++++++++++++++++++++++++++++++++++++++ Start SIMM_STABLE_TEST ++++++++++++++++++++++++++++++++++++++++");
  std::cout << "Test Args: key_limit=" << FLAGS_keylimit << "B"
            << " val_limit=" << FLAGS_vallimit << "B"
            << " threads=" << FLAGS_threads << " iomode=" << FLAGS_iomode << " fixed_kv=" << fixed_kv_mode
            << " batch_mode=" << batch_mode << " batch_size=" << FLAGS_batch_size << " iodepth=" << FLAGS_iodepth
            << " getratio=" << FLAGS_getratio << " putratio=" << FLAGS_putratio << " existsratio=" << FLAGS_existsratio
            << " delratio=" << FLAGS_delratio << " keyspace_per_thread=" << FLAGS_keyspace_per_thread
            << " report_interval=" << FLAGS_report_interval_inSecs
            << " strict_verify_exists=" << (FLAGS_strict_verify_exists ? "T" : "F") << std::endl;

  std::shared_ptr<simm::clnt::KVStore> simm_kvstore;
  try {
    simm_kvstore = std::make_shared<simm::clnt::KVStore>();
  } catch (const std::exception &ex) {
    std::cerr << "[simm_stable_test] failed to create KVStore: " << ex.what() << std::endl;
    return EHOSTUNREACH;
  }

  if (!RunStartupSelfCheck(*simm_kvstore)) {
    return EHOSTUNREACH;
  }

  std::atomic<bool> stop_threads{false};
  std::vector<std::shared_ptr<WorkerRuntime>> workers;
  workers.reserve(FLAGS_threads);
  for (uint32_t i = 0; i < FLAGS_threads; ++i) {
    workers.push_back(std::make_shared<WorkerRuntime>(i, FLAGS_keyspace_per_thread, FLAGS_keylimit));
  }

  auto test_start_ts = steady_clock_t::now();
  auto test_end_ts = test_start_ts + std::chrono::seconds(FLAGS_time);
  ThreadStats last_snapshot;
  auto reporter = std::thread([&]() {
    while (!stop_threads.load(std::memory_order_acquire)) {
      std::this_thread::sleep_for(std::chrono::seconds(FLAGS_report_interval_inSecs));
      if (stop_threads.load(std::memory_order_acquire)) {
        break;
      }
      auto snapshot = SnapshotStats(workers);
      ThreadStats delta = snapshot;
      delta.put_ -= last_snapshot.put_;
      delta.put_fails_ -= last_snapshot.put_fails_;
      delta.put_succs_ -= last_snapshot.put_succs_;
      delta.overwrite_put_ -= last_snapshot.overwrite_put_;
      delta.overwrite_put_fails_ -= last_snapshot.overwrite_put_fails_;
      delta.overwrite_put_succs_ -= last_snapshot.overwrite_put_succs_;
      delta.get_ -= last_snapshot.get_;
      delta.get_fails_ -= last_snapshot.get_fails_;
      delta.get_succs_ -= last_snapshot.get_succs_;
      delta.exists_ -= last_snapshot.exists_;
      delta.exists_fails_ -= last_snapshot.exists_fails_;
      delta.exists_succs_ -= last_snapshot.exists_succs_;
      delta.del_ -= last_snapshot.del_;
      delta.del_fails_ -= last_snapshot.del_fails_;
      delta.del_succs_ -= last_snapshot.del_succs_;
      delta.mput_ -= last_snapshot.mput_;
      delta.mput_fails_ -= last_snapshot.mput_fails_;
      delta.mput_succs_ -= last_snapshot.mput_succs_;
      delta.mget_ -= last_snapshot.mget_;
      delta.mget_fails_ -= last_snapshot.mget_fails_;
      delta.mget_succs_ -= last_snapshot.mget_succs_;
      delta.data_match_ -= last_snapshot.data_match_;
      delta.data_mismatch_ -= last_snapshot.data_mismatch_;
      delta.expected_miss_ -= last_snapshot.expected_miss_;
      delta.submit_fails_ -= last_snapshot.submit_fails_;
      delta.put_size_bytes -= last_snapshot.put_size_bytes;
      delta.get_size_bytes -= last_snapshot.get_size_bytes;
      last_snapshot = snapshot;
      print_stats(delta, FLAGS_threads, FLAGS_report_interval_inSecs, true);
    }
  });

  std::vector<std::thread> worker_threads;
  worker_threads.reserve(FLAGS_threads);
  for (uint32_t tid = 0; tid < FLAGS_threads; ++tid) {
    auto runtime = workers[tid];
    worker_threads.emplace_back([&, tid, runtime]() {
      if (FLAGS_iomode == "sync") {
        while (!stop_threads.load(std::memory_order_acquire)) {
          if (FLAGS_batch_mode) {
            RunSyncBatchOp(tid, *simm_kvstore, *runtime);
          } else {
            RunSyncSingleOp(tid, *simm_kvstore, *runtime);
          }
        }
      } else {
        while (!stop_threads.load(std::memory_order_acquire)) {
          SubmitAsyncSingleOp(tid, *simm_kvstore, runtime, stop_threads);
        }
        std::unique_lock lock(runtime->mutex);
        runtime->cv.wait(lock, [&]() { return runtime->inflight == 0; });
      }
      MLOG_INFO("Stable test worker {} exits", tid);
    });
  }

  while (!g_stop_requested.load(std::memory_order_acquire) && steady_clock_t::now() < test_end_ts) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  stop_threads.store(true, std::memory_order_release);
  for (const auto &worker : workers) {
    std::lock_guard lock(worker->mutex);
    worker->cv.notify_all();
  }

  for (auto &worker : worker_threads) {
    if (worker.joinable()) {
      worker.join();
    }
  }
  if (reporter.joinable()) {
    reporter.join();
  }

  auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(steady_clock_t::now() - test_start_ts).count();
  auto final_stats = SnapshotStats(workers);
  print_stats(final_stats, FLAGS_threads, static_cast<uint64_t>(elapsed), false);
  if (final_stats.total_ops() == 0) {
    std::cerr << "[simm_stable_test] no operations completed" << std::endl;
    return EIO;
  }
  if (final_stats.total_failures() > 0) {
    std::cerr << "[simm_stable_test] detected failures during run: " << final_stats.total_failures() << std::endl;
    return EIO;
  }
  return 0;
}
