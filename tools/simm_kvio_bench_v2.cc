/*
 * Copyright (c) 2026 The Scitix Authors.
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
#include <condition_variable>
#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gflags/gflags.h>

#include <folly/Likely.h>
#include <folly/Random.h>
#include <folly/String.h>
#include <folly/init/Init.h>

#include "common/errcode/errcode_def.h"
#include "common/logging/logging.h"
#include "simm/simm_common.h"
#include "simm/simm_kv.h"
#include "transport/types.h"

using steady_clock_t = std::chrono::steady_clock;
using micro_ts = std::chrono::microseconds;
using nano_ts = std::chrono::nanoseconds;

DEFINE_uint32(key_size, 20, "Fixed key size in bytes");
DEFINE_uint32(val_size, 8909, "Fixed value size in bytes");
DEFINE_uint32(threads, 32, "Concurrent benchmark worker threads");
DEFINE_uint32(run_time, 600, "Measured benchmark time in seconds");
DEFINE_uint32(warmup_time, 30, "Warmup time in seconds before measured run");
DEFINE_uint32(key_num, 100000, "Distinct keys per worker thread");
DEFINE_uint32(key_num_prepare_get, 1000, "Keys per thread to prefill for read-like modes");
DEFINE_uint32(report_interval, 5, "Periodic report interval in seconds during measured phase");
DEFINE_uint32(seed, 1, "Deterministic benchmark random seed");
DEFINE_uint32(batchsize, 128, "Batch size for mget/mput/mexists or async batch wave size");
DEFINE_uint32(iodepth, 128, "Per-worker max in-flight async requests");
DEFINE_uint32(async_global_iodepth,
              512,
              "Global max in-flight async requests across all workers, used to avoid overwhelming a single DS/QP");
DEFINE_uint32(async_backpressure_retry,
              64,
              "Retry count for async submit when SiCL returns tx queue full (-13)");
DEFINE_uint32(async_backpressure_backoff_us,
              50,
              "Base backoff in microseconds for async tx queue full retries");
DEFINE_bool(prefill_syncreq_enable_retry,
            true,
            "Enable sync retry during prefill phase only; measured phase still follows client defaults");
DEFINE_uint32(prefill_syncreq_retry_count,
              1,
              "Sync retry count used during prefill phase only");
DEFINE_int32(prefill_sync_req_timeout_ms,
             5000,
             "Sync request timeout in milliseconds used during prefill phase only");
DEFINE_string(mode, "get", "Benchmark modes: get, put, exists, del, mget, mput, mexists");
DEFINE_string(io_mode, "sync", "IO mode: sync, async");
DEFINE_bool(summary, true, "Print final detailed summary");
DEFINE_bool(verify_get, true, "Verify returned value bytes for get/mget when workload semantics allow");
DEFINE_bool(random_access, true, "Use random key access instead of sequential key order");
DEFINE_bool(allow_overwrite,
            false,
            "Allow put/mput workloads to reuse existing keys; when false, write workloads consume unique keys");

DECLARE_string(cm_primary_node_ip);
DECLARE_string(clnt_log_file);
DECLARE_bool(clnt_syncreq_enable_retry);
DECLARE_uint32(clnt_syncreq_retry_count);
DECLARE_int32(clnt_sync_req_timeout_ms);
DECLARE_LOG_MODULE("simm_client");

namespace {

static constexpr uint64_t ONE_MB = 1024ULL * 1024ULL;
static constexpr uint64_t ONE_GB = 1024ULL * 1024ULL * 1024ULL;
static constexpr const char *kColorRed = "\033[31m";
static constexpr const char *kColorGreen = "\033[32m";
static constexpr const char *kColorYellow = "\033[33m";
static constexpr const char *kColorBlue = "\033[34m";
static constexpr const char *kColorReset = "\033[0m";
static constexpr uint32_t kMaxAsyncBackoffUs = 5000;

static const char kCharset[] =
    "abcdefghijklmnopqrstuvwxyz"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "0123456789"
    "~.!@#$%^&*([{}])_-+=/><,?:;'";
static constexpr size_t kCharsetSize = sizeof(kCharset) - 1;

enum class IoMode {
  kSync,
  kAsync,
};

enum class WorkloadMode {
  kGet,
  kPut,
  kExists,
  kDelete,
  kMGet,
  kMPut,
  kMExists,
};

enum class OpType {
  kPut,
  kGet,
  kExists,
  kDelete,
  kMPut,
  kMGet,
  kMExists,
};

bool UseColor() {
  return ::isatty(fileno(stdout)) != 0;
}

std::string Colorize(const std::string &text, const char *color) {
  if (!UseColor()) {
    return text;
  }
  return folly::to<std::string>(color, text, kColorReset);
}

std::string Red(const std::string &text) {
  return Colorize(text, kColorRed);
}

std::string Green(const std::string &text) {
  return Colorize(text, kColorGreen);
}

std::string Yellow(const std::string &text) {
  return Colorize(text, kColorYellow);
}

std::string Blue(const std::string &text) {
  return Colorize(text, kColorBlue);
}

std::string NowTs() {
  using namespace std::chrono;
  const auto now = system_clock::now();
  const auto t = system_clock::to_time_t(now);
  struct tm tm_buf;
  localtime_r(&t, &tm_buf);
  char buf[32];
  std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm_buf);
  return std::string(buf);
}

std::string HumanBytes(uint64_t bytes) {
  static constexpr std::array<const char *, 5> kUnits = {"B", "KiB", "MiB", "GiB", "TiB"};
  double value = static_cast<double>(bytes);
  size_t unit = 0;
  while (value >= 1024.0 && unit + 1 < kUnits.size()) {
    value /= 1024.0;
    ++unit;
  }
  std::ostringstream oss;
  oss << std::fixed << std::setprecision(unit == 0 ? 0 : 2) << value << " " << kUnits[unit];
  return oss.str();
}

IoMode ParseIoMode() {
  if (FLAGS_io_mode == "sync") {
    return IoMode::kSync;
  }
  if (FLAGS_io_mode == "async") {
    return IoMode::kAsync;
  }
  std::cerr << "[simm_kvio_bench_v2] invalid --io_mode=" << FLAGS_io_mode << "\n";
  std::exit(EINVAL);
}

WorkloadMode ParseWorkloadMode() {
  if (FLAGS_mode == "get") {
    return WorkloadMode::kGet;
  }
  if (FLAGS_mode == "put") {
    return WorkloadMode::kPut;
  }
  if (FLAGS_mode == "exists") {
    return WorkloadMode::kExists;
  }
  if (FLAGS_mode == "del") {
    return WorkloadMode::kDelete;
  }
  if (FLAGS_mode == "mget") {
    return WorkloadMode::kMGet;
  }
  if (FLAGS_mode == "mput") {
    return WorkloadMode::kMPut;
  }
  if (FLAGS_mode == "mexists") {
    return WorkloadMode::kMExists;
  }
  std::cerr << "[simm_kvio_bench_v2] invalid --mode=" << FLAGS_mode << "\n";
  std::exit(EINVAL);
}

void ValidateArgs(IoMode io_mode, WorkloadMode workload_mode) {
  bool valid = true;
  auto fail = [&](const std::string &reason) {
    std::cerr << "[simm_kvio_bench_v2] invalid args: " << reason << "\n";
    valid = false;
  };

  if (FLAGS_threads == 0) {
    fail("--threads must be > 0");
  }
  if (FLAGS_key_size == 0) {
    fail("--key_size must be > 0");
  }
  if (FLAGS_val_size == 0) {
    fail("--val_size must be > 0");
  }
  if (FLAGS_key_num == 0) {
    fail("--key_num must be > 0");
  }
  if (FLAGS_key_num_prepare_get == 0) {
    fail("--key_num_prepare_get must be > 0");
  }
  if (FLAGS_batchsize == 0) {
    fail("--batchsize must be > 0");
  }
  if (FLAGS_iodepth == 0) {
    fail("--iodepth must be > 0");
  }
  if (FLAGS_async_global_iodepth == 0) {
    fail("--async_global_iodepth must be > 0");
  }
  if (FLAGS_async_backpressure_retry == 0) {
    fail("--async_backpressure_retry must be > 0");
  }
  if (FLAGS_async_backpressure_backoff_us == 0) {
    fail("--async_backpressure_backoff_us must be > 0");
  }
  if (FLAGS_prefill_sync_req_timeout_ms <= 0) {
    fail("--prefill_sync_req_timeout_ms must be > 0");
  }
  if (FLAGS_run_time == 0) {
    fail("--run_time must be > 0");
  }
  if (FLAGS_report_interval == 0) {
    fail("--report_interval must be > 0");
  }
  if (FLAGS_cm_primary_node_ip.empty()) {
    fail("--cm_primary_node_ip must not be empty");
  }
  if (io_mode == IoMode::kAsync && workload_mode == WorkloadMode::kDelete) {
    fail("async delete benchmark is not supported in v2");
  }
  if (io_mode == IoMode::kAsync && FLAGS_iodepth < FLAGS_batchsize &&
      (workload_mode == WorkloadMode::kMGet || workload_mode == WorkloadMode::kMPut ||
       workload_mode == WorkloadMode::kMExists)) {
    fail("--iodepth must be >= --batchsize for async batch workloads");
  }
  if (!valid) {
    std::exit(EINVAL);
  }
}

uint64_t ValueSeed(uint32_t tid, uint32_t key_idx) {
  return (static_cast<uint64_t>(tid) << 32U) ^ static_cast<uint64_t>(key_idx) ^ static_cast<uint64_t>(FLAGS_seed);
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

std::string BuildKey(uint32_t tid, uint32_t key_idx, uint32_t key_sz) {
  std::string prefix = folly::to<std::string>("bench_t", tid, "_k", key_idx, "_");
  if (prefix.size() >= key_sz) {
    return prefix.substr(0, key_sz);
  }
  std::string key = std::move(prefix);
  std::mt19937_64 rng((static_cast<uint64_t>(tid) << 32U) ^ key_idx ^ FLAGS_seed);
  while (key.size() < key_sz) {
    key.push_back(kCharset[rng() % kCharsetSize]);
  }
  return key;
}

struct LatencySnapshot {
  uint64_t samples{0};
  uint64_t sum_us{0};
  uint64_t max_us{0};
  std::array<uint64_t, 21> buckets{};
};

class LatencyHistogram {
 public:
  void add(micro_ts latency) {
    const auto us = static_cast<uint64_t>(std::max<int64_t>(0, latency.count()));
    samples_.fetch_add(1, std::memory_order_relaxed);
    sum_us_.fetch_add(us, std::memory_order_relaxed);
    UpdateMax(us);
    buckets_[BucketIndex(us)].fetch_add(1, std::memory_order_relaxed);
  }

  LatencySnapshot snapshot() const {
    LatencySnapshot snap;
    snap.samples = samples_.load(std::memory_order_relaxed);
    snap.sum_us = sum_us_.load(std::memory_order_relaxed);
    snap.max_us = max_us_.load(std::memory_order_relaxed);
    for (size_t i = 0; i < buckets_.size(); ++i) {
      snap.buckets[i] = buckets_[i].load(std::memory_order_relaxed);
    }
    return snap;
  }

  void reset() {
    samples_.store(0, std::memory_order_relaxed);
    sum_us_.store(0, std::memory_order_relaxed);
    max_us_.store(0, std::memory_order_relaxed);
    for (auto &bucket : buckets_) {
      bucket.store(0, std::memory_order_relaxed);
    }
  }

  static uint64_t Percentile(const LatencySnapshot &snap, double p) {
    if (snap.samples == 0) {
      return 0;
    }
    const uint64_t target = std::max<uint64_t>(1, static_cast<uint64_t>(p * static_cast<double>(snap.samples)));
    uint64_t seen = 0;
    for (size_t i = 0; i < snap.buckets.size(); ++i) {
      seen += snap.buckets[i];
      if (seen >= target) {
        return BucketUpperBound(i);
      }
    }
    return BucketUpperBound(snap.buckets.size() - 1);
  }

 private:
  static constexpr std::array<uint64_t, 21> kBoundsUs = {
      10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 20000,
      50000, 100000, 200000, 500000, 1000000, 2000000, 5000000, 10000000, 30000000, UINT64_MAX};

  static size_t BucketIndex(uint64_t us) {
    for (size_t i = 0; i < kBoundsUs.size(); ++i) {
      if (us <= kBoundsUs[i]) {
        return i;
      }
    }
    return kBoundsUs.size() - 1;
  }

  static uint64_t BucketUpperBound(size_t idx) {
    return kBoundsUs[std::min(idx, kBoundsUs.size() - 1)];
  }

  void UpdateMax(uint64_t us) {
    auto curr = max_us_.load(std::memory_order_relaxed);
    while (curr < us && !max_us_.compare_exchange_weak(curr, us, std::memory_order_relaxed)) {
    }
  }

  std::atomic<uint64_t> samples_{0};
  std::atomic<uint64_t> sum_us_{0};
  std::atomic<uint64_t> max_us_{0};
  std::array<std::atomic<uint64_t>, kBoundsUs.size()> buckets_{};
};

struct ErrorCountersSnapshot {
  std::unordered_map<int32_t, uint64_t> counts;
};

struct BenchSnapshot {
  uint64_t req_total{0};
  uint64_t req_success{0};
  uint64_t req_fail{0};
  uint64_t submit_fail{0};
  uint64_t submit_backpressure{0};
  uint64_t item_total{0};
  uint64_t item_success{0};
  uint64_t item_fail{0};
  uint64_t bytes_write{0};
  uint64_t bytes_read{0};
  uint64_t put_count{0};
  uint64_t get_count{0};
  uint64_t exists_count{0};
  uint64_t delete_count{0};
  uint64_t mput_count{0};
  uint64_t mget_count{0};
  uint64_t mexists_count{0};
  LatencySnapshot latency;
  std::unordered_map<int32_t, uint64_t> err_counts;
};

class ThreadStats {
 public:
  void record_submit_fail(OpType op, int32_t rc) {
    submit_fail_.fetch_add(1, std::memory_order_relaxed);
    req_fail_.fetch_add(1, std::memory_order_relaxed);
    req_total_.fetch_add(1, std::memory_order_relaxed);
    item_fail_.fetch_add(1, std::memory_order_relaxed);
    item_total_.fetch_add(1, std::memory_order_relaxed);
    IncOpCount(op);
    RecordError(rc);
  }

  void record_submit_backpressure() {
    submit_backpressure_.fetch_add(1, std::memory_order_relaxed);
  }

  void record_request(OpType op,
                      bool success,
                      uint32_t item_total,
                      uint32_t item_success,
                      uint64_t bytes,
                      micro_ts latency,
                      const std::vector<int32_t> &error_codes = {}) {
    req_total_.fetch_add(1, std::memory_order_relaxed);
    if (success) {
      req_success_.fetch_add(1, std::memory_order_relaxed);
    } else {
      req_fail_.fetch_add(1, std::memory_order_relaxed);
    }
    item_total_.fetch_add(item_total, std::memory_order_relaxed);
    item_success_.fetch_add(item_success, std::memory_order_relaxed);
    item_fail_.fetch_add(item_total - item_success, std::memory_order_relaxed);
    IncOpCount(op);
    if (op == OpType::kPut || op == OpType::kMPut) {
      bytes_write_.fetch_add(bytes, std::memory_order_relaxed);
    } else if (op == OpType::kGet || op == OpType::kMGet) {
      bytes_read_.fetch_add(bytes, std::memory_order_relaxed);
    }
    latency_.add(latency);
    for (auto rc : error_codes) {
      if (rc != CommonErr::OK) {
        RecordError(rc);
      }
    }
  }

  BenchSnapshot snapshot() const {
    BenchSnapshot snap;
    snap.req_total = req_total_.load(std::memory_order_relaxed);
    snap.req_success = req_success_.load(std::memory_order_relaxed);
    snap.req_fail = req_fail_.load(std::memory_order_relaxed);
    snap.submit_fail = submit_fail_.load(std::memory_order_relaxed);
    snap.submit_backpressure = submit_backpressure_.load(std::memory_order_relaxed);
    snap.item_total = item_total_.load(std::memory_order_relaxed);
    snap.item_success = item_success_.load(std::memory_order_relaxed);
    snap.item_fail = item_fail_.load(std::memory_order_relaxed);
    snap.bytes_write = bytes_write_.load(std::memory_order_relaxed);
    snap.bytes_read = bytes_read_.load(std::memory_order_relaxed);
    snap.put_count = put_count_.load(std::memory_order_relaxed);
    snap.get_count = get_count_.load(std::memory_order_relaxed);
    snap.exists_count = exists_count_.load(std::memory_order_relaxed);
    snap.delete_count = delete_count_.load(std::memory_order_relaxed);
    snap.mput_count = mput_count_.load(std::memory_order_relaxed);
    snap.mget_count = mget_count_.load(std::memory_order_relaxed);
    snap.mexists_count = mexists_count_.load(std::memory_order_relaxed);
    snap.latency = latency_.snapshot();
    {
      std::lock_guard<std::mutex> g(err_mu_);
      snap.err_counts = err_counts_;
    }
    return snap;
  }

  void reset() {
    req_total_.store(0, std::memory_order_relaxed);
    req_success_.store(0, std::memory_order_relaxed);
    req_fail_.store(0, std::memory_order_relaxed);
    submit_fail_.store(0, std::memory_order_relaxed);
    submit_backpressure_.store(0, std::memory_order_relaxed);
    item_total_.store(0, std::memory_order_relaxed);
    item_success_.store(0, std::memory_order_relaxed);
    item_fail_.store(0, std::memory_order_relaxed);
    bytes_write_.store(0, std::memory_order_relaxed);
    bytes_read_.store(0, std::memory_order_relaxed);
    put_count_.store(0, std::memory_order_relaxed);
    get_count_.store(0, std::memory_order_relaxed);
    exists_count_.store(0, std::memory_order_relaxed);
    delete_count_.store(0, std::memory_order_relaxed);
    mput_count_.store(0, std::memory_order_relaxed);
    mget_count_.store(0, std::memory_order_relaxed);
    mexists_count_.store(0, std::memory_order_relaxed);
    latency_.reset();
    std::lock_guard<std::mutex> g(err_mu_);
    err_counts_.clear();
  }

  void mark_submit_error(int32_t rc) {
    submit_fail_.fetch_add(1, std::memory_order_relaxed);
    RecordError(rc);
  }

 private:
  void IncOpCount(OpType op) {
    switch (op) {
      case OpType::kPut:
        put_count_.fetch_add(1, std::memory_order_relaxed);
        break;
      case OpType::kGet:
        get_count_.fetch_add(1, std::memory_order_relaxed);
        break;
      case OpType::kExists:
        exists_count_.fetch_add(1, std::memory_order_relaxed);
        break;
      case OpType::kDelete:
        delete_count_.fetch_add(1, std::memory_order_relaxed);
        break;
      case OpType::kMPut:
        mput_count_.fetch_add(1, std::memory_order_relaxed);
        break;
      case OpType::kMGet:
        mget_count_.fetch_add(1, std::memory_order_relaxed);
        break;
      case OpType::kMExists:
        mexists_count_.fetch_add(1, std::memory_order_relaxed);
        break;
    }
  }

  void RecordError(int32_t rc) {
    std::lock_guard<std::mutex> g(err_mu_);
    err_counts_[rc]++;
  }

  std::atomic<uint64_t> req_total_{0};
  std::atomic<uint64_t> req_success_{0};
  std::atomic<uint64_t> req_fail_{0};
  std::atomic<uint64_t> submit_fail_{0};
  std::atomic<uint64_t> submit_backpressure_{0};
  std::atomic<uint64_t> item_total_{0};
  std::atomic<uint64_t> item_success_{0};
  std::atomic<uint64_t> item_fail_{0};
  std::atomic<uint64_t> bytes_write_{0};
  std::atomic<uint64_t> bytes_read_{0};
  std::atomic<uint64_t> put_count_{0};
  std::atomic<uint64_t> get_count_{0};
  std::atomic<uint64_t> exists_count_{0};
  std::atomic<uint64_t> delete_count_{0};
  std::atomic<uint64_t> mput_count_{0};
  std::atomic<uint64_t> mget_count_{0};
  std::atomic<uint64_t> mexists_count_{0};
  LatencyHistogram latency_;
  mutable std::mutex err_mu_;
  std::unordered_map<int32_t, uint64_t> err_counts_;
};

BenchSnapshot operator-(const BenchSnapshot &lhs, const BenchSnapshot &rhs) {
  BenchSnapshot delta;
  delta.req_total = lhs.req_total - rhs.req_total;
  delta.req_success = lhs.req_success - rhs.req_success;
  delta.req_fail = lhs.req_fail - rhs.req_fail;
  delta.submit_fail = lhs.submit_fail - rhs.submit_fail;
  delta.submit_backpressure = lhs.submit_backpressure - rhs.submit_backpressure;
  delta.item_total = lhs.item_total - rhs.item_total;
  delta.item_success = lhs.item_success - rhs.item_success;
  delta.item_fail = lhs.item_fail - rhs.item_fail;
  delta.bytes_write = lhs.bytes_write - rhs.bytes_write;
  delta.bytes_read = lhs.bytes_read - rhs.bytes_read;
  delta.put_count = lhs.put_count - rhs.put_count;
  delta.get_count = lhs.get_count - rhs.get_count;
  delta.exists_count = lhs.exists_count - rhs.exists_count;
  delta.delete_count = lhs.delete_count - rhs.delete_count;
  delta.mput_count = lhs.mput_count - rhs.mput_count;
  delta.mget_count = lhs.mget_count - rhs.mget_count;
  delta.mexists_count = lhs.mexists_count - rhs.mexists_count;
  delta.latency.samples = lhs.latency.samples - rhs.latency.samples;
  delta.latency.sum_us = lhs.latency.sum_us - rhs.latency.sum_us;
  delta.latency.max_us = lhs.latency.max_us;
  for (size_t i = 0; i < lhs.latency.buckets.size(); ++i) {
    delta.latency.buckets[i] = lhs.latency.buckets[i] - rhs.latency.buckets[i];
  }
  delta.err_counts = lhs.err_counts;
  for (const auto &[rc, cnt] : rhs.err_counts) {
    auto it = delta.err_counts.find(rc);
    if (it == delta.err_counts.end()) {
      continue;
    }
    if (it->second <= cnt) {
      delta.err_counts.erase(it);
    } else {
      it->second -= cnt;
    }
  }
  return delta;
}

BenchSnapshot MergeSnapshots(const std::vector<BenchSnapshot> &snaps) {
  BenchSnapshot merged;
  for (const auto &snap : snaps) {
    merged.req_total += snap.req_total;
    merged.req_success += snap.req_success;
    merged.req_fail += snap.req_fail;
    merged.submit_fail += snap.submit_fail;
    merged.submit_backpressure += snap.submit_backpressure;
    merged.item_total += snap.item_total;
    merged.item_success += snap.item_success;
    merged.item_fail += snap.item_fail;
    merged.bytes_write += snap.bytes_write;
    merged.bytes_read += snap.bytes_read;
    merged.put_count += snap.put_count;
    merged.get_count += snap.get_count;
    merged.exists_count += snap.exists_count;
    merged.delete_count += snap.delete_count;
    merged.mput_count += snap.mput_count;
    merged.mget_count += snap.mget_count;
    merged.mexists_count += snap.mexists_count;
    merged.latency.samples += snap.latency.samples;
    merged.latency.sum_us += snap.latency.sum_us;
    merged.latency.max_us = std::max(merged.latency.max_us, snap.latency.max_us);
    for (size_t i = 0; i < merged.latency.buckets.size(); ++i) {
      merged.latency.buckets[i] += snap.latency.buckets[i];
    }
    for (const auto &[rc, cnt] : snap.err_counts) {
      merged.err_counts[rc] += cnt;
    }
  }
  return merged;
}

struct WorkerRuntime {
  explicit WorkerRuntime(uint32_t worker_id) : tid(worker_id) {}

  uint32_t tid;
  ThreadStats stats;
  std::vector<std::string> keys;
  std::vector<uint32_t> shuffled_indices;
  uint32_t cursor{0};
  std::mt19937_64 rng{static_cast<uint64_t>(FLAGS_seed)};
};

class AsyncCreditLimiter;

bool NeedsPrefill(WorkloadMode mode) {
  return mode == WorkloadMode::kGet || mode == WorkloadMode::kMGet || mode == WorkloadMode::kExists ||
         mode == WorkloadMode::kMExists || mode == WorkloadMode::kDelete;
}

bool IsWriteWorkload(WorkloadMode mode) {
  return mode == WorkloadMode::kPut || mode == WorkloadMode::kMPut;
}

bool IsBatchWorkload(WorkloadMode mode) {
  return mode == WorkloadMode::kMGet || mode == WorkloadMode::kMPut || mode == WorkloadMode::kMExists;
}

OpType BatchOpType(WorkloadMode mode) {
  switch (mode) {
    case WorkloadMode::kMGet:
      return OpType::kMGet;
    case WorkloadMode::kMPut:
      return OpType::kMPut;
    case WorkloadMode::kMExists:
      return OpType::kMExists;
    default:
      break;
  }
  return OpType::kGet;
}

uint32_t PrepareKeyCount(WorkloadMode mode) {
  if (NeedsPrefill(mode)) {
    return std::min(FLAGS_key_num, FLAGS_key_num_prepare_get);
  }
  return FLAGS_key_num;
}

uint32_t NextKeyIndex(WorkerRuntime &runtime, uint32_t span) {
  if (FLAGS_random_access) {
    return static_cast<uint32_t>(runtime.rng() % span);
  }
  const uint32_t idx = runtime.cursor;
  runtime.cursor = (runtime.cursor + 1) % span;
  return idx;
}

std::optional<uint32_t> NextWriteKeyIndex(WorkerRuntime &runtime) {
  if (FLAGS_allow_overwrite) {
    return NextKeyIndex(runtime, FLAGS_key_num);
  }

  if (runtime.cursor >= FLAGS_key_num) {
    return std::nullopt;
  }

  const uint32_t idx = runtime.cursor++;
  if (FLAGS_random_access) {
    return runtime.shuffled_indices[idx];
  }
  return idx;
}

void PrepareWorkerKeys(std::vector<std::unique_ptr<WorkerRuntime>> &workers) {
  for (auto &worker_ptr : workers) {
    auto &worker = *worker_ptr;
    worker.keys.reserve(FLAGS_key_num);
    worker.shuffled_indices.reserve(FLAGS_key_num);
    worker.rng.seed((static_cast<uint64_t>(worker.tid) << 32U) ^ FLAGS_seed);
    for (uint32_t i = 0; i < FLAGS_key_num; ++i) {
      worker.keys.emplace_back(BuildKey(worker.tid, i, FLAGS_key_size));
      worker.shuffled_indices.emplace_back(i);
    }
    if (FLAGS_random_access) {
      std::shuffle(worker.shuffled_indices.begin(), worker.shuffled_indices.end(), worker.rng);
    }
  }
}

void PrepareDataBuffer(simm::clnt::Data &data, uint64_t seed) {
  FillBufferFromSeed(data.AsRef().subspan(0, FLAGS_val_size), seed);
}

bool VerifyDataBuffer(const simm::clnt::Data &data, uint64_t seed, int32_t ret_len) {
  if (ret_len < 0) {
    return false;
  }
  if (static_cast<uint32_t>(ret_len) != FLAGS_val_size) {
    return false;
  }
  return VerifyBufferFromSeed(data.AsRef().subspan(0, FLAGS_val_size), seed);
}

struct PrefillFailure {
  uint32_t worker_tid{0};
  uint32_t key_idx{0};
  int32_t rc{0};
};

class ScopedPrefillClientFlags {
 public:
  ScopedPrefillClientFlags()
      : old_enable_retry_(FLAGS_clnt_syncreq_enable_retry),
        old_retry_count_(FLAGS_clnt_syncreq_retry_count),
        old_timeout_ms_(FLAGS_clnt_sync_req_timeout_ms) {
    FLAGS_clnt_syncreq_enable_retry = FLAGS_prefill_syncreq_enable_retry;
    FLAGS_clnt_syncreq_retry_count = FLAGS_prefill_syncreq_retry_count;
    FLAGS_clnt_sync_req_timeout_ms = FLAGS_prefill_sync_req_timeout_ms;
  }

  ~ScopedPrefillClientFlags() {
    FLAGS_clnt_syncreq_enable_retry = old_enable_retry_;
    FLAGS_clnt_syncreq_retry_count = old_retry_count_;
    FLAGS_clnt_sync_req_timeout_ms = old_timeout_ms_;
  }

 private:
  const bool old_enable_retry_;
  const uint32_t old_retry_count_;
  const int32_t old_timeout_ms_;
};

void PrefillWorkerDataset(simm::clnt::KVStore &kvstore,
                          WorkerRuntime &worker,
                          WorkloadMode mode,
                          std::atomic<bool> &failed,
                          std::mutex &failure_mu,
                          std::optional<PrefillFailure> &first_failure) {
  if (!NeedsPrefill(mode)) {
    return;
  }

  const uint32_t prefill_count = PrepareKeyCount(mode);
  simm::clnt::Data put_data = kvstore.Allocate(FLAGS_val_size);
  simm::clnt::DataView put_view(put_data);
  for (uint32_t idx = 0; idx < prefill_count && !failed.load(std::memory_order_relaxed); ++idx) {
    PrepareDataBuffer(put_data, ValueSeed(worker.tid, idx));
    const auto rc = kvstore.Put(worker.keys[idx], put_view);
    if (rc != CommonErr::OK) {
      failed.store(true, std::memory_order_relaxed);
      std::lock_guard<std::mutex> g(failure_mu);
      if (!first_failure.has_value()) {
        first_failure = PrefillFailure{worker.tid, idx, rc};
      }
      return;
    }
  }
}

bool PrefillDataset(simm::clnt::KVStore &kvstore,
                    std::vector<std::unique_ptr<WorkerRuntime>> &workers,
                    WorkloadMode mode) {
  if (!NeedsPrefill(mode)) {
    return true;
  }
  ScopedPrefillClientFlags scoped_prefill_flags;
  std::vector<std::thread> threads;
  threads.reserve(workers.size());
  std::atomic<bool> failed{false};
  std::mutex failure_mu;
  std::optional<PrefillFailure> first_failure;
  for (auto &worker_ptr : workers) {
    auto *worker = worker_ptr.get();
    threads.emplace_back([&kvstore, worker, mode, &failed, &failure_mu, &first_failure] {
      PrefillWorkerDataset(kvstore, *worker, mode, failed, failure_mu, first_failure);
    });
  }
  for (auto &t : threads) {
    t.join();
  }
  if (first_failure.has_value()) {
    std::cerr << "[simm_kvio_bench_v2] prefill failed for worker " << first_failure->worker_tid
              << " key_idx=" << first_failure->key_idx << " rc=" << first_failure->rc << "\n";
    return false;
  }
  return true;
}

void RecordSyncSingle(ThreadStats &stats,
                      OpType op,
                      int32_t rc,
                      micro_ts latency,
                      uint64_t bytes,
                      bool data_ok = true) {
  const bool success = (op == OpType::kGet) ? (rc >= 0 && data_ok) : (rc == CommonErr::OK);
  stats.record_request(op, success, 1, success ? 1 : 0, success ? bytes : 0, latency, {rc});
}

void RecordSyncBatch(ThreadStats &stats,
                     OpType op,
                     const std::vector<int32_t> &ret_codes,
                     micro_ts latency,
                     uint64_t bytes_per_item) {
  uint32_t succ = 0;
  std::vector<int32_t> errs;
  errs.reserve(ret_codes.size());
  for (auto rc : ret_codes) {
    const bool ok = (op == OpType::kMGet) ? (rc >= 0) : (rc == CommonErr::OK);
    succ += ok ? 1U : 0U;
    if (!ok) {
      errs.push_back(rc);
    }
  }
  stats.record_request(op,
                       succ == ret_codes.size(),
                       static_cast<uint32_t>(ret_codes.size()),
                       succ,
                       succ * bytes_per_item,
                       latency,
                       errs);
}

void RunSyncWorker(simm::clnt::KVStore &kvstore,
                   WorkerRuntime &runtime,
                   WorkloadMode workload_mode,
                   steady_clock_t::time_point deadline) {
  const uint32_t working_key_count = PrepareKeyCount(workload_mode);
  simm::clnt::Data single_data = kvstore.Allocate(FLAGS_val_size);
  simm::clnt::DataView single_view(single_data);

  std::vector<simm::clnt::Data> batch_data_store;
  std::vector<simm::clnt::DataView> batch_views;
  std::vector<std::string> batch_keys;
  if (IsBatchWorkload(workload_mode)) {
    batch_data_store.reserve(FLAGS_batchsize);
    batch_views.reserve(FLAGS_batchsize);
    batch_keys.reserve(FLAGS_batchsize);
    for (uint32_t i = 0; i < FLAGS_batchsize; ++i) {
      batch_data_store.emplace_back(kvstore.Allocate(FLAGS_val_size));
    }
  }

  while (steady_clock_t::now() < deadline) {
    if (workload_mode == WorkloadMode::kPut) {
      const auto key_idx = NextWriteKeyIndex(runtime);
      if (!key_idx.has_value()) {
        break;
      }
      PrepareDataBuffer(single_data, ValueSeed(runtime.tid, *key_idx));
      const auto start = steady_clock_t::now();
      const auto rc = kvstore.Put(runtime.keys[*key_idx], single_view);
      const auto latency = std::chrono::duration_cast<micro_ts>(steady_clock_t::now() - start);
      RecordSyncSingle(runtime.stats, OpType::kPut, rc, latency, FLAGS_val_size);
      continue;
    }

    if (workload_mode == WorkloadMode::kGet) {
      const auto key_idx = NextKeyIndex(runtime, working_key_count);
      const auto start = steady_clock_t::now();
      const auto rc = kvstore.Get(runtime.keys[key_idx], single_view);
      const auto latency = std::chrono::duration_cast<micro_ts>(steady_clock_t::now() - start);
      const bool verified = !FLAGS_verify_get || VerifyDataBuffer(single_data, ValueSeed(runtime.tid, key_idx), rc);
      RecordSyncSingle(runtime.stats, OpType::kGet, verified ? rc : CommonErr::InternalError, latency, FLAGS_val_size, verified);
      continue;
    }

    if (workload_mode == WorkloadMode::kExists) {
      const auto key_idx = NextKeyIndex(runtime, working_key_count);
      const auto start = steady_clock_t::now();
      const auto rc = kvstore.Exists(runtime.keys[key_idx]);
      const auto latency = std::chrono::duration_cast<micro_ts>(steady_clock_t::now() - start);
      RecordSyncSingle(runtime.stats, OpType::kExists, rc, latency, 0);
      continue;
    }

    if (workload_mode == WorkloadMode::kDelete) {
      const auto key_idx = NextKeyIndex(runtime, working_key_count);
      const auto start = steady_clock_t::now();
      const auto rc = kvstore.Delete(runtime.keys[key_idx]);
      const auto latency = std::chrono::duration_cast<micro_ts>(steady_clock_t::now() - start);
      RecordSyncSingle(runtime.stats, OpType::kDelete, rc, latency, 0);
      if (rc == CommonErr::OK) {
        PrepareDataBuffer(single_data, ValueSeed(runtime.tid, key_idx));
        const auto refill_rc = kvstore.Put(runtime.keys[key_idx], single_view);
        if (refill_rc != CommonErr::OK) {
          runtime.stats.record_submit_fail(OpType::kPut, refill_rc);
        }
      }
      continue;
    }

    batch_keys.clear();
    batch_views.clear();
    std::vector<std::string> exists_keys;
    exists_keys.reserve(FLAGS_batchsize);
    std::vector<uint32_t> batch_key_indices;
    batch_key_indices.reserve(FLAGS_batchsize);
    for (uint32_t i = 0; i < FLAGS_batchsize; ++i) {
      std::optional<uint32_t> key_idx;
      if (workload_mode == WorkloadMode::kMPut) {
        key_idx = NextWriteKeyIndex(runtime);
      } else {
        key_idx = NextKeyIndex(runtime, working_key_count);
      }
      if (!key_idx.has_value()) {
        batch_key_indices.clear();
        break;
      }
      batch_key_indices.emplace_back(*key_idx);
      if (workload_mode == WorkloadMode::kMPut) {
        PrepareDataBuffer(batch_data_store[i], ValueSeed(runtime.tid, *key_idx));
        batch_keys.emplace_back(runtime.keys[*key_idx]);
        batch_views.emplace_back(batch_data_store[i]);
      } else if (workload_mode == WorkloadMode::kMGet) {
        batch_keys.emplace_back(runtime.keys[*key_idx]);
        batch_views.emplace_back(batch_data_store[i]);
      } else {
        exists_keys.emplace_back(runtime.keys[*key_idx]);
      }
    }
    if (batch_key_indices.empty()) {
      break;
    }

    const auto start = steady_clock_t::now();
    if (workload_mode == WorkloadMode::kMPut) {
      const auto rets = kvstore.MPut(batch_keys, batch_views);
      const auto latency = std::chrono::duration_cast<micro_ts>(steady_clock_t::now() - start);
      std::vector<int32_t> rc32(rets.begin(), rets.end());
      RecordSyncBatch(runtime.stats, OpType::kMPut, rc32, latency, FLAGS_val_size);
      continue;
    }

    if (workload_mode == WorkloadMode::kMGet) {
      auto rets = kvstore.MGet(batch_keys, batch_views);
      const auto latency = std::chrono::duration_cast<micro_ts>(steady_clock_t::now() - start);
      if (FLAGS_verify_get) {
        for (uint32_t i = 0; i < rets.size(); ++i) {
          if (rets[i] > 0) {
            const bool verified =
                VerifyBufferFromSeed(batch_data_store[i].AsRef().subspan(0, FLAGS_val_size),
                                     ValueSeed(runtime.tid, batch_key_indices[i]));
            if (!verified) {
              rets[i] = CommonErr::InternalError;
            }
          }
        }
      }
      RecordSyncBatch(runtime.stats, OpType::kMGet, rets, latency, FLAGS_val_size);
      continue;
    }

    const auto rets = kvstore.MExists(exists_keys);
    const auto latency = std::chrono::duration_cast<micro_ts>(steady_clock_t::now() - start);
    std::vector<int32_t> rc32(rets.begin(), rets.end());
    RecordSyncBatch(runtime.stats, OpType::kMExists, rc32, latency, 0);
  }
}

struct AsyncWorkerContext {
  simm::clnt::KVStore *kvstore;
  WorkerRuntime *runtime;
  WorkloadMode workload_mode;
  uint32_t working_key_count;
  steady_clock_t::time_point deadline;
  AsyncCreditLimiter *limiter;
  std::mutex mu;
  std::condition_variable cv;
  size_t inflight_requests{0};
  std::atomic<bool> stop{false};
};

class AsyncCreditLimiter {
 public:
  explicit AsyncCreditLimiter(size_t credits) : capacity_(credits), available_(credits) {}

  bool acquire(size_t credits, std::atomic<bool> &stop, steady_clock_t::time_point deadline) {
    std::unique_lock<std::mutex> lk(mu_);
    cv_.wait_until(lk, deadline, [&] {
      return stop.load(std::memory_order_relaxed) || available_ >= credits;
    });
    if (stop.load(std::memory_order_relaxed) || available_ < credits) {
      return false;
    }
    available_ -= credits;
    return true;
  }

  void release(size_t credits) {
    {
      std::lock_guard<std::mutex> lk(mu_);
      available_ = std::min(capacity_, available_ + credits);
    }
    cv_.notify_all();
  }

 private:
  const size_t capacity_;
  size_t available_;
  std::mutex mu_;
  std::condition_variable cv_;
};

struct AsyncBatchTracker {
  AsyncBatchTracker(AsyncWorkerContext *ctx_in, OpType op_in, uint32_t total_in, steady_clock_t::time_point start_in)
      : ctx(ctx_in), op(op_in), total(total_in), start(start_in) {}

  AsyncWorkerContext *ctx;
  OpType op;
  uint32_t total;
  steady_clock_t::time_point start;
  std::atomic<uint32_t> remaining;
  std::atomic<uint32_t> succ_items;
  std::atomic<bool> request_ok;
  std::mutex err_mu;
  std::vector<int32_t> error_codes;
};

struct AsyncSlot {
  explicit AsyncSlot(simm::clnt::KVStore &kvstore) : data(kvstore.Allocate(FLAGS_val_size)), view(data) {}

  simm::clnt::Data data;
  simm::clnt::DataView view;
  uint32_t key_idx{0};
  steady_clock_t::time_point start{};
};

void FinishAsyncBatch(const std::shared_ptr<AsyncBatchTracker> &batch, uint64_t bytes_per_item) {
  const auto latency = std::chrono::duration_cast<micro_ts>(steady_clock_t::now() - batch->start);
  std::vector<int32_t> errs;
  {
    std::lock_guard<std::mutex> g(batch->err_mu);
    errs = batch->error_codes;
  }
  batch->ctx->runtime->stats.record_request(batch->op,
                                            batch->request_ok.load(std::memory_order_relaxed),
                                            batch->total,
                                            batch->succ_items.load(std::memory_order_relaxed),
                                            batch->succ_items.load(std::memory_order_relaxed) * bytes_per_item,
                                            latency,
                                            errs);
  {
    std::lock_guard<std::mutex> g(batch->ctx->mu);
    batch->ctx->inflight_requests -= batch->total;
  }
  batch->ctx->limiter->release(batch->total);
  batch->ctx->cv.notify_one();
}

bool IsAsyncBackpressure(int32_t rc) {
  return rc == sicl::transport::SICL_ERR_CH_TX_FULL;
}

template <typename SubmitFn>
int16_t SubmitWithBackpressureRetry(AsyncWorkerContext &ctx, OpType op, SubmitFn &&submit_fn) {
  int16_t rc = CommonErr::OK;
  for (uint32_t attempt = 0; attempt < FLAGS_async_backpressure_retry; ++attempt) {
    rc = submit_fn();
    if (!IsAsyncBackpressure(rc)) {
      return rc;
    }
    ctx.runtime->stats.record_submit_backpressure();
    if (steady_clock_t::now() >= ctx.deadline) {
      return rc;
    }
    const uint32_t backoff_us =
        std::min<uint32_t>(kMaxAsyncBackoffUs, FLAGS_async_backpressure_backoff_us * (attempt + 1));
    std::this_thread::sleep_for(std::chrono::microseconds(backoff_us));
  }
  ctx.runtime->stats.record_submit_fail(op, rc);
  return rc;
}

void SubmitAsyncSingle(AsyncWorkerContext &ctx, OpType op, uint32_t key_idx) {
  auto slot = std::make_shared<AsyncSlot>(*ctx.kvstore);
  slot->key_idx = key_idx;
  slot->start = steady_clock_t::now();
  auto key = ctx.runtime->keys[key_idx];

  auto finish_single = [slot, &ctx, op](int32_t rc, bool verified, uint64_t bytes) {
    const auto latency = std::chrono::duration_cast<micro_ts>(steady_clock_t::now() - slot->start);
    const int32_t final_rc = verified ? rc : CommonErr::InternalError;
    const bool success = (op == OpType::kGet) ? (final_rc >= 0) : (final_rc == CommonErr::OK);
    ctx.runtime->stats.record_request(op,
                                      success,
                                      1,
                                      success ? 1 : 0,
                                      success ? bytes : 0,
                                      latency,
                                      {final_rc});
    {
      std::lock_guard<std::mutex> g(ctx.mu);
      ctx.inflight_requests--;
    }
    ctx.limiter->release(1);
    ctx.cv.notify_one();
  };

  int16_t submit_rc = CommonErr::OK;
  if (op == OpType::kPut) {
    PrepareDataBuffer(slot->data, ValueSeed(ctx.runtime->tid, key_idx));
    submit_rc = SubmitWithBackpressureRetry(
        ctx, op, [&]() {
          return ctx.kvstore->AsyncPut(
              key, slot->view, [finish_single](int result) { finish_single(result, true, FLAGS_val_size); });
        });
  } else if (op == OpType::kGet) {
    submit_rc = SubmitWithBackpressureRetry(
        ctx, op, [&]() {
          return ctx.kvstore->AsyncGet(
              key, slot->view, [finish_single, slot, &ctx](int result) {
                const bool verified =
                    !FLAGS_verify_get || VerifyDataBuffer(slot->data, ValueSeed(ctx.runtime->tid, slot->key_idx), result);
                finish_single(result, verified, FLAGS_val_size);
              });
        });
  } else if (op == OpType::kExists) {
    submit_rc = SubmitWithBackpressureRetry(
        ctx, op, [&]() {
          return ctx.kvstore->AsyncExists(key, [finish_single](int result) { finish_single(result, true, 0); });
        });
  }

  if (submit_rc != CommonErr::OK) {
    {
      std::lock_guard<std::mutex> g(ctx.mu);
      ctx.inflight_requests--;
    }
    ctx.limiter->release(1);
    if (!IsAsyncBackpressure(submit_rc)) {
      ctx.runtime->stats.record_submit_fail(op, submit_rc);
    }
    ctx.cv.notify_one();
  }
}

void SubmitAsyncBatchWave(AsyncWorkerContext &ctx, WorkloadMode workload_mode) {
  const OpType op = BatchOpType(workload_mode);
  auto batch = std::make_shared<AsyncBatchTracker>(&ctx, op, FLAGS_batchsize, steady_clock_t::now());
  batch->remaining.store(FLAGS_batchsize, std::memory_order_relaxed);
  batch->succ_items.store(0, std::memory_order_relaxed);
  batch->request_ok.store(true, std::memory_order_relaxed);

  for (uint32_t i = 0; i < FLAGS_batchsize; ++i) {
    auto slot = std::make_shared<AsyncSlot>(*ctx.kvstore);
    std::optional<uint32_t> key_idx;
    if (workload_mode == WorkloadMode::kMPut) {
      key_idx = NextWriteKeyIndex(*ctx.runtime);
    } else {
      key_idx = NextKeyIndex(*ctx.runtime, ctx.working_key_count);
    }
    if (!key_idx.has_value()) {
      const uint32_t submitted = i;
      const uint32_t unsent = FLAGS_batchsize - submitted;
      batch->total = submitted;
      batch->remaining.store(submitted, std::memory_order_relaxed);
      if (unsent > 0) {
        {
          std::lock_guard<std::mutex> g(ctx.mu);
          ctx.inflight_requests -= unsent;
        }
        ctx.limiter->release(unsent);
        ctx.cv.notify_one();
      }
      if (submitted == 0) {
        FinishAsyncBatch(batch, workload_mode == WorkloadMode::kMExists ? 0 : FLAGS_val_size);
      }
      return;
    }
    slot->key_idx = *key_idx;
    slot->start = batch->start;
    auto key = ctx.runtime->keys[*key_idx];

    auto on_done = [slot, batch, &ctx](int32_t rc, bool verified, uint64_t bytes) {
      const int32_t final_rc = verified ? rc : CommonErr::InternalError;
      const bool ok = (batch->op == OpType::kMGet) ? (final_rc >= 0) : (final_rc == CommonErr::OK);
      if (ok) {
        batch->succ_items.fetch_add(1, std::memory_order_relaxed);
      } else {
        batch->request_ok.store(false, std::memory_order_relaxed);
        std::lock_guard<std::mutex> g(batch->err_mu);
        batch->error_codes.push_back(final_rc);
      }
      if (batch->remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        FinishAsyncBatch(batch, bytes);
      }
    };

    int16_t submit_rc = CommonErr::OK;
    if (workload_mode == WorkloadMode::kMPut) {
      PrepareDataBuffer(slot->data, ValueSeed(ctx.runtime->tid, *key_idx));
      submit_rc = SubmitWithBackpressureRetry(
          ctx, op, [&]() {
            return ctx.kvstore->AsyncPut(
                key, slot->view, [on_done](int result) { on_done(result, true, FLAGS_val_size); });
          });
    } else if (workload_mode == WorkloadMode::kMGet) {
      submit_rc = SubmitWithBackpressureRetry(
          ctx, op, [&]() {
            return ctx.kvstore->AsyncGet(
                key, slot->view, [slot, on_done, &ctx](int result) {
                  const bool verified =
                      !FLAGS_verify_get || VerifyDataBuffer(slot->data, ValueSeed(ctx.runtime->tid, slot->key_idx), result);
                  on_done(result, verified, FLAGS_val_size);
                });
          });
    } else {
      submit_rc = SubmitWithBackpressureRetry(
          ctx, op, [&]() { return ctx.kvstore->AsyncExists(key, [on_done](int result) { on_done(result, true, 0); }); });
    }

    if (submit_rc != CommonErr::OK) {
      batch->request_ok.store(false, std::memory_order_relaxed);
      if (!IsAsyncBackpressure(submit_rc)) {
        {
          std::lock_guard<std::mutex> g(batch->err_mu);
          batch->error_codes.push_back(submit_rc);
        }
        ctx.runtime->stats.mark_submit_error(submit_rc);
      }
      if (batch->remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        FinishAsyncBatch(batch, workload_mode == WorkloadMode::kMExists ? 0 : FLAGS_val_size);
      }
    }
  }
}

void RunAsyncWorker(AsyncWorkerContext &ctx, steady_clock_t::time_point deadline) {
  while (steady_clock_t::now() < deadline) {
    const size_t wave = IsBatchWorkload(ctx.workload_mode) ? FLAGS_batchsize : 1;
    {
      std::unique_lock<std::mutex> lk(ctx.mu);
      ctx.cv.wait(lk, [&] {
        return ctx.stop.load(std::memory_order_relaxed) || (ctx.inflight_requests + wave <= FLAGS_iodepth);
      });
      if (ctx.stop.load(std::memory_order_relaxed)) {
        break;
      }
    }
    // Acquire global credits outside ctx.mu to avoid deadlock: completion
    // callbacks need ctx.mu to decrement inflight_requests, so we must not
    // hold ctx.mu while blocking here.
    if (!ctx.limiter->acquire(wave, ctx.stop, deadline)) {
      break;
    }
    {
      std::lock_guard<std::mutex> g(ctx.mu);
      ctx.inflight_requests += wave;
    }

    if (IsBatchWorkload(ctx.workload_mode)) {
      SubmitAsyncBatchWave(ctx, ctx.workload_mode);
      continue;
    }

    if (ctx.workload_mode == WorkloadMode::kPut) {
      const auto key_idx = NextWriteKeyIndex(*ctx.runtime);
      if (!key_idx.has_value()) {
        // Key space exhausted — undo the credit and inflight acquired above
        // before breaking, otherwise the drain phase hangs forever.
        {
          std::lock_guard<std::mutex> g(ctx.mu);
          ctx.inflight_requests -= wave;
        }
        ctx.limiter->release(wave);
        ctx.cv.notify_one();
        break;
      }
      SubmitAsyncSingle(ctx, OpType::kPut, *key_idx);
      continue;
    }
    if (ctx.workload_mode == WorkloadMode::kGet) {
      SubmitAsyncSingle(ctx, OpType::kGet, NextKeyIndex(*ctx.runtime, ctx.working_key_count));
      continue;
    }
    SubmitAsyncSingle(ctx, OpType::kExists, NextKeyIndex(*ctx.runtime, ctx.working_key_count));
  }

  {
    std::unique_lock<std::mutex> lk(ctx.mu);
    ctx.stop.store(true, std::memory_order_relaxed);
    ctx.cv.wait(lk, [&] { return ctx.inflight_requests == 0; });
  }
}

std::vector<BenchSnapshot> SnapshotAll(const std::vector<std::unique_ptr<WorkerRuntime>> &workers) {
  std::vector<BenchSnapshot> snaps;
  snaps.reserve(workers.size());
  for (const auto &worker_ptr : workers) {
    snaps.emplace_back(worker_ptr->stats.snapshot());
  }
  return snaps;
}

void PrintTopErrors(const std::unordered_map<int32_t, uint64_t> &err_counts) {
  if (err_counts.empty()) {
    return;
  }
  std::vector<std::pair<int32_t, uint64_t>> entries(err_counts.begin(), err_counts.end());
  std::sort(entries.begin(), entries.end(), [](const auto &lhs, const auto &rhs) { return lhs.second > rhs.second; });
  std::cout << "TopErrors       : ";
  for (size_t i = 0; i < std::min<size_t>(entries.size(), 5); ++i) {
    if (i != 0) {
      std::cout << ", ";
    }
    std::cout << entries[i].first << "=" << entries[i].second;
  }
  std::cout << "\n";
}

void PrintReport(const BenchSnapshot &snap, uint32_t threads, uint64_t elapsed_secs, bool delta_report) {
  const double req_qps = elapsed_secs == 0 ? 0.0 : static_cast<double>(snap.req_total) / static_cast<double>(elapsed_secs);
  const double item_qps = elapsed_secs == 0 ? 0.0 : static_cast<double>(snap.item_total) / static_cast<double>(elapsed_secs);
  const double throughput_mib =
      elapsed_secs == 0 ? 0.0
                        : static_cast<double>(snap.bytes_write + snap.bytes_read) / static_cast<double>(ONE_MB) /
                              static_cast<double>(elapsed_secs);
  const double avg_us = snap.latency.samples == 0
                            ? 0.0
                            : static_cast<double>(snap.latency.sum_us) / static_cast<double>(snap.latency.samples);

  auto maybe_red = [&](const std::string &label, uint64_t value) {
    return folly::to<std::string>(Red(label), ": ", Red(folly::to<std::string>(value)));
  };
  auto maybe_green = [&](const std::string &label, uint64_t value) {
    return folly::to<std::string>(Green(label), ": ", Green(folly::to<std::string>(value)));
  };

  std::cout << "++++++++++++++++++++++++++++ "
            << (delta_report ? Blue("KVIO BENCH V2 DELTA") : Blue("KVIO BENCH V2 REPORT"))
            << " +++++++++++++++++++++++++\n"
            << "Timestamp       : " << NowTs() << "\n"
            << "Mode            : " << FLAGS_mode << "\n"
            << "IOMode          : " << FLAGS_io_mode << "\n"
            << "Threads         : " << threads << "\n"
            << "ElapsedSecs     : " << elapsed_secs << "\n"
            << "KeySize         : " << FLAGS_key_size << " B\n"
            << "ValSize         : " << FLAGS_val_size << " B\n"
            << "BatchSize       : " << FLAGS_batchsize << "\n"
            << "IODepth         : " << FLAGS_iodepth << "\n"
            << "ReqTotal        : " << snap.req_total << "\n"
            << maybe_green("ReqSuccess      ", snap.req_success) << "\n"
            << maybe_red("ReqFail         ", snap.req_fail) << "\n"
            << Yellow("SubmitBackpressure") << ": " << Yellow(folly::to<std::string>(snap.submit_backpressure)) << "\n"
            << maybe_red("SubmitFail      ", snap.submit_fail) << "\n"
            << "ItemTotal       : " << snap.item_total << "\n"
            << maybe_green("ItemSuccess     ", snap.item_success) << "\n"
            << maybe_red("ItemFail        ", snap.item_fail) << "\n"
            << "PutCnt          : " << snap.put_count << "\n"
            << "GetCnt          : " << snap.get_count << "\n"
            << "ExistsCnt       : " << snap.exists_count << "\n"
            << "DeleteCnt       : " << snap.delete_count << "\n"
            << "MPutCnt         : " << snap.mput_count << "\n"
            << "MGetCnt         : " << snap.mget_count << "\n"
            << "MExistsCnt      : " << snap.mexists_count << "\n"
            << "WriteBytes      : " << HumanBytes(snap.bytes_write) << "\n"
            << "ReadBytes       : " << HumanBytes(snap.bytes_read) << "\n"
            << "ReqQPS          : " << std::fixed << std::setprecision(2) << req_qps << "\n"
            << "ItemQPS         : " << item_qps << "\n"
            << "ThroughputMiB/s : " << throughput_mib << "\n"
            << "AvgLatencyUs    : " << avg_us << "\n"
            << "P50LatencyUs    : " << LatencyHistogram::Percentile(snap.latency, 0.50) << "\n"
            << "P95LatencyUs    : " << LatencyHistogram::Percentile(snap.latency, 0.95) << "\n"
            << "P99LatencyUs    : " << LatencyHistogram::Percentile(snap.latency, 0.99) << "\n"
            << "P999LatencyUs   : " << LatencyHistogram::Percentile(snap.latency, 0.999) << "\n"
            << "MaxLatencyUs    : " << snap.latency.max_us << "\n";
  PrintTopErrors(snap.err_counts);
  if (ParseIoMode() == IoMode::kAsync && IsBatchWorkload(ParseWorkloadMode())) {
    std::cout << Yellow("Note            : async batch mode is implemented as async wave submission of single-key APIs\n");
  }
  std::cout << std::endl;
}

void RunMeasurePhase(simm::clnt::KVStore &kvstore,
                     std::vector<std::unique_ptr<WorkerRuntime>> &workers,
                     IoMode io_mode,
                     WorkloadMode workload_mode) {
  std::vector<std::thread> threads;
  threads.reserve(workers.size());
  const auto measure_start = steady_clock_t::now();
  const auto deadline = measure_start + std::chrono::seconds(FLAGS_run_time);
  auto limiter = std::make_shared<AsyncCreditLimiter>(FLAGS_async_global_iodepth);

  if (io_mode == IoMode::kSync) {
    for (auto &worker_ptr : workers) {
      threads.emplace_back([&kvstore, &worker_ptr, workload_mode, deadline] {
        RunSyncWorker(kvstore, *worker_ptr, workload_mode, deadline);
      });
    }
  } else {
    for (auto &worker_ptr : workers) {
      threads.emplace_back([&kvstore, &worker_ptr, workload_mode, deadline, limiter] {
        AsyncWorkerContext ctx{
            &kvstore, worker_ptr.get(), workload_mode, PrepareKeyCount(workload_mode), deadline, limiter.get(), {}, {}, 0, false};
        RunAsyncWorker(ctx, deadline);
      });
    }
  }

  if (!FLAGS_summary) {
    auto previous = MergeSnapshots(SnapshotAll(workers));
    auto last_report = steady_clock_t::now();
    while (steady_clock_t::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::seconds(FLAGS_report_interval));
      const auto current = MergeSnapshots(SnapshotAll(workers));
      const auto delta = current - previous;
      const auto now = steady_clock_t::now();
      const auto elapsed = std::max<uint64_t>(
          1, std::chrono::duration_cast<std::chrono::seconds>(now - last_report).count());
      PrintReport(delta, FLAGS_threads, elapsed, true);
      previous = current;
      last_report = now;
    }
  }

  for (auto &t : threads) {
    t.join();
  }

  if (FLAGS_summary) {
    const auto actual_elapsed = std::max<uint64_t>(
        1, std::chrono::duration_cast<std::chrono::seconds>(steady_clock_t::now() - measure_start).count());
    PrintReport(MergeSnapshots(SnapshotAll(workers)), FLAGS_threads, actual_elapsed, false);
  }
}

void RunWarmupPhase(simm::clnt::KVStore &kvstore,
                    std::vector<std::unique_ptr<WorkerRuntime>> &workers,
                    IoMode io_mode,
                    WorkloadMode workload_mode) {
  if (FLAGS_warmup_time == 0) {
    return;
  }
  std::cout << Yellow("[simm_kvio_bench_v2] starting warmup phase") << std::endl;
  std::vector<std::thread> threads;
  threads.reserve(workers.size());
  const auto deadline = steady_clock_t::now() + std::chrono::seconds(FLAGS_warmup_time);
  auto limiter = std::make_shared<AsyncCreditLimiter>(FLAGS_async_global_iodepth);
  if (io_mode == IoMode::kSync) {
    for (auto &worker_ptr : workers) {
      threads.emplace_back([&kvstore, &worker_ptr, workload_mode, deadline] {
        RunSyncWorker(kvstore, *worker_ptr, workload_mode, deadline);
      });
    }
  } else {
    for (auto &worker_ptr : workers) {
      threads.emplace_back([&kvstore, &worker_ptr, workload_mode, deadline, limiter] {
        AsyncWorkerContext ctx{
            &kvstore, worker_ptr.get(), workload_mode, PrepareKeyCount(workload_mode), deadline, limiter.get(), {}, {}, 0, false};
        RunAsyncWorker(ctx, deadline);
      });
    }
  }
  for (auto &t : threads) {
    t.join();
  }
  for (auto &worker_ptr : workers) {
    worker_ptr->stats.reset();
    if (FLAGS_allow_overwrite || !IsWriteWorkload(workload_mode)) {
      worker_ptr->cursor = 0;
    }
  }
}

}  // namespace

int main(int argc, char **argv) {
  gflags::SetUsageMessage(
      "simm_kvio_bench_v2 --cm_primary_node_ip=<CM_IP> --mode=get --io_mode=sync "
      "--threads=32 --run_time=300 --warmup_time=30 --key_size=20 --val_size=4096");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  folly::Init init(&argc, &argv);

#ifdef NDEBUG
  simm::logging::LogConfig clnt_log_config = simm::logging::LogConfig{FLAGS_clnt_log_file, "INFO"};
#else
  simm::logging::LogConfig clnt_log_config = simm::logging::LogConfig{FLAGS_clnt_log_file, "DEBUG"};
#endif
  simm::logging::LoggerManager::Instance().UpdateConfig("simm_client", clnt_log_config);

  const auto io_mode = ParseIoMode();
  const auto workload_mode = ParseWorkloadMode();
  ValidateArgs(io_mode, workload_mode);

  std::cout << Blue("== SIMM KVIO BENCH V2 ==") << "\n"
            << "mode=" << FLAGS_mode << ", io_mode=" << FLAGS_io_mode << ", threads=" << FLAGS_threads
            << ", key_size=" << FLAGS_key_size << ", val_size=" << FLAGS_val_size << ", batchsize=" << FLAGS_batchsize
            << ", iodepth=" << FLAGS_iodepth << ", async_global_iodepth=" << FLAGS_async_global_iodepth
            << ", allow_overwrite=" << (FLAGS_allow_overwrite ? "true" : "false")
            << ", warmup=" << FLAGS_warmup_time << "s"
            << ", run=" << FLAGS_run_time << "s\n";

  auto kvstore = std::make_unique<simm::clnt::KVStore>();
  if (kvstore == nullptr) {
    std::cerr << "[simm_kvio_bench_v2] failed to create KVStore\n";
    return ENOMEM;
  }

  std::vector<std::unique_ptr<WorkerRuntime>> workers;
  workers.reserve(FLAGS_threads);
  for (uint32_t i = 0; i < FLAGS_threads; ++i) {
    workers.emplace_back(std::make_unique<WorkerRuntime>(i));
  }

  PrepareWorkerKeys(workers);
  if (!PrefillDataset(*kvstore, workers, workload_mode)) {
    return EIO;
  }
  RunWarmupPhase(*kvstore, workers, io_mode, workload_mode);
  RunMeasurePhase(*kvstore, workers, io_mode, workload_mode);
  return 0;
}
