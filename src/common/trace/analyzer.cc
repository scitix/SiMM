#include "analyzer.h"
#include "trace.h"

#include <algorithm>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <iostream>

namespace simm {
namespace trace {

void SegmentStats::CalculateAdvancedStats() {
  if (latencies_us.empty())
    return;

  std::sort(latencies_us.begin(), latencies_us.end());

  size_t n = latencies_us.size();
  if (n % 2 == 0) {
    median_us = (latencies_us[n / 2 - 1] + latencies_us[n / 2]) / 2.0;
  } else {
    median_us = latencies_us[n / 2];
  }

  size_t p99_idx = static_cast<size_t>(n * 0.99);
  if (p99_idx >= n)
    p99_idx = n - 1;
  p99_us = latencies_us[p99_idx];

  size_t p999_idx = static_cast<size_t>(n * 0.999);
  if (p999_idx >= n)
    p999_idx = n - 1;
  p999_us = latencies_us[p999_idx];

  double mean = sum_us / count;
  double sum_sq_diff = 0.0;
  for (double lat : latencies_us) {
    double diff = lat - mean;
    sum_sq_diff += diff * diff;
  }
  variance_us = sum_sq_diff / count;
  stddev_us = std::sqrt(variance_us);
}

bool TraceAnalyzer::Analyze(const std::string &shm_path) {
  TraceSharedMemory *shm = TraceSharedMemory::Open(shm_path);
  if (!shm) {
    std::cerr << "Failed to open shared memory: " << shm_path << std::endl;
    return false;
  }

  std::vector<RequestTraceShm *> traces = shm->ReadAllTraces();
  ProcessTraces(traces);

  shm->Close(false);
  delete shm;

  return true;
}

void TraceAnalyzer::ProcessTraces(const std::vector<RequestTraceShm *> &traces) {
  segment_stats_.clear();
  all_traces_.clear();

  total_traces_ = traces.size();

  for (auto *trace : traces) {
    if (trace) {
      ProcessTrace(trace);

      TraceData trace_data;
      trace_data.msg_tag_id = trace->msg_tag_id;
      trace_data.point_count = trace->point_count.load(std::memory_order_acquire);

      for (uint32_t i = 0; i < trace_data.point_count && i < MAX_TRACE_POINTS_PER_REQUEST; ++i) {
        uint32_t type = trace->points[i].type;
        uint64_t timestamp = trace->points[i].timestamp;
        trace_data.points.push_back({type, timestamp});
      }

      all_traces_.push_back(trace_data);
    }
  }

  for (auto &[key, stats] : segment_stats_) {
    stats.CalculateAdvancedStats();
  }
}

void TraceAnalyzer::ProcessTrace(RequestTraceShm *trace) {
  if (!trace)
    return;

  uint32_t point_count = trace->point_count.load(std::memory_order_acquire);
  if (point_count < 2)
    return;

  for (uint32_t j = 1; j < point_count; ++j) {
    const auto &point_from = trace->points[j - 1];
    const auto &point_to = trace->points[j];

    max_time_stamp_ = std::max(max_time_stamp_, point_to.timestamp);
    min_time_stamp_ = std::min(min_time_stamp_, point_from.timestamp);

    TracePointType from_type = static_cast<TracePointType>(point_from.type);
    TracePointType to_type = static_cast<TracePointType>(point_to.type);

    uint64_t ns = point_to.timestamp - point_from.timestamp;
    if (point_to.timestamp < point_from.timestamp || ns > 1000000000ULL) {
      continue;
    }

    double us = static_cast<double>(ns) / 1000.0;

    uint32_t key = (static_cast<uint32_t>(from_type) << 16) | static_cast<uint32_t>(to_type);

    auto &stats = segment_stats_[key];
    stats.count++;
    stats.sum_us += us;
    stats.min_us = std::min(stats.min_us, us);
    stats.max_us = std::max(stats.max_us, us);
    stats.latencies_us.push_back(us);

    if (stats.name.empty()) {
      stats.name = GetSegmentName(from_type, to_type);
    }
  }
}

void TraceAnalyzer::PrintStatistics() const {
  if (segment_stats_.empty()) {
    std::cout << "[SiMM Trace Analyzer] No data found" << std::endl;
    return;
  }

  // Determine dynamic width for the "Segment" column
  size_t max_name_len = std::string("Segment").size();
  for (const auto &kv : segment_stats_) {
    max_name_len = std::max(max_name_len, kv.second.name.size());
  }
  const int name_w = static_cast<int>(std::max<size_t>(40, max_name_len));
  const int count_w = 10;
  const int num_w = 12;   // numeric columns width
  const int metrics = 7;  // Avg/Min/Max/Median/P99/P99.9/StdDev
  const int total_w = name_w + count_w + metrics * num_w;

  std::cout << "\n=== SiMM Trace Statistics (Total Requests: " << total_traces_ << ") ===" << std::endl;
  std::cout << std::fixed << std::setprecision(2);

  auto now_time = get_timestamp_ns();
  std::cout << "Overall Time Range: " << static_cast<double>(now_time - min_time_stamp_) / 1e9 << " s ago to "
            << static_cast<double>(now_time - max_time_stamp_) / 1e9 << " s ago" << std::endl;

  // Header: left align name, right align numbers
  std::cout << std::left << std::setw(name_w) << "Segment" << std::right << std::setw(count_w) << "Count"
            << std::setw(num_w) << "Avg (us)" << std::setw(num_w) << "Min (us)" << std::setw(num_w) << "Max (us)"
            << std::setw(num_w) << "Median (us)" << std::setw(num_w) << "P99 (us)" << std::setw(num_w) << "P99.9 (us)"
            << std::setw(num_w) << "StdDev (us)" << std::endl;
  std::cout << std::string(total_w, '-') << std::endl;

  std::vector<std::pair<uint32_t, SegmentStats>> sorted_stats(segment_stats_.begin(), segment_stats_.end());
  std::sort(sorted_stats.begin(), sorted_stats.end(), [](const auto &a, const auto &b) { return a.first < b.first; });

  // Calculate total statistics
  uint64_t total_count = 0;
  double total_avg_us = 0.0;
  double total_min_us = 0.0;
  double total_max_us = 0.0;
  double total_median_us = 0.0;
  double total_p99_us = 0.0;
  double total_p999_us = 0.0;
  bool first_segment = true;

  for (const auto &[key, stats] : sorted_stats) {
    double avg_us = stats.sum_us / stats.count;
    std::cout << std::left << std::setw(name_w) << stats.name << std::right << std::setw(count_w) << stats.count
              << std::setw(num_w) << avg_us << std::setw(num_w) << stats.min_us << std::setw(num_w) << stats.max_us
              << std::setw(num_w) << stats.median_us << std::setw(num_w) << stats.p99_us << std::setw(num_w)
              << stats.p999_us << std::setw(num_w) << stats.stddev_us << std::endl;

    // Accumulate for total: sum all metrics, count takes max value
    if (first_segment) {
      total_count = stats.count;
      total_avg_us = avg_us;
      total_min_us = stats.min_us;
      total_max_us = stats.max_us;
      total_median_us = stats.median_us;
      total_p99_us = stats.p99_us;
      total_p999_us = stats.p999_us;
      first_segment = false;
    } else {
      // Count: take maximum (for async serialization cases)
      total_count = std::max(total_count, stats.count);
      // Other fields: sum
      total_avg_us += avg_us;
      total_min_us += stats.min_us;
      total_max_us += stats.max_us;
      total_median_us += stats.median_us;
      total_p99_us += stats.p99_us;
      total_p999_us += stats.p999_us;
    }
  }

  // Print total row
  std::cout << std::string(total_w, '-') << std::endl;
  std::cout << std::left << std::setw(name_w) << "TOTAL" << std::right << std::setw(count_w) << total_count
            << std::setw(num_w) << total_avg_us << std::setw(num_w) << total_min_us << std::setw(num_w) << total_max_us
            << std::setw(num_w) << total_median_us << std::setw(num_w) << total_p99_us << std::setw(num_w)
            << total_p999_us << std::setw(num_w) << "" << std::endl;  // StdDev left empty

  std::cout << std::endl;
}

void TraceAnalyzer::PrintAllTraces() const {
  if (all_traces_.empty()) {
    std::cout << "[SiMM Trace Analyzer] No traces found" << std::endl;
    return;
  }

  std::cout << "\n=== All Collected Traces (Total: " << all_traces_.size() << ") ===" << std::endl;
  std::cout << std::endl;

  for (size_t i = 0; i < all_traces_.size(); ++i) {
    const auto &trace_data = all_traces_[i];

    std::cout << "Trace #" << (i + 1) << " (msg_tag_id=" << trace_data.msg_tag_id
              << ", points=" << trace_data.point_count << "):" << std::endl;

    if (trace_data.point_count == 0 || trace_data.points.empty()) {
      std::cout << "  (empty trace)" << std::endl;
      continue;
    }

    uint64_t start_time = trace_data.points[0].second;
    for (size_t j = 0; j < trace_data.points.size(); ++j) {
      uint32_t type = trace_data.points[j].first;
      uint64_t timestamp = trace_data.points[j].second;
      uint64_t relative_time = timestamp - start_time;
      double relative_time_us = static_cast<double>(relative_time) / 1000.0;

      TracePointType point_type = static_cast<TracePointType>(type);
      std::string type_name = GetTracePointTypeName(point_type);

      std::cout << "  [" << std::setw(8) << std::fixed << std::setprecision(2) << relative_time_us << " us] "
                << "Type=" << std::setw(3) << static_cast<int>(type) << " (" << type_name << ")" << std::endl;
    }

    if (trace_data.points.size() > 1) {
      std::cout << "  Segments:" << std::endl;
      for (size_t j = 1; j < trace_data.points.size(); ++j) {
        uint64_t ns = trace_data.points[j].second - trace_data.points[j - 1].second;
        double us = static_cast<double>(ns) / 1000.0;
        TracePointType from_type = static_cast<TracePointType>(trace_data.points[j - 1].first);
        TracePointType to_type = static_cast<TracePointType>(trace_data.points[j].first);
        std::string seg_name = GetSegmentName(from_type, to_type);
        std::cout << "    " << std::setw(40) << seg_name << ": " << std::setw(12) << std::fixed << std::setprecision(2)
                  << us << " us" << std::endl;
      }
    }
    std::cout << std::endl;
  }
}

bool TraceAnalyzer::ExportToJson(const std::string &output_file) const {
  std::ofstream ofs(output_file);
  if (!ofs.is_open()) {
    std::cerr << "Failed to open output file: " << output_file << std::endl;
    return false;
  }

  ofs << std::fixed << std::setprecision(3);
  ofs << "{\n";
  ofs << "  \"total_traces\": " << total_traces_ << ",\n";
  ofs << "  \"traces\": [\n";

  for (size_t i = 0; i < all_traces_.size(); ++i) {
    const auto &trace_data = all_traces_[i];

    ofs << "    {\n";
    ofs << "      \"trace_id\": " << i << ",\n";
    ofs << "      \"msg_tag_id\": " << trace_data.msg_tag_id << ",\n";
    ofs << "      \"point_count\": " << trace_data.point_count << ",\n";
    ofs << "      \"points\": [\n";

    uint64_t start_time = trace_data.points.empty() ? 0 : trace_data.points[0].second;

    for (size_t j = 0; j < trace_data.points.size(); ++j) {
      uint32_t type = trace_data.points[j].first;
      uint64_t timestamp = trace_data.points[j].second;
      uint64_t relative_time = timestamp - start_time;
      double relative_time_us = static_cast<double>(relative_time) / 1000.0;

      TracePointType point_type = static_cast<TracePointType>(type);
      std::string type_name = GetTracePointTypeName(point_type);

      ofs << "        {\n";
      ofs << "          \"index\": " << j << ",\n";
      ofs << "          \"type\": " << type << ",\n";
      ofs << "          \"type_name\": \"" << type_name << "\",\n";
      ofs << "          \"timestamp_ns\": " << timestamp << ",\n";
      ofs << "          \"relative_time_us\": " << relative_time_us;

      if (j > 0) {
        uint64_t ns = timestamp - trace_data.points[j - 1].second;
        double us = static_cast<double>(ns) / 1000.0;
        TracePointType from_type = static_cast<TracePointType>(trace_data.points[j - 1].first);
        std::string seg_name = GetSegmentName(from_type, point_type);

        ofs << ",\n";
        ofs << "          \"segment_latency_us\": " << us << ",\n";
        ofs << "          \"segment_name\": \"" << seg_name << "\"";
      }

      ofs << "\n        }";
      if (j < trace_data.points.size() - 1) {
        ofs << ",";
      }
      ofs << "\n";
    }

    ofs << "      ]\n";
    ofs << "    }";
    if (i < all_traces_.size() - 1) {
      ofs << ",";
    }
    ofs << "\n";
  }

  ofs << "  ],\n";
  ofs << "  \"segment_stats\": [\n";

  std::vector<std::pair<uint32_t, SegmentStats>> sorted_stats(segment_stats_.begin(), segment_stats_.end());
  std::sort(sorted_stats.begin(), sorted_stats.end(), [](const auto &a, const auto &b) { return a.first < b.first; });

  for (size_t i = 0; i < sorted_stats.size(); ++i) {
    const auto &[key, stats] = sorted_stats[i];
    double avg_us = stats.sum_us / stats.count;

    ofs << "    {\n";
    ofs << "      \"segment_name\": \"" << stats.name << "\",\n";
    ofs << "      \"count\": " << stats.count << ",\n";
    ofs << "      \"avg_us\": " << avg_us << ",\n";
    ofs << "      \"min_us\": " << stats.min_us << ",\n";
    ofs << "      \"max_us\": " << stats.max_us << ",\n";
    ofs << "      \"median_us\": " << stats.median_us << ",\n";
    ofs << "      \"p99_us\": " << stats.p99_us << ",\n";
    ofs << "      \"p999_us\": " << stats.p999_us << ",\n";
    ofs << "      \"stddev_us\": " << stats.stddev_us << "\n";
    ofs << "    }";
    if (i < sorted_stats.size() - 1) {
      ofs << ",";
    }
    ofs << "\n";
  }

  ofs << "  ]\n";
  ofs << "}\n";

  ofs.close();
  std::cout << "Exported trace data to: " << output_file << std::endl;
  return true;
}

}  // namespace trace
}  // namespace simm
