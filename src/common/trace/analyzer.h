#pragma once

#include "trace.h"
#include "trace_shm.h"

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

namespace simm {
namespace trace {

struct SegmentStats {
  uint64_t count = 0;
  double sum_us = 0.0;
  double min_us = 1e9;
  double max_us = 0.0;
  double median_us = 0.0;
  double p99_us = 0.0;
  double p999_us = 0.0;
  double variance_us = 0.0;
  double stddev_us = 0.0;
  std::vector<double> latencies_us;
  std::string name;

  void CalculateAdvancedStats();
};

class TraceAnalyzer {
 public:
  bool Analyze(const std::string &shm_path);
  void PrintStatistics() const;
  void PrintAllTraces() const;
  bool ExportToJson(const std::string &output_file) const;
  uint64_t GetTotalTraces() const { return total_traces_; }

 private:
  void ProcessTraces(const std::vector<RequestTraceShm *> &traces);
  void ProcessTrace(RequestTraceShm *trace);

  struct TraceData {
    uint64_t msg_tag_id;
    uint32_t point_count;
    std::vector<std::pair<uint32_t, uint64_t>> points;
  };

  uint64_t total_traces_ = 0;
  std::unordered_map<uint32_t, SegmentStats> segment_stats_;
  std::vector<TraceData> all_traces_;
  uint64_t max_time_stamp_ = 0;
  uint64_t min_time_stamp_ = UINT64_MAX;
};

}  // namespace trace
}  // namespace simm
