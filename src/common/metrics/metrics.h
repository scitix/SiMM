#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include <prometheus/counter.h>
#include <prometheus/exposer.h>
#include <prometheus/histogram.h>
#include <prometheus/registry.h>
#include <prometheus/summary.h>

#include "common/errcode/errcode_def.h"

namespace simm {
namespace common {

class Metrics {
 public:
  const prometheus::Summary::Quantiles quantiles = {{0.5, 0.01},
                                                    {0.9, 0.01},
                                                    {0.95, 0.001},
                                                    {0.99, 0.001},
                                                    {0.999, 0.0001}};

  static Metrics &Instance(const std::string &type);

  // Latency metric in microseconds
  void ObserveRequestDuration(const std::string &type, double microseconds);

  // Request count metric
  void IncRequestsTotal(const std::string &type);

  // Error count metric
  void IncErrorsTotal(const std::string &type);

  // Read bytes metric for throughput calculation
  void IncReadTotal(double bytes);

  // Write bytes metric for throughput calculation
  void IncWrittenTotal(double bytes);

 private:
  Metrics(const std::string &type);
  ~Metrics() = default;

  Metrics(const Metrics &) = delete;
  Metrics &operator=(const Metrics &) = delete;
  Metrics(Metrics &&) = delete;
  Metrics &operator=(Metrics &&) = delete;

#ifdef SIMM_ENABLE_METRICS
  std::shared_ptr<prometheus::Registry> registry_;

  prometheus::Family<prometheus::Summary>* request_duration_microseconds_family_{nullptr};
  prometheus::Family<prometheus::Counter>* requests_total_family_{nullptr};
  prometheus::Family<prometheus::Counter>* errors_total_family_{nullptr};
  prometheus::Family<prometheus::Counter>* read_bytes_total_family_{nullptr};
  prometheus::Family<prometheus::Counter>* written_bytes_total_family_{nullptr};

  prometheus::Counter* requests_total_{nullptr};
  prometheus::Counter* errors_total_{nullptr};
  prometheus::Counter* read_bytes_total_{nullptr};
  prometheus::Counter* written_bytes_total_{nullptr};

  std::unique_ptr<prometheus::Exposer> exposer_;
  std::string instance_;
#endif
};

}  // namespace common
}  // namespace simm
