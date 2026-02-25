#include <gflags/gflags_declare.h>
#include <prometheus/histogram.h>
#include <prometheus/registry.h>
#include <prometheus/summary.h>

#include "common/errcode/errcode_def.h"
#include "common/utils/ip_util.h"
#include "common/logging/logging.h"
#include "metrics.h"

DECLARE_uint32(metrics_port);

DECLARE_LOG_MODULE("metrics");

namespace simm {
namespace common {

Metrics &Metrics::Instance(const std::string &type) {
  static Metrics inst{type};
  return inst;
}

Metrics::Metrics(const std::string &type) {
#ifdef SIMM_ENABLE_METRICS
  registry_ = std::make_shared<prometheus::Registry>();
  instance_ = utils::GetHostIp();

  request_duration_microseconds_family_ = &prometheus::BuildSummary()
                                              .Name(type + "_request_duration_microseconds")
                                              .Help("Request duration in microseconds")
                                              .Labels({{"instance", instance_}})
                                              .Register(*registry_);
  requests_total_family_ = &prometheus::BuildCounter()
                                .Name(type + "_requests_total")
                                .Help("Total requests by type")
                                .Labels({{"instance", instance_}})
                                .Register(*registry_);
  errors_total_family_ = &prometheus::BuildCounter()
                              .Name(type + "_errors_total")
                              .Help("Total errors by type")
                              .Labels({{"instance", instance_}})
                              .Register(*registry_);
  read_bytes_total_family_ = &prometheus::BuildCounter()
                                  .Name(type + "_read_bytes_total")
                                  .Help("Total bytes read by instance")
                                  .Labels({{"instance", instance_}})
                                  .Register(*registry_);
  written_bytes_total_family_ = &prometheus::BuildCounter()
                                     .Name(type + "_written_bytes_total")
                                     .Help("Total bytes written by instance")
                                     .Labels({{"instance", instance_}})
                                     .Register(*registry_);

  read_bytes_total_ = &read_bytes_total_family_->Add({});
  written_bytes_total_ = &written_bytes_total_family_->Add({});

  try {
    exposer_ = std::make_unique<prometheus::Exposer>("0.0.0.0:" + std::to_string(FLAGS_metrics_port));
    exposer_->RegisterCollectable(registry_);
  } catch (const std::exception &e) {
    MLOG_ERROR("Metrics exposer start failed: {}", e.what());
  }
#endif
}

void Metrics::ObserveRequestDuration(const std::string &type, double microseconds) {
#ifdef SIMM_ENABLE_METRICS
  request_duration_microseconds_family_->Add({{"type", type}}, quantiles).Observe(microseconds);
#endif
}

void Metrics::IncRequestsTotal(const std::string &type) {
#ifdef SIMM_ENABLE_METRICS
  requests_total_family_->Add({{"type", type}}).Increment();
#endif
}

void Metrics::IncErrorsTotal(const std::string &type) {
#ifdef SIMM_ENABLE_METRICS
  errors_total_family_->Add({{"type", type}}).Increment();
#endif
}

void Metrics::IncReadTotal(double bytes) {
#ifdef SIMM_ENABLE_METRICS
  read_bytes_total_->Increment(bytes);
#endif
}

void Metrics::IncWrittenTotal(double bytes) {
#ifdef SIMM_ENABLE_METRICS
  written_bytes_total_->Increment(bytes);
#endif
}

}  // namespace common
}  // namespace simm
