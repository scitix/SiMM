#pragma once

#include <gflags/gflags.h>

DEFINE_bool(simm_enable_trace, false, "enable kv request latency tracing");
DEFINE_double(simm_trace_sample_rate, 0.1, "trace sample rate (0.0 - 1.0)");
DEFINE_int32(simm_trace_shm_size_mb, 256, "trace shared memory size in MB");