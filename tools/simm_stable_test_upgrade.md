# simm_stable_test Upgrade Notes

## Background

The original `tools/simm_stable_test.cc` was more of a functional smoke-test tool, with several notable shortcomings:

- The `async` path was largely incomplete; `iodepth` had no real effect
- The workload model was too simplistic to support long-running stability testing
- No periodic statistics or latency metrics, making it hard to observe during extended runs
- Keys were essentially generated once, unable to produce realistic overwrite, hotspot, or live-data pressure
- Shutdown logic was too coarse for long-running scenarios

The goal of this change is to elevate the tool into one better suited for sustained stress testing, stability regression, and fault observation.

## Key Changes

### 1. Complete the async mode

- Implement true asynchronous submission and callback handling
- `iodepth` now actually controls per-worker concurrent async request count
- Support async `put/get/exists/delete`
- `async + batch` combination is explicitly disallowed to prevent use of incomplete paths

### 2. Introduce long-running stable workload model

- Each worker owns a fixed logical keyspace
- Keys are reused within the keyspace instead of only writing new keys
- Support mixed traffic closer to real-world services:
  - `putratio`
  - `getratio`
  - `existsratio`
  - `delratio`
  - `oputratio`

This enables coverage of:

- Overwrite scenarios
- Stale data reads
- Access after deletion
- Repeated hotspot key updates

### 3. Improve data verification

- No longer store a full value copy per key
- Instead track `(exists, size, seed)` metadata
- Value content is deterministically generated and verified via `seed`

Benefits:

- Reduced memory footprint of the tool itself
- Suitable for longer runs
- Still able to verify consistency of `get` return data

### 4. Add latency and throughput statistics

New metrics:

- Total operation count
- Success/failure counts
- Submit failure count
- Data match/mismatch counts
- put/get byte volumes
- Latency statistics:
  - avg
  - p50
  - p95
  - p99
  - max

### 5. Add periodic reporting

New `report_interval_inSecs` flag:

- Periodically print incremental statistics
- Print final summary at test completion

This makes the tool better suited for:

- Long-running observation
- Problem diagnosis
- Performance regression comparison

### 6. Improve shutdown and cleanup

- Register `SIGINT` / `SIGTERM` handlers
- Support graceful shutdown upon receiving stop signals
- Async workers wait for inflight requests to complete before exiting

This reduces:

- Abnormal tool termination
- Incomplete statistics at end of long runs
- Issues from exiting before callbacks have settled

### 7. Improve usability

- Add more complete flags
- `--help` now shows proper usage information
- Print key test parameters at startup

## New/Adjusted Parameters

- `putratio`
- `getratio`
- `existsratio`
- `delratio`
- `oputratio`
- `keyspace_per_thread`
- `report_interval_inSecs`
- `strict_verify_exists`

## Current Limitations

- `async + batch_mode` is not yet supported
- The tool tests stability from the client perspective only; it does not replace server-side profiling/resource analysis
- Latency statistics use bucketed percentile estimation, not exact quantiles from full samples

## Build Verification

The following verifications have been completed:

- `simm_stable_test` builds successfully under `build/release`
- `simm_stable_test` builds successfully under `build/debug`
- `--help` startup smoke test passes

Before testing, set:

```bash
export SICL_LOG_LEVEL=WARN
```

## Suggested Future Enhancements

If further iteration is planned, consider prioritizing:

- Add error code bucketed statistics
- Add a more explicit hotspot distribution model
- Add periodic summary file output
- Support fault injection integration scenarios
- Add finer-grained async timeout/callback anomaly observation
