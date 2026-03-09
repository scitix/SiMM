# SiMM Trace Analyzer

The SiMM Trace Analyzer is a tool for analyzing trace data stored in shared memory. It can also export traces to JSON for further analysis and visualization in Python.

## Features

1. **Latency statistics**: compute latency metrics such as mean, median, P99, P99.9, standard deviation, etc.
2. **Data export**: export trace data to JSON files.
3. **Visualization support**: provide JSON outputs that can be consumed by Python scripts to generate charts.

## Runtime Control

Trace can be controlled dynamically at runtime via RPC or Unix domain sockets.

### Enable / Disable Trace at Runtime

Use the `simmctl` tool with the `trace` subcommand to toggle tracing for a specific client process via the admin UDS channel:

```bash
# Turn trace ON for a client process with PID <PID>
./build/debug/bin/tools/simmctl --ip <IP> -P <PID> trace 1

# Turn trace OFF for the same process
./build/debug/bin/tools/simmctl --ip <IP> -P <PID> trace 0
```

- `-P <PID>`: target client PID. The trace server listens on a Unix domain socket derived from this PID.
- `trace 1` / `trace 0`: enable / disable trace.

### Adjust Trace-Related GFlags at Runtime

Trace-related configuration flags (for example the shared memory size flag `simm_trace_shm_size_mb`) can be inspected and updated via `simmctl gflag` over RPC or UDS:

```bash
# List all gflags on the target
./build/debug/bin/tools/simmctl --ip <IP> gflag list

# Get a specific gflag value (e.g., simm_trace_shm_size_mb)
./build/debug/bin/tools/simmctl --ip <IP> gflag get simm_trace_shm_size_mb

# Set a specific gflag value
./build/debug/bin/tools/simmctl --ip <IP> gflag set simm_trace_shm_size_mb 128
```

When used with `-P <PID>`, the same commands go through the Unix domain admin channel:

```bash
# List gflags via UDS admin channel for a specific client
./build/debug/bin/tools/simmctl --ip <IP> -P <PID> gflag list
```

## Usage

### 1. Basic Analysis

```bash
# Analyze all available trace files
./simm_trace_analyzer

# Analyze a specific trace file
./simm_trace_analyzer /dev/shm/.simm_trace.helloworld_client.12345

# List all available trace files
./simm_trace_analyzer -l

# Print detailed trace information
./simm_trace_analyzer -a
```

### 2. Export JSON Data

```bash
# Export all traces to a JSON file
./simm_trace_analyzer -o traces.json

# Analyze a specific file and export to JSON
./simm_trace_analyzer -o traces.json /dev/shm/.simm_trace.helloworld_client.12345
```

### 3. Python Visualization

First install the dependencies:

```bash
pip install matplotlib numpy
```

Then run the visualization script:

```bash
# Generate charts from a JSON export
python3 src/common/trace/visualize.py build/bin/traces_.simm_trace.helloworld_client.12345.json
```

The JSON output can also be consumed by your own Python or data analysis scripts for customized dashboards and post-processing.