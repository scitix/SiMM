# Cluster Integration Test Framework Design Document

## 1. Overview

A pytest-based end-to-end integration test framework for SiMM's cluster management protocols (node join/leave, heartbeat, shard rebalancing, deferred reshard). Tests run against real CM and DS binaries — no mocking. Fault injection via process signals, state observation via Admin CLI tools and log parsing.

Key properties:

- **Non-invasive**: zero modification to production code (except AdminServer, documented separately)
- **Multi-machine**: test runner on node A, CM/DS on remote nodes via passwordless SSH
- **YAML-driven**: declarative scenario definitions with composable overrides
- **Dual verification**: Admin RPC (primary) + log parsing (fallback for non-RDMA environments)

## 2. Motivation

- **Unit tests are insufficient**: existing gtest tests (`test_cm_rebalance.cc`) use MockDataServer. They verify CM logic in isolation but cannot catch issues in the full handshake/heartbeat/rebalance protocol across real processes.
- **Protocol correctness**: SiMM's distributed protocols (heartbeat failure detection, shard migration, deferred reshard) involve timing, concurrency, and multi-process coordination that require end-to-end verification.
- **Regression safety**: as the cluster management layer evolves, automated integration tests catch regressions that unit tests miss.

## 3. Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  Test Runner (Node A)                                           │
│                                                                 │
│  pytest                                                         │
│    ├── conftest.py (fixtures: cluster_small, cluster_medium)    │
│    ├── tests/test_*.py (48 test cases)                          │
│    └── framework/                                               │
│         ├── SimmCluster        ← high-level orchestration       │
│         ├── ProcessManager     ← start/stop/signal via SSH      │
│         ├── ClusterObserver    ← state queries + assertions     │
│         ├── FaultInjector      ← SIGKILL/SIGSTOP/iptables       │
│         ├── AdminClient        ← wraps simm_ctl_admin CLI       │
│         ├── LogParser          ← remote log tailing + parsing   │
│         ├── ScenarioRunner     ← YAML-driven test execution     │
│         ├── PortAllocator      ← dynamic port allocation        │
│         ├── SshExecutor        ← SSH command abstraction         │
│         └── config.py          ← YAML config + ClusterConfig    │
│                                                                 │
│  simm_ctl_admin ──(SiCL RPC)──► CM admin port                  │
│  simm_ctl_admin ──(UDS)──────► DS /run/simm/admin/simm_ds.<pid>.sock │
│  simm_flags_admin ─(SiCL RPC)─► CM/DS admin port               │
│  ssh ────────────────────────► Node B, C, D (CM/DS hosts)       │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────┐   ┌─────────────────────┐
│  Node B (CM host)   │   │  Node C/D (DS hosts) │
│  cluster_manager    │   │  data_server          │
│    :30001 inter     │   │    :40000 io          │
│    :30000 intra     │   │    :40001 mgt         │
│    :30002 admin     │   │    :40002 admin       │
│    UDS: simm_cm.*   │   │    UDS: simm_ds.*     │
└─────────────────────┘   └─────────────────────┘
```

### Single-Machine Mode

When `SIMM_CLUSTER_CONFIG` is not set (or `cm_host` is None), all processes run on localhost. No SSH overhead — commands execute as local subprocesses.

### Multi-Machine Mode

Set `SIMM_CLUSTER_CONFIG` to a YAML file specifying host topology. The test runner SSHs to remote hosts to start/stop processes, read logs, and probe ports.

## 4. Module Design

### 4.1 SshExecutor — SSH Command Abstraction

Unified interface for local and remote command execution. Automatically detects localhost (127.0.0.1, ::1, localhost) and skips SSH for local commands.

```python
class SshExecutor:
    run(host, command, timeout=30, check=True) -> CompletedProcess
    run_background(host, command) -> int | None    # nohup, returns PID
    send_signal(host, pid, sig) -> bool
    is_process_alive(host, pid) -> bool
    read_file(host, path) -> str
    read_file_tail(host, path, offset=0) -> tuple[str, int]  # incremental reads
    find_free_port(host) -> int | None
    run_iptables(host, args) -> bool
    check_connectivity(host) -> bool
```

SSH configuration: batch mode, `StrictHostKeyChecking=no`, non-interactive. Requires passwordless SSH (key-based auth).

### 4.2 PortAllocator — Dynamic Port Allocation

Thread-safe port allocator supporting both local and remote hosts.

```python
class PortAllocator:
    allocate(count=1, host="127.0.0.1") -> list[int]
    allocate_cm_ports(host) -> {"intra": N, "inter": N, "admin": N}
    allocate_ds_ports(host) -> {"io": N, "mgt": N, "admin": N}
    release_all()
```

Strategy: bind-then-close probing with per-host collision tracking. Retries up to 100 times per port.

### 4.3 ProcessManager — Process Lifecycle Management

Manages CM and DS binaries across local and remote hosts. Each process tracked by `ProcessHandle`.

```python
@dataclass
class ProcessHandle:
    pid: int
    process: subprocess.Popen | None   # None for remote processes
    role: str                           # "cm" or "ds"
    index: int                          # ds index (0, 1, ...), 0 for cm
    ip: str                             # host IP
    host: str                           # SSH target address
    ports: dict[str, int]
    log_path: str                       # path on target host
    cmd_args: list[str]                 # for restart with same args
    build_dir: str

class ProcessManager:
    start_cluster_manager(host, ip, ports, build_dir, log_dir,
        cm_cluster_init_grace_period_inSecs=5,
        cm_heartbeat_timeout_inSecs=10,
        cm_heartbeat_bg_scan_interval_inSecs=2,
        shard_total_num=64,
        cm_deferred_reshard_enabled=True,
        cm_deferred_reshard_window_inSecs=120,
        extra_flags=None) -> ProcessHandle

    start_data_server(cm_ip, cm_inter_port, host, ip, ports, build_dir, log_dir,
        heartbeat_cooldown_sec=2,
        register_cooldown_sec=2,
        memory_limit_bytes=1<<30,
        ds_logical_node_id="",
        extra_flags=None) -> ProcessHandle

    kill(handle, sig=SIGKILL)
    stop(handle, timeout=10)            # SIGTERM + wait
    freeze(handle) / unfreeze(handle)   # SIGSTOP / SIGCONT
    is_alive(handle) -> bool
    restart(handle) -> ProcessHandle    # kill + start with same args
    cleanup_all()                       # teardown: SIGCONT + SIGKILL all
```

Parameter names (e.g., `cm_heartbeat_timeout_inSecs`) match SiMM gflag names exactly, ensuring transparent configuration mapping.

### 4.4 AdminClient — Non-Invasive State Queries

Wraps `simm_ctl_admin` and `simm_flags_admin` as subprocess calls. Parses tabulate output.

```python
class AdminClient:
    # Node operations (via simm_ctl_admin --ip --port, SiCL RPC)
    list_nodes(cm_ip, cm_admin_port) -> list[NodeInfo]
    set_node_status(cm_ip, cm_admin_port, node_addr, status) -> bool

    # Shard operations
    list_shards(cm_ip, cm_admin_port) -> dict[str, int]
    list_shards_verbose(cm_ip, cm_admin_port) -> dict[str, list[int]]

    # DS status (via simm_ctl_admin --pid, UDS, local only)
    get_ds_status(ds_pid) -> {"is_registered": "true/false",
                              "cm_ready": "true/false",
                              "heartbeat_failure_count": "N"}

    # GFlag operations (via simm_flags_admin, SiCL RPC)
    get_flag(ip, port, flag_name) -> str | None
    set_flag(ip, port, flag_name, value) -> bool
    list_flags(ip, port) -> dict[str, str]
```

Two communication paths:
- **SiCL RPC** (`--ip`/`--port`): for CM admin operations (node list, shard list, gflags). Requires RDMA.
- **Unix Domain Socket** (`--pid`): for DS internal status queries. Local only, no RDMA required.

### 4.5 LogParser — Log Analysis (Fallback Path)

Parses SiMM spdlog-format logs for event extraction. Supports remote logs via SSH.

```python
class LogParser:
    wait_for_pattern(pattern, timeout=30) -> str | None  # incremental tail
    find_handshake_events() -> list[str]
    find_heartbeat_timeout_events() -> list[str]
    find_rebalance_events() -> list[str]
    find_node_status_changes() -> list[str]
    count_pattern(pattern) -> int
    contains(pattern) -> bool
```

Uses byte-offset-based incremental tailing (`read_file_tail`) to efficiently follow growing log files without re-reading from the start.

### 4.6 ClusterObserver — State Observation & Assertions

Central module for querying cluster state, waiting for conditions, and asserting invariants. Uses AdminClient as primary path, falls back to LogParser.

```python
class ClusterObserver:
    # CM-side state queries (via Admin RPC)
    get_alive_node_count() -> int
    get_dead_node_count() -> int
    get_all_node_statuses() -> dict[str, str]
    get_shard_distribution() -> dict[str, int]

    # DS-side state queries (via UDS admin)
    get_ds_status(ds_pid) -> dict[str, str]

    # Condition waiters (polling with timeout)
    wait_for_node_count(expected, timeout=60) -> bool
    wait_for_node_status(node_addr, status, timeout=60) -> bool
    wait_for_shard_coverage(expected_total, timeout=60) -> bool
    wait_for_rebalance_complete(timeout=60) -> bool
    wait_for_stable_state(duration=10) -> bool
    wait_for_ds_registered(ds_pid, timeout=60) -> bool
    wait_for_ds_cm_not_ready(ds_pid, timeout=60) -> bool

    # Invariant assertions
    assert_no_orphaned_shards()
    assert_total_shard_count(expected)
    assert_shard_balance(max_imbalance_ratio=0.3)
    assert_all_nodes_running()
    assert_ds_is_registered(ds_pid)
    assert_ds_cm_ready(ds_pid, expected=True)
    assert_ds_heartbeat_failure_count(ds_pid, min_count, max_count)
```

### 4.7 FaultInjector — Fault Injection

```python
class FaultInjector:
    kill_process(handle)                    # SIGKILL
    graceful_stop(handle)                   # SIGTERM + wait
    freeze_process(handle)                  # SIGSTOP
    unfreeze_process(handle)                # SIGCONT
    kill_multiple(handles, simultaneous=True)

    @contextmanager
    temporary_partition(handle, peer_handles, duration=None)
        # Bidirectional iptables DROP rules (TCP/IP only)
        # NOTE: does NOT work for SiCL/RDMA transport
```

| Fault Type | Mechanism | SiMM Behavior | Verification |
|-----------|-----------|---------------|--------------|
| SIGKILL | `kill -9` | DS stops immediately | CM marks DEAD after heartbeat timeout, reshards |
| SIGTERM | `kill -15` | DS graceful exit | Same as SIGKILL from CM perspective |
| SIGSTOP | `kill -19` | DS hangs, HB stops | CM marks DEAD; SIGCONT resumes, DS re-registers |
| Network partition | iptables DROP | HB packets blocked | **NOT working for RDMA** — tests skipped |
| CM crash | Kill CM + restart | DS detects HB failure | DS re-registers via RegisterToRestartedManager |

### 4.8 SimmCluster — High-Level Orchestration

```python
class SimmCluster:
    cm: ProcessHandle
    data_servers: list[ProcessHandle]
    observer: ClusterObserver
    fault_injector: FaultInjector
    process_manager: ProcessManager

    start()                     # start CM, wait, start all DS
    wait_ready(timeout=120)     # wait for all DS registered + shards assigned
    restart_cm() -> ProcessHandle
    add_data_server(ports, host, ds_logical_node_id) -> ProcessHandle
    teardown()                  # kill all, release ports
```

Startup sequence:
1. Start CM with allocated ports
2. Sleep 1s for CM initialization
3. Start DS nodes with 0.2s stagger (avoids port contention)
4. Initialize ClusterObserver and FaultInjector
5. `wait_ready()`: sleep grace period, verify all processes alive, poll node count via Admin RPC

Mode detection: `config.cm_host is not None` triggers multi-machine mode. In multi-machine mode, DS nodes are distributed round-robin across `config.ds_hosts`.

### 4.9 ScenarioRunner — YAML-Driven Orchestration

Declarative test execution engine with pluggable fault and validation registries.

```python
class ScenarioRunner:
    run_scenario(*yaml_paths: str | Path)
    run(cluster_config, fault_configs, validation_steps)
```

Execution flow:
1. Load and merge YAML files (base + overrides)
2. Start cluster, wait for ready
3. Execute faults sequentially (with optional delay)
4. Run validation steps
5. Teardown (guaranteed via try/finally)

Fault and validation types are registered via decorators:

```python
@register_fault("sigkill")
def _exec_sigkill(cluster, fault): ...

@register_validation("node_status")
def _validate_node_status(cluster, step): ...
```

Built-in faults: `sigkill`, `sigterm`, `sigstop`, `sigcont`, `kill_multiple`, `restart_cm`

Built-in validations: `node_status`, `alive_node_count`, `all_nodes_running`, `shard_total`, `shard_balance`, `no_orphaned_shards`, `node_has_no_shards`, `ds_status`

### 4.10 Config — YAML Configuration

```python
@dataclass
class ClusterConfig:
    # Topology
    num_data_servers: int = 3
    shard_total_num: int = 64
    cm_host: HostConfig | None = None       # None = single-machine
    ds_hosts: list[HostConfig] = []

    # CM gflags (names match source)
    cm_cluster_init_grace_period_inSecs: int = 5
    cm_heartbeat_timeout_inSecs: int = 10
    cm_heartbeat_bg_scan_interval_inSecs: int = 2

    # DS gflags (names match source)
    heartbeat_cooldown_sec: int = 2
    register_cooldown_sec: int = 2
    cm_hb_tolerance_count: int = 5

    # Deferred reshard
    cm_deferred_reshard_enabled: bool = True
    cm_deferred_reshard_window_inSecs: int = 120
    ds_logical_node_id_prefix: str = "simm-ds"

    # Computed properties
    cm_failure_detection_max_sec -> float   # heartbeat_timeout + 2*scan_interval
    ds_cm_failure_detection_sec -> float    # tolerance_count * heartbeat_cooldown
```

YAML composition via `deep_merge()` enables scenario reuse:

```yaml
# base: clusters/small.yaml
cluster:
  num_data_servers: 3
  shard_total_num: 64
  cluster_manager:
    cm_heartbeat_timeout_inSecs: 10

# override: overrides/fast_heartbeat.yaml
cluster:
  cluster_manager:
    cm_heartbeat_timeout_inSecs: 6
    cm_heartbeat_bg_scan_interval_inSecs: 1
```

## 5. Verification Strategy

Three-layer verification, with graceful degradation for non-RDMA environments:

| Layer | Mechanism | What It Verifies | RDMA Required |
|-------|-----------|-----------------|---------------|
| Admin RPC (CM-side) | `simm_ctl_admin --ip --port node list / shard list` | Node status, shard distribution | Yes |
| UDS Admin (DS-side) | `simm_ctl_admin --pid ds status` | `is_registered`, `cm_ready`, `heartbeat_failure_count` | No |
| Log Parsing | Regex on CM/DS log files | Handshake events, timeout events, rebalance events | No |
| Process State | `kill -0 <pid>` | Process alive/dead | No |

### Core Invariants

Checked after every fault injection:

1. **Shard total preserved**: `sum(shard_count) == shard_total_num` (no loss or duplication)
2. **No orphaned shards**: no shard assigned to a DEAD node after rebalance completes
3. **Shard balance**: `(max - min) / avg <= max_imbalance_ratio` across alive nodes
4. **Recovery completeness**: after fault recovery, all surviving/restarted DS report `is_registered=true`, `cm_ready=true`

### RDMA Dependency Handling

```bash
# With RDMA — full verification
pytest tests/cluster_integration/ -v --timeout=120

# Without RDMA — skip Admin RPC tests
SIMM_TEST_NO_RDMA=1 pytest tests/cluster_integration/ -v \
    -m "not requires_rdma and not requires_root" --timeout=120
```

Tests requiring Admin RPC are marked `@pytest.mark.requires_rdma`. Tests requiring iptables are marked `@pytest.mark.requires_root`.

## 6. Test Coverage

48 test cases across 12 test modules:

| Module | Tests | Description | Markers |
|--------|-------|-------------|---------|
| `test_node_join.py` | 5 | Node registration, initial shard distribution | — |
| `test_heartbeat.py` | 5 | Steady-state heartbeat, DS status healthy | requires_rdma (1) |
| `test_failure_detection.py` | 4 | Kill DS, CM detects DEAD, shard migration, timing | — |
| `test_rebalance.py` | 5 | Shard migration correctness, timing, balance | — |
| `test_multi_failure.py` | 2 | Simultaneous/sequential multi-DS failure | — |
| `test_cm_restart.py` | 5 | CM crash, DS detects failure, re-registration | requires_rdma |
| `test_node_rejoin.py` | 3 | Freeze/unfreeze, kill/restart, DS status | requires_rdma |
| `test_graceful_shutdown.py` | 2 | SIGTERM handling, shard migration | — |
| `test_flag_management.py` | 4 | Runtime gflag get/set/list | requires_rdma |
| `test_network_partition.py` | 2 | Network partition (SKIPPED — awaiting RDMA-aware method) | requires_root |
| `test_deferred_reshard.py` | 9 | Replace within window, timeout, disabled mode, edge cases | requires_rdma |
| `test_scenario.py` | 3 | YAML-driven scenario execution | — |

### Test Categories

**Normal Operations** (10 tests):
- Node join and initial shard distribution
- Steady-state heartbeat
- Runtime gflag management

**Failure Detection** (6 tests):
- Single DS kill → DEAD detection + timing verification
- SIGTERM graceful shutdown

**Shard Rebalancing** (7 tests):
- Shard migration after node failure
- Shard total preservation
- Balance verification
- Multi-node simultaneous failure

**Recovery** (8 tests):
- CM crash → all DS re-register
- DS freeze/unfreeze → rejoin
- DS kill/restart → rejoin

**Deferred Reshard** (9 tests):
- Replace within window (IP update, no reshard)
- Window timeout → degrade to DEAD + reshard
- Disabled mode → immediate DEAD
- Edge cases: wrong logical ID, fast restart, multiple DS down

**Scenario-Driven** (3 tests):
- YAML-defined fault + validation sequences

## 7. Pytest Integration

### Fixtures (`conftest.py`)

| Fixture | Scope | Description |
|---------|-------|-------------|
| `has_rdma` | session | Detects `/sys/class/infiniband` or `SIMM_TEST_NO_RDMA` |
| `has_root` | session | Checks `geteuid() == 0` |
| `build_dir` | session | Resolves SiMM build directory |
| `cluster_small` | function | 1 CM + 3 DS, fast heartbeat settings, auto-teardown |
| `cluster_medium` | function | 1 CM + 6 DS, auto-teardown |
| `cluster_from_config` | function | Factory fixture accepting `ClusterConfig` |

### Markers

| Marker | Purpose |
|--------|---------|
| `requires_rdma` | Test uses Admin RPC via SiCL (needs RDMA hardware) |
| `requires_root` | Test uses iptables (needs root privileges) |
| `slow` | Test takes > 60s |

### Configuration (`pytest.ini`)

```ini
[pytest]
testpaths = tests
timeout = 120
log_cli = true
log_cli_level = INFO
```

## 8. YAML Scenario Format

```yaml
cluster:
  num_data_servers: 3
  shard_total_num: 64
  cluster_manager:
    cm_cluster_init_grace_period_inSecs: 5
    cm_heartbeat_timeout_inSecs: 10
    cm_heartbeat_bg_scan_interval_inSecs: 2
  data_servers:
    heartbeat_cooldown_sec: 2
    register_cooldown_sec: 2

faults:
  - type: sigkill
    targets: ["ds:0"]
    delay_after_sec: 5
  - type: sigstop
    targets: ["ds:1"]
    duration_sec: 30

validations:
  - type: node_status
    target: "ds:0"
    expected: DEAD
    within_sec: 20
  - type: shard_total
    expected: "64"
  - type: no_orphaned_shards
  - type: shard_balance
    max_imbalance_ratio: 0.3
  - type: ds_status
    target: "ds:1"
    field: cm_ready
    expected: "true"
```

Scenarios compose via YAML merging: `run_scenario("clusters/small.yaml", "overrides/fast_heartbeat.yaml", "faults/kill_one_ds.yaml")`.

## 9. Multi-Machine Topology

### Topology YAML (`SIMM_CLUSTER_CONFIG`)

```yaml
ssh:
  user: simm
  port: 22

cm_host:
  ip: 10.0.1.10
  build_dir: /opt/simm/build/release/bin
  log_dir: /var/log/simm/test

ds_hosts:
  - ip: 10.0.1.11
    build_dir: /opt/simm/build/release/bin
    log_dir: /var/log/simm/test
  - ip: 10.0.1.12
    build_dir: /opt/simm/build/release/bin
    log_dir: /var/log/simm/test
```

DS nodes are distributed round-robin across `ds_hosts`. If `num_data_servers > len(ds_hosts)`, multiple DS run on the same host (different ports).

### SSH Requirements

- Passwordless SSH from test runner to all hosts
- Binaries pre-deployed at `build_dir` on each host
- `/run/simm/` writable on each host (for AdminServer UDS sockets)

## 10. Module Dependency Graph

```
conftest.py
  └── SimmCluster (cluster.py)
        ├── ProcessManager (process_manager.py)
        │     └── SshExecutor (ssh_executor.py)
        ├── PortAllocator (port_allocator.py)
        │     └── SshExecutor
        ├── AdminClient (admin_client.py)
        ├── ClusterObserver (cluster_observer.py)
        │     ├── AdminClient
        │     └── LogParser (log_parser.py)
        │           └── SshExecutor
        └── FaultInjector (fault_injector.py)
              ├── ProcessManager
              └── SshExecutor

ScenarioRunner (scenario_runner.py)
  ├── SimmCluster
  └── config.py (ClusterConfig, FaultConfig)
```

## 11. File List

| File | Description |
|------|-------------|
| `framework/__init__.py` | Package init |
| `framework/ssh_executor.py` | SSH command execution, local/remote abstraction |
| `framework/port_allocator.py` | Thread-safe dynamic port allocation |
| `framework/process_manager.py` | CM/DS process lifecycle management |
| `framework/admin_client.py` | simm_ctl_admin / simm_flags_admin CLI wrapper |
| `framework/log_parser.py` | Log file parsing and event extraction |
| `framework/cluster_observer.py` | State observation, condition waiting, invariant assertions |
| `framework/fault_injector.py` | SIGKILL/SIGSTOP/iptables fault injection |
| `framework/cluster.py` | SimmCluster high-level orchestration |
| `framework/config.py` | YAML configuration and ClusterConfig |
| `framework/scenario_runner.py` | YAML-driven test scenario engine |
| `conftest.py` | pytest fixtures and session configuration |
| `pytest.ini` | pytest settings, markers, timeout defaults |
| `tests/test_node_join.py` | Node registration tests (5) |
| `tests/test_heartbeat.py` | Steady-state heartbeat tests (5) |
| `tests/test_failure_detection.py` | Failure detection tests (4) |
| `tests/test_rebalance.py` | Shard rebalancing tests (5) |
| `tests/test_multi_failure.py` | Multi-failure tests (2) |
| `tests/test_cm_restart.py` | CM crash recovery tests (5) |
| `tests/test_node_rejoin.py` | DS rejoin tests (3) |
| `tests/test_graceful_shutdown.py` | SIGTERM handling tests (2) |
| `tests/test_flag_management.py` | Runtime gflag tests (4) |
| `tests/test_network_partition.py` | Network partition tests (2, SKIPPED) |
| `tests/test_deferred_reshard.py` | Deferred reshard tests (9) |
| `tests/test_scenario.py` | YAML scenario tests (3) |

## 12. Known Limitations

1. **Network partition via iptables does not work for RDMA**: SiCL uses RDMA bypass, not TCP/IP. `test_network_partition.py` is skipped pending an RDMA-aware partition injection method.
2. **DS status via UDS is local-only**: `simm_ctl_admin --pid` connects to a Unix domain socket on the same host. In multi-machine mode, DS status queries require SSH to the DS host first.
3. **Admin RPC requires RDMA hardware**: `simm_ctl_admin --ip --port` uses SiCL RPC. Tests using this path are marked `requires_rdma` and skipped in non-RDMA environments.

## 13. Running Tests

```bash
cd proj/SiMM

# Build binaries
./build.sh --mode=release

# Full test suite (requires RDMA)
pytest tests/cluster_integration/ -v --timeout=120

# Without RDMA
SIMM_TEST_NO_RDMA=1 pytest tests/cluster_integration/ -v \
    -m "not requires_rdma and not requires_root" --timeout=120

# Single test module
pytest tests/cluster_integration/tests/test_failure_detection.py -v

# Multi-machine mode
SIMM_CLUSTER_CONFIG=/path/to/topology.yaml \
    pytest tests/cluster_integration/ -v --timeout=180
```
