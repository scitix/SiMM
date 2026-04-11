# SiMM Admin Control Tool

Unified admin tool to operate maintenance works on SiMM Cluster, Nodes, Shards, Global Flags, Tracing, and process status.

## Binary Output

```bash
# source code
tools/simm_ctl_admin.cc

# binary output path after build
build/${build_mode}/bin/tools/simmctl
```

## Two Communication Modes

simmctl supports two modes for communicating with CM/DS processes:

| Mode | Options | Transport | Use Case |
|------|---------|-----------|----------|
| RPC mode | `--ip`, `--port` | SiCL RPC (RDMA) | Remote admin from any host with RDMA |
| UDS mode | `--pid` or `--proc` | Unix domain socket | Local admin on the same host, no RDMA needed |

**UDS mode** connects to the AdminServer inside CM/DS via a local Unix domain socket (`/run/simm/admin_cm.<pid>.sock` or `/run/simm/admin_ds.<pid>.sock`). It must run on the same host as the target process.

`--pid` and `--proc` are mutually exclusive. When `--proc` is used, simmctl resolves the PID automatically via `pgrep`. If multiple processes match, an error is returned.

## Help Output

```
Usage: simmctl [OPTIONS] SUBCOMMAND [ARGS]

OPTIONS:
  -h [ --help ]              Show help message
  -i [ --ip ] arg            Target IP address for RPC-based commands
  -p [ --port ] arg (=30002) Target port for RPC-based commands
  -P [ --pid ] arg (=-1)     Target process PID for UDS-based commands
  --proc arg                 Target process name for UDS-based commands
                             (cluster_manager or data_server)
  -v [ --verbose ]           Enable verbose output

SUBCOMMANDS:
  node list [OPTIONS]           List all nodes
  node summary [OPTIONS]        Show cluster-wide node resource summary
  node stat <IP:PORT>           Show detailed resource stats for one node
  node set <IP:PORT> <STATUS>   Set node status (0=DEAD, 1=RUNNING)
  cm status                     Query CM internal status via UDS
  ds status                     Query DS internal status via UDS
  shard list [OPTIONS]          List all shards
  gflag list [OPTIONS]          List all gflags
  gflag get <NAME> [OPTIONS]    Get a gflag value
  gflag set <NAME> <VALUE>      Set a gflag value
  trace <STATUS>                Set tracing status (0=OFF, 1=ON)

UDS options (--pid or --proc, mutually exclusive):
  --pid <PID>                   Target process by PID
  --proc <NAME>                 Target process by name (cluster_manager or data_server)
```

## Subcommand Mode Support

| Subcommand | RPC mode | UDS mode |
|------------|----------|----------|
| `node list` | yes | yes |
| `node summary` | yes | no |
| `node stat` | yes | no |
| `node set` | yes | no |
| `cm status` | no | yes |
| `ds status` | no | yes |
| `shard list` | yes | yes |
| `gflag list` | yes | yes |
| `gflag get` | yes | yes |
| `gflag set` | yes | yes |
| `trace` | yes | yes |

## Cheatsheet

```
simmctl
  ├── [RPC] -i <ip> -p <port=30002>
  │   ├── node
  │   │   ├── list [-v]
  │   │   ├── summary [-v]
  │   │   ├── stat <IP:PORT> [-v]
  │   │   └── set <IP:PORT> <STATUS>
  │   ├── shard
  │   │   └── list [-v]
  │   ├── gflag
  │   │   ├── list [-v]
  │   │   ├── get <NAME> [-v]
  │   │   └── set <NAME> <VALUE>
  │   └── trace <0|1>
  │
  └── [UDS] --pid <PID> | --proc <NAME>
      ├── node
      │   └── list [-v]
      ├── cm
      │   └── status
      ├── ds
      │   └── status
      ├── shard
      │   └── list [-v]
      ├── gflag
      │   ├── list [-v]
      │   ├── get <NAME> [-v]
      │   └── set <NAME> <VALUE>
      └── trace <0|1>
```

## Command Examples

### Node Subcommand

#### List all nodes (RPC mode)

```bash
simmctl --ip=172.168.1.1 node list
```

output:

```
+--------------------+------------+
| Node Address       | Status     |
+--------------------+------------+
| 172.18.77.65:40000 | RUNNING    |
+--------------------+------------+
| 172.18.11.43:40000 | DEAD       |
+--------------------+------------+
```

#### List all nodes (UDS mode)

```bash
# By PID
simmctl --pid 12345 node list

# By process name
simmctl --proc cluster_manager node list
```

#### Show cluster resource summary (RPC mode)

```bash
simmctl --ip=172.168.1.1 node summary
```

#### Show single node resource stats (RPC mode)

```bash
simmctl --ip=172.168.1.1 node stat 172.18.77.65:40000
```

#### Set status of single DataServer (RPC mode)

```bash
simmctl --ip=172.168.1.1 node set 172.18.11.43:40000 DEAD
```

**STATUS** types: **RUNNING**, **DEAD**

⚠️ **CAUTION**: Setting a DataServer status to **DEAD** may trigger background shard migration!

### CM / DS Status Subcommand (UDS only)

#### Query CM internal status

```bash
# By PID
simmctl --pid 12345 cm status

# By process name
simmctl --proc cluster_manager cm status
```

#### Query DS internal status

```bash
# By PID
simmctl --pid 67890 ds status

# By process name
simmctl --proc data_server ds status
```

output:

```
+----------------------------+-------+
| Field                      | Value |
+----------------------------+-------+
| is_registered              | true  |
| cm_ready                   | true  |
| heartbeat_failure_count    | 0     |
+----------------------------+-------+
```

| Field | Meaning |
|-------|---------|
| `is_registered` | DS has completed handshake with CM |
| `cm_ready` | DS considers CM reachable (false when consecutive HB failures reach tolerance) |
| `heartbeat_failure_count` | Consecutive heartbeat failure counter (resets on success or re-registration) |

### Shard Subcommand

#### Query cluster shard distribution (RPC mode)

```bash
simmctl --ip=172.168.1.1 shard list
```

output:

```
+--------------------+---------------+
| Data Server        | Shard Count   |
+--------------------+---------------+
| 172.18.11.43:40000 | 8192          |
+--------------------+---------------+
| 172.18.77.65:40000 | 8192          |
+--------------------+---------------+
```

Use `-v` / `--verbose` to see complete shard distribution list (one shard per line).

#### Query cluster shard distribution (UDS mode)

```bash
# By PID
simmctl --pid 12345 shard list

# By process name
simmctl --proc cluster_manager shard list
```

### Global Flag Subcommand

#### List all gflags (RPC mode)

```bash
simmctl --ip=172.168.1.1 --port=30002 gflag list
```

output:

```
+------------------------------+------------------------------+
| Flag Name                    | VALUE                        |
+------------------------------+------------------------------+
| alsologtoemail               |                              |
+------------------------------+------------------------------+
...
```

Use `-v` / `--verbose` to display Key/Value/Default/Type/Description.

#### List all gflags (UDS mode)

```bash
# By PID — query CM's gflags
simmctl --pid 12345 gflag list

# By process name — query DS's gflags
simmctl --proc data_server gflag list
```

#### Get a single gflag

```bash
# RPC mode
simmctl --ip=172.168.1.1 --port=30002 gflag get cm_heartbeat_timeout_inSecs

# UDS mode — by PID
simmctl --pid 12345 gflag get cm_heartbeat_timeout_inSecs

# UDS mode — by process name
simmctl --proc cluster_manager gflag get cm_heartbeat_timeout_inSecs
```

output:

```
+--------------------+--------------------------------------------------+
| Flag Name          | cm_heartbeat_timeout_inSecs                      |
+--------------------+--------------------------------------------------+
| VALUE              | 30                                               |
+--------------------+--------------------------------------------------+
```

#### Set a single gflag

```bash
# RPC mode
simmctl --ip=172.168.1.1 --port=30002 gflag set cm_heartbeat_timeout_inSecs 10

# UDS mode — by PID
simmctl --pid 12345 gflag set cm_heartbeat_timeout_inSecs 10

# UDS mode — by process name
simmctl --proc cluster_manager gflag set cm_heartbeat_timeout_inSecs 10
```

### Trace Subcommand

```bash
# Enable tracing (RPC mode)
simmctl --ip=172.168.1.1 --port=30002 trace 1

# Disable tracing (UDS mode — by PID)
simmctl --pid 12345 trace 0

# Disable tracing (UDS mode — by process name)
simmctl --proc data_server trace 0
```

## Limitations

- **UDS mode is local-only**: `simmctl --pid` / `--proc` connects via Unix domain socket on the same host. For remote hosts, SSH to the target host first, or use RPC mode.
- **`--proc` requires unique process**: fails if zero or multiple processes match. Use `--pid` for disambiguation.
- **Payload size**: admin requests with payload larger than 1 MB (server-side `--admin_max_payload_bytes` flag) are rejected.
- **RPC mode requires RDMA**: `--ip` / `--port` uses SiCL RPC, which requires RDMA hardware.

## Issues

If you find any issues about simmctl tool, feel free to create an issue to report it!
