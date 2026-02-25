# SiMM Deployment & Operations Guide

This page summarizes depoyment methods of SiMM, and describes usefult flags, environment variables, HTTP endpoints to help advanced users tune SiMM service.

## Cluster Manager Startup Flags (with defaults)
---

- RPC Related
    - `--cm_rpc_intra_port` (int, default 30000): Port number for intra-cm RPC service, e.g. data sync.
    - `--cm_rpc_inter_port` (int, default 30001): Port number for in-cluster RPC service, e.g. communication with dataserver or client.
    - `--cm_rpc_admin_port` (int, default 30002): Port number for admin RPC service, e.g. maintainence scenario.
    - `--cm_rpc_intra_name` (str, default Intra-CM): RPC service name for cluster manager nodes internal services, e.g. metadata sync.
    - `--cm_rpc_inter_name` (str, default In-Cluster): RPC service name for in-cluster nodes, e.g. communication with dataserver or client.
    - `--cm_rpc_admin_name` (str, default Admin): RPC service name for admin maintanence scenarios.
    - `--rpc_timeout_inSecs` (int, default 1): Common RPC timeout in seconds

- Init Related
    - `--cm_cluster_init_grace_period_inSecs` (int, default 60): Cluster initialization grace period in seconds, to wait for all dataserver nodes to join cluster(with hand-shake rpc) before starting the service.

- Heartbeat Related
  - `--cm_heartbeat_records_perserver` (int, default 100): Cluster Manager will record latest N heartbeat timestamps for each dataserver.
  - `--cm_heartbeat_bg_scan_interval_inSecs` (int, default 5): Backgroud scan interval on dataserver heartbeat records.
  - `--cm_heartbeat_timeout_inSecs` (int, default 30): Timeout strategy : dataserver heartbeat timeout in seconds, if no heartbeat received in this time, the dataserver will be considered to mark as dead.

- Dataserver Manage Related
  - `--dataserver_resource_interval_inSecs` (int, default 60): Interval in seconds for cluster manager to send resource usage query RPC requests to dataserver nodes.
  - `--dataserver_min_num` (int, default 3): Minimum number of dataservers in a cluster.
  - `--dataserver_max_num` (int, default 1000): Maximum number of dataservers in a cluster.

- Data Shard Related
  - `--shard_total_num` (int, default 16384): Total number of shards per cluster.

- Log Related
  - `--cm_log_file` (str, default /var/log/simm/simm_cm.log): Simm cluster manager log file path & name.

Example (set init_grace_priod and dataserver min num):
```bash
cluster_manager \
  --cm_cluster_init_grace_period_inSecs=30 \
  --dataserver_min_num=3
```

## Data Server Startup Flags (with defaults)
---
- RPC Related
  - `--rpc_timeout_inSecs` (int, default 1): Common RPC timeout in seconds.
  - `--io_service_port` (int, default 40000): Port number for client IO RPC service.
  - `--mgt_service_port` (int, default 40001): Port number for service management RPC service.
  - `--ds_rpc_admin_port` (int, default 40002): Port number for admin RPC service, e.g. maintainence scenario.
  - `--cm_connect_retry_interval_sec` (int, default 1): Duration seconds between attempts to reconnect to Cluster Manager.

- Heartbeat Related
  - `--dataserver_heartbeat_interval_inSecs` (int, default 5): Interval in seconds for dataserver to send heartbeat RPC requests to cluster manager.
  - `--heartbeat_cooldown_sec` (int, default 5): Duration seconds to heartbeat with Cluster Manager.
  - `--cm_hb_tolerance_count` (int, default 5): Count of allowed failed heartbeats before reconnecting to Cluster Manager.

- Data Shard Related
  - `--shard_total_num` (int, default 16384): Total number of shards per cluster.

- Memory Allocate Related
  - `--ds_free_memory_usable_ratio` (int, default 80): Percentage of free memory can be used by data server.
  - `--memory_limit_bytes` (int64, default 0): Memory limit for data servers, 0 means to check /proc/meminfo.

- Log Related
  - `--ds_log_file` (str, default /var/log/simm/simm_ds.log): Simm data server log file path & name.

- K8S Related
  - `--cm_namespace` (str, default default): K8S namespace of cluster manager.
  - `--cm_svc_name` (str, default simm-clustermanager-svc): K8S service name of cluster manager.
  - `--cm_port_name` (str, default simm-clustermanager-port): K8S service port name of cluster manager.

- Debug Related
  - `--debug` (bool, default false): Enable debug mode for the data server.

Example (set each data_server use 100GiB memory):
```bash
data_server \
  --memory_limit_bytes=107374182400
```
