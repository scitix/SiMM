#include <gflags/gflags.h>

DEFINE_string(cm_rpc_intra_name,
              "Intra-CM",
              "RPC service name for cluster manager nodes internal services, e.g. metadata sync");
DEFINE_string(cm_rpc_inter_name,
              "In-Cluster",
              "RPC service name for in-cluster nodes, e.g. communication with dataserver or client");
DEFINE_string(cm_rpc_admin_name, "Admin", "RPC service name for admin maintanence scenarios");

DEFINE_uint32(cm_cluster_init_grace_period_inSecs,
              60,
              "Cluster initialization grace period in seconds, "
              "to wait for all dataserver nodes to join cluster(with hand-shake rpc) before starting the service");
DEFINE_uint32(cm_aggregated_shards_migration_timewindow_inSecs,
              900,
              "When ClusterManager receives a hand-shake rpc from, "
              "it won't trigger shards migration immediately, it will wait for one time window to aggregate more new "
              "dataservers to join, and trigger batch shards migration tasks for them");

// HB monitor related flags
DEFINE_uint32(cm_heartbeat_records_perserver, 100, "cm will record latest N heartbeat timestamps for each dataserver");
DEFINE_uint32(cm_heartbeat_bg_scan_interval_inSecs, 5, "backgroud scan interval on dataserver heartbeat records");
DEFINE_uint32(cm_heartbeat_timeout_inSecs,
              30,
              "Timeout strategy : dataserver heartbeat timeout in seconds,"
              "if no heartbeat received in this time, the dataserver will be considered to mark as dead");

// Deferred Reshard related flags
DEFINE_bool(cm_deferred_reshard_enabled,
            true,
            "Enable Deferred Reshard: when a DS heartbeat times out, CM waits for a replacement DS "
            "with the same logical_node_id before triggering reshard");
DEFINE_uint32(cm_deferred_reshard_window_inSecs,
              120,
              "Deferred Reshard window in seconds: if no replacement DS registers within this window, "
              "CM falls back to standard reshard");

// Node Manager related flags
DEFINE_uint32(dataserver_resource_interval_inSecs,
              60,
              "Interval in seconds for cluster manager "
              "to send resource usage query RPC requests to dataserver nodes");

// Log related flags
DEFINE_string(cm_log_file, "/var/log/simm/simm_cm.log", "simm cluster manager log file path & name");
