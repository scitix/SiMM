#include <gflags/gflags.h>

DEFINE_bool(debug, false, "Enable debug mode for the data server");
DEFINE_int32(io_service_port, 40000, "Port number for client IO RPC service");
DEFINE_int32(mgt_service_port, 40001, "Port number for service management RPC service");
DEFINE_int32(ds_rpc_admin_port, 40002, "Port number for admin RPC service, e.g. maintainence scenario");
DEFINE_int64(ds_hash_seed, 0, "Hash seed for key string mapping in hash table");
DEFINE_int32(register_cooldown_sec, 10, "Duration seconds to register with Cluster Manager");
DEFINE_int32(heartbeat_cooldown_sec, 5, "Duration seconds to heartbeat with Cluster Manager");
DEFINE_uint32(cm_hb_tolerance_count, 5, "Count of allowed failed heartbeats before reconnecting to Cluster Manager");
DEFINE_bool(ds_process_exit_cm_disconnection,
            true,
            "Exit data server gracefully after repeated CM heartbeat failures; if false, trigger CM re-register logic");
DEFINE_uint32(cm_connect_retry_interval_sec, 1, "Duration seconds between attempts to reconnect to Cluster Manager");
DEFINE_uint32(ds_free_memory_usable_ratio, 90, "Percentage of free memory can be used by data server");
DEFINE_int32(ds_initial_blocks, 3, "Initial number of cache blocks to pre-allocate");
DEFINE_int32(ds_bg_evict_thread_num, 3, "background chunk level eviction thread num");
DEFINE_uint32(ds_bg_evict_interval_ms, 500, "background chunk level eviction checking interval");
DEFINE_uint32(ds_bg_evict_prefetch_factor, 1, "background chunk level eviction prefetch factor");
DEFINE_double(ds_bg_evict_trigger_threshold, 0.95, "background chunk level eviction trigger threshold");
DEFINE_uint32(ds_bg_evict_slab_class_num, 3, "background chunk level eviction for how much slab class number");
DEFINE_uint64(ds_bg_evict_chunk_cooldown_ms,
              3600000,
              "background chunk level eviction for empty chunk cooldown duration");
DEFINE_bool(ds_clean_stale_block, false, "clean stale block before initialization");
// Used in k8s scenarios where /proc/meminfo is inaccurate, see doc comments for GetMemoryFreeToUse
DEFINE_uint64(memory_limit_bytes, 0, "Memory limit for data servers, 0 means to check /proc/meminfo");
DEFINE_uint32(busy_wait_timeout_us, 10000, "Duration microseconds to wait for certain conditions");
// Log related flags
DEFINE_string(ds_log_file, "/var/log/simm/simm_ds.log", "simm data server log file path & name");
// logical node identity flag
DEFINE_string(ds_logical_node_id, "",
              "Logical node ID for this DS; in K8s, auto-detected from POD_NAME env var. "
              "Used by CM for Deferred Reshard node identity tracking");
