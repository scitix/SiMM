/*
 *  define common global flags which are not limited to a specific module
 */

#include <gflags/gflags.h>

DEFINE_string(product_name, "SiMM", "Product name, used for logging and versioning");
DEFINE_string(product_version, "v0.1.0", "Product version, used for logging and versioning");

// log flags
DEFINE_string(default_logging_file, "/var/log/simm/simm.log", "Logging file for the default module");
DEFINE_string(default_logging_level, "INFO", "Logging level for the default module");

// bool type to print build information of module binary
DEFINE_bool(buildinfo, false, "Print simm version information of module binary, then exit");

// CM related common flags
DEFINE_string(cm_primary_node_ip, "", "For dataserver & client test usage");
DEFINE_int32(cm_rpc_intra_port, 30000, "Port number for intra-cm RPC service, e.g. data sync");
DEFINE_int32(cm_rpc_inter_port,
             30001,
             "Port number for in-cluster RPC service, e.g. communication with dataserver or client");
DEFINE_int32(cm_rpc_admin_port, 30002, "Port number for admin RPC service, e.g. maintainence scenario");
DEFINE_string(cm_namespace, "default", "K8S namespace of cluster manager");
DEFINE_string(cm_svc_name, "simm-clustermanager-svc", "K8S service name of cluster manager");
DEFINE_string(cm_port_name, "simm-clustermanager-port", "K8S service port name of cluster manager");

// TODO(ytji): maybe we also should use a config file to pass these parameters
DEFINE_uint32(shard_total_num, 16384, "Total number of shards per cluster");
DEFINE_uint32(dataserver_min_num, 3, "minimum number of dataservers in a cluster");
DEFINE_uint32(dataserver_max_num, 1000, "maximum number of dataservers in a cluster");
DEFINE_string(dataserver_namespace, "default", "K8S namespace of dataservers");
DEFINE_string(dataserver_svc_name, "simm-data-svc", "K8S service name of dataservers");
DEFINE_string(dataserver_port_name, "simm-dataserver-port", "K8S service port name of dataservers");

// common flags used in RPC requests
DEFINE_uint32(dataserver_heartbeat_interval_inSecs,
              5,
              "Interval in seconds for dataserver to send heartbeat RPC requests to cluster manager");
DEFINE_uint32(rpc_timeout_inSecs, 1, "Common RPC timeout in seconds");

// common flags for k8s
DEFINE_string(k8s_api_server, "https://kubernetes.default.svc", "K8S API Server url");
DEFINE_string(k8s_service_account, "/var/run/secrets/kubernetes.io/serviceaccount", "K8S service account uri");

// common flags for hash
DEFINE_uint32(hash_seed, 0, "hash seed for calculate hash value of shard_id etc");

DEFINE_uint32(metrics_port, 9464, "Port number for Prometheus metrics exposition");