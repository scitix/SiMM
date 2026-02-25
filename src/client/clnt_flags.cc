#include <gflags/gflags.h>

DEFINE_bool(clnt_use_k8s, true, "simm client in K8S env");
DEFINE_uint32(clnt_thread_pool_size, 10, "simm client thread pool size");
DEFINE_uint32(clnt_cm_addr_check_interval_inSecs, 10,
	"simm client backgroud thread(check cm address update) trigger interval in seconds, default is 10s");
DEFINE_int32(clnt_sync_req_timeout_ms, 1000, "simm client sync request timeout in milliseconds, default is 1s");
DEFINE_int32(clnt_async_req_timeout_ms, 3000, "simm client sync request timeout in milliseconds, default is 3s");
DEFINE_string(clnt_log_file, "/var/log/simm/simm_clnt.log", "simm client log file path & name");
