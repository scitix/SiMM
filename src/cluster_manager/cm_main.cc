#include <execinfo.h>
#include <unistd.h>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>

#include <gflags/gflags.h>
#include <folly/init/Init.h>

#include "common/admin/admin_server.h"
#include "common/base/common_types.h"
#include "common/errcode/errcode_def.h"
#include "common/logging/logging.h"
#include "common/version/version_info.h"

#include "cm_service.h"

DECLARE_bool(buildinfo);
DECLARE_string(product_name);
DECLARE_string(product_version);
DECLARE_string(cm_log_file);
DECLARE_int32(cm_rpc_intra_port);
DECLARE_int32(cm_rpc_inter_port);
DECLARE_int32(cm_rpc_admin_port);
DECLARE_uint32(cm_cluster_init_grace_period_inSecs);

DECLARE_LOG_MODULE("cluster_manager");

static std::atomic<bool> sQuitPorcess{false};

void signalHandler(int signal) {
  switch (signal) {
    case SIGINT:
      MLOG_WARN("SIGINT received. ClusterManager Exit...");
      break;
    case SIGTERM:
      MLOG_WARN("SIGTERM received. ClusterManager Exit...");
      break;
    default:
      MLOG_WARN("Unknwon signal {} received. ClusterManager Exit...", signal);
  }
  sQuitPorcess.store(true);
}

void segfaultHandler([[maybe_unused]] int signal) {
  MLOG_CRITICAL("SIGSEGV received. Trigger codedump and exit...");
  void *array[128];
  size_t size = backtrace(array, 128);
  backtrace_symbols_fd(array, size, STDERR_FILENO);
  abort();
}

int main(int argc, char *argv[]) {
  // init thirdparty modules
  // parse command line options to update global flags from command line
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  // folly module init
  folly::Init init(&argc, &argv);

  // print version information and eixt
  if (FLAGS_buildinfo) {
    simm::common::VersionInfo::printWholeVersionInfo();
    return 0;
  }

  // adjust log level for debug mode
#ifdef NDEBUG
  simm::logging::LogConfig cm_log_config = simm::logging::LogConfig{FLAGS_cm_log_file, "INFO"};
#else
  simm::logging::LogConfig cm_log_config = simm::logging::LogConfig{FLAGS_cm_log_file, "DEBUG"};
#endif
  simm::logging::LoggerManager::Instance().UpdateConfig("cluster_manager", cm_log_config);

  MLOG_INFO("Start to launch ClusterManager main process...");
  MLOG_INFO("Product    : {}", FLAGS_product_name);
  MLOG_INFO("Version    : {}", FLAGS_product_version);
  MLOG_INFO("Intra-Port : {}", FLAGS_cm_rpc_intra_port);
  MLOG_INFO("Inter-Port : {}", FLAGS_cm_rpc_inter_port);
  MLOG_INFO("Admin-Port : {}", FLAGS_cm_rpc_admin_port);

  // TODO(ytji): load configuration from file, e.g. cm_conf.json

  auto admin_server = std::make_unique<simm::common::AdminServer>(simm::common::kCmAdminUdsBasePath);
  if (admin_server == nullptr || !admin_server->isRunning()) {
    MLOG_CRITICAL("Failed to init AdminServer");
    return CmErr::InitFailed;
  }

  // Register signal handlers
  MLOG_INFO("Register signal handers for SIGINT/SIGTERM/SIGSEGV");
  std::signal(SIGINT, signalHandler);
  std::signal(SIGTERM, signalHandler);
  std::signal(SIGSEGV, segfaultHandler);

  // cm grace period, wait for all dataservers to join by hand-shake rpc
  // all rpc requests from simm clients will be rejected, clients should do retry later
  simm::common::ModuleServiceState::GetInstance().SetServiceGracePeriod(FLAGS_cm_cluster_init_grace_period_inSecs);
  MLOG_INFO("Enter into grace period, duration is {} seconds", FLAGS_cm_cluster_init_grace_period_inSecs);

  auto cm_service_ptr = std::make_unique<simm::cm::ClusterManagerService>();
  if (cm_service_ptr == nullptr) {
    MLOG_CRITICAL("Failed to create ClusterManagerService instance");
    return CmErr::InitFailed;
  }

  error_code_t rc = CommonErr::OK;
  // rc = cm_service_ptr->Init();
  // if (rc != CommonErr::OK) {
  //   MLOG_CRITICAL("Failed to init ClusterManagerService, rc:{}", rc);
  //   goto exit;
  // }
  rc = cm_service_ptr->Start();
  if (rc != CommonErr::OK) {
    MLOG_CRITICAL("Failed to start ClusterManager service, rc:{}", rc);
    goto exit;
  }

  rc = cm_service_ptr->RegisterAdminHandlers(admin_server.get());
  if (rc != CommonErr::OK) {
    MLOG_CRITICAL("Failed to register CM admin handlers, rc:{}", rc);
    goto exit;
  }

  MLOG_INFO("ClusterManager main process starts successfully!");

  // Simulate as a long-running process
  while (!sQuitPorcess.load()) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  MLOG_WARN("Signal caught. ClusterManager main process exit...");

exit:
  return rc;
}
