#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <thread>

#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>

#include "common/admin/admin_server.h"
#include "common/errcode/errcode_def.h"
#include "common/logging/logging.h"
#include "common/version/version_info.h"

#include "data_server/kv_rpc_service.h"
#include "rpc/rpc.h"

DECLARE_bool(debug);
DECLARE_bool(buildinfo);
DECLARE_int32(io_service_port);
DECLARE_int32(mgt_service_port);
DECLARE_string(product_name);
DECLARE_string(product_version);
DECLARE_string(ds_log_file);

DECLARE_LOG_MODULE("data_server");

static std::atomic<bool> quitPorcess{false};
static void (*prevSigIntHandler)(int) = nullptr;
static void (*prevSigTermHandler)(int) = nullptr;
static void (*prevSigSegvHandler)(int) = nullptr;

void signalHandler(int signal) {
  // Call previous handler if exists
  switch (signal) {
    case SIGINT:
      if (prevSigIntHandler)
        prevSigIntHandler(signal);
      break;
    case SIGTERM:
      if (prevSigTermHandler)
        prevSigTermHandler(signal);
      break;
    case SIGSEGV:
      if (prevSigSegvHandler)
        prevSigSegvHandler(signal);
      break;
  }

  // Our own handling logic
  switch (signal) {
    case SIGINT:
      MLOG_WARN("SIGINT received. Exiting gracefully...");
      break;
    case SIGTERM:
      MLOG_WARN("SIGTERM received. Exiting gracefully...");
      break;
    case SIGSEGV:
      MLOG_CRITICAL("SIGSEGV received. Trigger codedump and exit...");
      abort();  // trigger coredump
      break;
    default:
      MLOG_WARN("Unknown signal {} received. Exiting gracefully...", signal);
  }
  quitPorcess.store(true);
}

int main(int argc, char *argv[]) {
  // init dependencies
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  folly::Init init(&argc, &argv);

  // print version information and eixt
  if (FLAGS_buildinfo) {
    simm::common::VersionInfo::printWholeVersionInfo();
    return 0;
  }

  // adjust log level for debug mode
#ifdef NDEBUG
  simm::logging::LogConfig ds_log_config = simm::logging::LogConfig{FLAGS_ds_log_file, "INFO"};
#else
  simm::logging::LogConfig ds_log_config = simm::logging::LogConfig{FLAGS_ds_log_file, "DEBUG"};
#endif
  simm::logging::LoggerManager::Instance().UpdateConfig("data_server", ds_log_config);

  MLOG_INFO("Start to launch data server process...");
  MLOG_INFO("Product       : {}", FLAGS_product_name);
  MLOG_INFO("Version       : {}", FLAGS_product_version);
  MLOG_INFO("IO Srv-Port   : {}", FLAGS_io_service_port);
  MLOG_INFO("Mgmt Srv-Port : {}", FLAGS_mgt_service_port);
  MLOG_INFO("Debug-Mode    : {}", FLAGS_debug ? "Enabled" : "Disabled");

  // TODO: load configuration file

  auto admin_server = std::make_unique<simm::common::AdminServer>("/run/simm/simm_ds");
  if (admin_server == nullptr || !admin_server->isRunning()) {
    MLOG_ERROR("Failed to init AdminServer");
    return -1;
  }

  // Register signal handlers and save previous ones
  MLOG_INFO("Register signal handers...");
  prevSigIntHandler = std::signal(SIGINT, signalHandler);
  prevSigTermHandler = std::signal(SIGTERM, signalHandler);
  prevSigSegvHandler = std::signal(SIGSEGV, signalHandler);

  std::unique_ptr<simm::ds::KVRpcService> server = std::make_unique<simm::ds::KVRpcService>();
  error_code_t rc;
  rc = server->Init();
  if (rc != CommonErr::OK) {
    MLOG_ERROR("DataServer Inits failed, rc:{}", rc);
    return -1;
  }

  rc = server->Start();
  if (rc != CommonErr::OK) {
    MLOG_ERROR("DataServer starts failed, rc:{}", rc);
    return -1;
  }

  rc = server->RegisterAdminHandlers(admin_server.get());
  if (rc != CommonErr::OK) {
    MLOG_ERROR("Failed to register DS admin handlers, rc:{}", rc);
    return -1;
  }

  MLOG_INFO("DataServer starts normally ...");

  // Simulate a long-running process
  while (!quitPorcess.load()) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  MLOG_INFO("Signal caught, cleanup done, exiting process ...");

  return 0;
}
