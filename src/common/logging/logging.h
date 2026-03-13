#pragma once
#include <sys/select.h>
#include <sys/time.h>
#include <unistd.h>

#include <atomic>
#include <cassert>
#include <cstdarg>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>

#include <folly/Chrono.h>
#include <folly/FileUtil.h>
#include <folly/Synchronized.h>
#include <folly/logging/AsyncFileWriter.h>
#include <folly/logging/LogHandler.h>
#include <folly/logging/LogHandlerFactory.h>
#include <folly/logging/LogMessage.h>
#include <folly/logging/Logger.h>
#include <folly/logging/LoggerDB.h>
#include <folly/logging/xlog.h>

namespace simm {
namespace logging {
struct LogConfig {
  std::string log_path;
  std::string log_level_ = "INFO";
  size_t max_rotate_size_ = 1UL << 25;  // 32 MB
  uint32_t max_log_num_ = 100;
  size_t async_buffer_size_ = 1UL << 20;  // 1 MB
};

class RotateLogHandler : public folly::LogHandler {
 public:
  explicit RotateLogHandler(std::string log_path, size_t max_size, uint32_t max_files, size_t async_buffer_size);
  ~RotateLogHandler() override;

  void handleMessage(const folly::LogMessage &message, const folly::LogCategory *handlerCategory) override;
  void flush() override;
  void requestRotation();

  folly::LogHandlerConfig getConfig() const override;

 private:
  std::string getNewFileName();
  void rotate();
  void deleteOldFiles();
  void backgroundRotate();
  std::shared_ptr<folly::AsyncFileWriter> createWriter(const std::string &filename);
  void updateSymlinkAtomic(const std::string &target_file);

 private:
  std::string log_path_;
  size_t max_size_{0};
  uint32_t max_log_num_{0};

  std::ofstream file_;

  std::shared_ptr<folly::AsyncFileWriter> async_writer_;
  size_t async_buffer_size_{0};
  std::atomic<size_t> current_size_{0};
  std::shared_mutex file_mutex_;

  std::atomic<bool> need_rotate_{false};
  std::atomic<bool> stop_background_{false};
  std::mutex rotate_mutex_;
  std::condition_variable rotation_cv_;
  std::thread rotate_thread_;
};

class RotateHandlerFactory : public folly::LogHandlerFactory {
 public:
  explicit RotateHandlerFactory() {}

  std::shared_ptr<folly::LogHandler> createHandler(const folly::LogHandlerFactory::Options &options) override;

  folly::StringPiece getType() const override { return "rotate_file"; }
};

class LoggerManager {
 public:
  static LoggerManager &Instance();

  folly::Logger getLogger(const std::string &module);

  void UpdateConfig(std::string module, LogConfig &config);

 private:
  LoggerManager();

  void updateCategory(folly::LogCategory *category, const LogConfig *conf);

 private:
  folly::Synchronized<std::unordered_map<std::string, LogConfig>> config_map_;
  folly::LoggerDB &db_{folly::LoggerDB::get()};

  std::mutex logger_mutex_;
  std::atomic<bool> need_update{false};
};

class DefaultLogger {
 public:
  static folly::Logger &get() {
    static folly::Logger logger = simm::logging::LoggerManager::Instance().getLogger("default");
    return logger;
  }
};

std::string vsnprintf_args(const char *fmt, ...);

}  // namespace logging
}  // namespace simm

// --- log macros ---
#define _MLOG_COMMON0(logger, level, ...) FB_LOG(logger, level, ##__VA_ARGS__)

#define _MLOG_COMMON1(logger, level, fmt, arg1, ...) FB_LOGF(logger, level, fmt, arg1, ##__VA_ARGS__)

// support macro, max 10 params（logger, level, fmt, arg1, ..., arg7）
#define _MLOG_GET_MACRO(_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, NAME, ...) NAME

// commn log marco
#define _MLOG_COMMON(...)        \
  _MLOG_GET_MACRO(__VA_ARGS__,   \
                  _MLOG_COMMON1, \
                  _MLOG_COMMON1, \
                  _MLOG_COMMON1, \
                  _MLOG_COMMON1, \
                  _MLOG_COMMON1, \
                  _MLOG_COMMON1, \
                  _MLOG_COMMON1, \
                  _MLOG_COMMON0, \
                  _MLOG_COMMON0, \
                  _MLOG_COMMON0) \
  (__VA_ARGS__)

#define DEFAULT_LOG(level, ...) _MLOG_COMMON(simm::logging::DefaultLogger::get(), level, ##__VA_ARGS__)

/**
 * @brief print log to file and stdout/stderr according to default config.
 * The default log file path is set by env 'SIMM_LOG_PATH', default log level is
 * set by env 'SIMM_LOG_LEVEL'.
 * Example:
 *     LOG_INFO("The default log file is {}", default_log_file);
 **/
#define LOG_DEBUG(...) DEFAULT_LOG(DBG, ##__VA_ARGS__);
#define LOG_INFO(...) DEFAULT_LOG(INFO, ##__VA_ARGS__);
#define LOG_WARN(...) DEFAULT_LOG(WARN, ##__VA_ARGS__);
#define LOG_ERROR(...) DEFAULT_LOG(ERR, ##__VA_ARGS__);
#define LOG_CRITICAL(...) DEFAULT_LOG(CRITICAL, ##__VA_ARGS__);

/**
 * @brief print log to file and stdout/stderr according to module config.
 * Example:
 *     DECLARE_LOG_MODULE(module);
 *     simm::logging::LogConfig module_config;
 *     simm::logging::LoggerManager::Instance().UpdateConfig(module,
 *module_config); If not UpdateConfig(), the module config is the same as
 *default config.
 **/
#define DECLARE_LOG_MODULE(module)                                                                         \
  [[maybe_unused]] static auto &getModuleLogger() {                                                        \
    static thread_local folly::Logger logger = simm::logging::LoggerManager::Instance().getLogger(module); \
    return logger;                                                                                         \
  }

#define MLOG_DEBUG(...) _MLOG_COMMON(getModuleLogger(), DBG, ##__VA_ARGS__);
#define MLOG_INFO(...) _MLOG_COMMON(getModuleLogger(), INFO, ##__VA_ARGS__);
#define MLOG_WARN(...) _MLOG_COMMON(getModuleLogger(), WARN, ##__VA_ARGS__);
#define MLOG_ERROR(...) _MLOG_COMMON(getModuleLogger(), ERR, ##__VA_ARGS__);
#define MLOG_CRITICAL(...) _MLOG_COMMON(getModuleLogger(), CRITICAL, ##__VA_ARGS__);
