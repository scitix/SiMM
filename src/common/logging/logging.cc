
#include "common/logging/logging.h"

#include <pwd.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>

#include <gflags/gflags.h>

#include <folly/Format.h>
#include <folly/String.h>
#include <folly/executors/GlobalExecutor.h>
#include <folly/logging/AsyncFileWriter.h>
#include <folly/logging/FileHandlerFactory.h>
#include <folly/logging/Init.h>
#include <folly/logging/LogConfig.h>
#include <folly/logging/LogFormatter.h>
#include <folly/logging/LogHandler.h>
#include <folly/logging/LogStreamProcessor.h>
#include <folly/logging/Logger.h>
#include <folly/logging/LoggerDB.h>
#include <folly/logging/StandardLogHandler.h>
#include <folly/logging/xlog.h>

#include "common/utils/time_util.h"
#include "folly/Random.h"

DECLARE_string(default_logging_file);
DECLARE_string(default_logging_level);

namespace simm {
namespace logging {
RotateLogHandler::RotateLogHandler(std::string log_path, size_t max_size, uint32_t max_files, size_t async_buffer_size)
    : log_path_(log_path), max_size_(max_size), max_log_num_(max_files), async_buffer_size_(async_buffer_size) {
  // create the director of log file
  std::filesystem::path file_path(log_path);
  std::filesystem::create_directories(file_path.parent_path());

  std::string log_file = getNewFileName();
  async_writer_ = createWriter(log_file);
  updateSymlinkAtomic(log_file);

  // start backgroup rotate thread
  rotate_thread_ = std::thread(&RotateLogHandler::backgroundRotate, this);
}

RotateLogHandler::~RotateLogHandler() {
  stop_background_ = true;
  rotation_cv_.notify_all();
  if (rotate_thread_.joinable())
    rotate_thread_.join();

  if (async_writer_) {
    async_writer_->flush();
    async_writer_.reset();
  }
}

void RotateLogHandler::handleMessage(const folly::LogMessage &message,
                                     const folly::LogCategory * /* handlerCategory */) {
  std::string timestamp = simm::utils::TimestampToString(message.getTimestamp());
  std::string thread_id = folly::to<std::string>(message.getThreadID());
  std::string file_name = folly::to<std::string>(message.getFileBaseName());
  std::string line_num = folly::to<std::string>(message.getLineNumber());
  std::string logline = folly::sformat("[{}][{}][{}][{}:{}]: {}\n",
                                       timestamp,
                                       folly::logLevelToString(message.getLevel()),
                                       thread_id,
                                       file_name,
                                       line_num,
                                       message.getMessage());

  std::shared_ptr<folly::AsyncFileWriter> writer;
  {
    std::shared_lock<std::shared_mutex> lock(file_mutex_);
    writer = async_writer_;
  }

  if (writer) {
    writer->writeMessage(logline);
    size_t new_size = current_size_.fetch_add(logline.size(), std::memory_order_relaxed) + logline.size();
    if (new_size >= max_size_) {
      requestRotation();
    }
  }
}

void RotateLogHandler::flush() {
  std::shared_ptr<folly::AsyncFileWriter> writer;
  {
    std::lock_guard<std::shared_mutex> lock(file_mutex_);
    writer = async_writer_;
  }
  if (writer) {
    writer->flush();
  }
}

void RotateLogHandler::requestRotation() {
  need_rotate_.store(true, std::memory_order_release);
  rotation_cv_.notify_one();
}

folly::LogHandlerConfig RotateLogHandler::getConfig() const {
  folly::LogHandlerConfig log_config;
  log_config.type = "rotate";
  log_config.options["path"] = log_path_;
  log_config.options["max_size"] = folly::to<std::string>(max_size_);
  log_config.options["max_files"] = folly::to<std::string>(max_log_num_);
  return log_config;
}

std::string RotateLogHandler::getNewFileName() {
  std::string cur_timestamp = simm::utils::GetTimestampString();
  return log_path_ + "." + cur_timestamp;
}

std::shared_ptr<folly::AsyncFileWriter> RotateLogHandler::createWriter(const std::string &filename) {
  auto writer = std::make_shared<folly::AsyncFileWriter>(filename);
  writer->setMaxBufferSize(async_buffer_size_);
  return writer;
}

void RotateLogHandler::rotate() {
  std::string new_filename = getNewFileName();
  auto new_writer = createWriter(new_filename);

  std::shared_ptr<folly::AsyncFileWriter> old_writer;
  {
    std::lock_guard<std::shared_mutex> lock(file_mutex_);
    old_writer = std::move(async_writer_);
    async_writer_ = new_writer;
    current_size_.store(0, std::memory_order_relaxed);
  }

  if (old_writer) {
    folly::getGlobalIOExecutor()->add([old = std::move(old_writer)]() { old->flush(); });
  }

  updateSymlinkAtomic(new_filename);
  deleteOldFiles();
}

void RotateLogHandler::backgroundRotate() {
  while (!stop_background_) {
    std::unique_lock<std::mutex> lock(rotate_mutex_);
    rotation_cv_.wait(lock, [this]() { return need_rotate_.load(std::memory_order_acquire) || stop_background_; });

    if (stop_background_) {
      break;
    }

    rotate();

    need_rotate_.store(false, std::memory_order_release);
  }
}

void RotateLogHandler::deleteOldFiles() {
  auto file_obj = std::filesystem::path(log_path_);
  auto dir = file_obj.parent_path();
  std::string origin_file = file_obj.filename().string();

  std::vector<std::filesystem::path> files;

  // collect all log files
  for (const auto &entry : std::filesystem::directory_iterator(dir)) {
    auto path = entry.path();
    std::string filenameStr = path.filename().string();

    // push log file
    if (filenameStr.find(origin_file) == 0) {
      files.push_back(path);
    }
  }

  // order by modify time (old first)
  std::sort(files.begin(), files.end(), [](const auto &a, const auto &b) {
    return std::filesystem::last_write_time(a) < std::filesystem::last_write_time(b);
  });

  // delete exceed num files
  if (files.size() > max_log_num_) {
    std::lock_guard<std::shared_mutex> lock(file_mutex_);
    for (size_t i = 0; i < files.size() - max_log_num_; i++) {
      std::filesystem::remove(files[i]);
    }
  }
}

void RotateLogHandler::updateSymlinkAtomic(const std::string &target_file) {
  const uint32_t random_suffix = folly::Random::secureRand32();
  // get tmp symlink name
  std::string symlink_name_ = log_path_ + ".latest";
  std::string tmp_symlink = fmt::format("{}.{}.tmp", symlink_name_, random_suffix);

  // create symlink with target file and rename tmp link
  file_mutex_.lock();
  std::filesystem::create_symlink(std::filesystem::absolute(target_file), tmp_symlink);
  std::filesystem::rename(tmp_symlink, symlink_name_);
  file_mutex_.unlock();
}

std::shared_ptr<folly::LogHandler> RotateHandlerFactory::createHandler(
    const folly::LogHandlerFactory::Options &options) {
  auto it = options.find("path");
  if (it == options.end()) {
    throw std::runtime_error("RotateHandlerFactory: missing 'path' option");
  }
  std::string path = it->second;

  size_t max_rotate_size = 1UL << 28;
  it = options.find("max_size");
  if (it != options.end()) {
    try {
      max_rotate_size = folly::to<size_t>(it->second);
    } catch (const std::exception &e) {
      throw std::runtime_error(fmt::format("Invalid max_size value '{}': {}", it->second, e.what()));
    }
  }

  uint32_t max_files = 10;
  it = options.find("max_files");
  if (it != options.end()) {
    try {
      max_files = folly::to<uint32_t>(it->second);
    } catch (const std::exception &e) {
      throw std::runtime_error(fmt::format("Invalid max_files value '{}': {}", it->second, e.what()));
    }
  }

  size_t async_buffer_size = 1UL << 20;
  it = options.find("async_buffer_size");
  if (it != options.end()) {
    try {
      async_buffer_size = folly::to<size_t>(it->second);
    } catch (const std::exception &e) {
      throw std::runtime_error(fmt::format("Invalid async_buffer_size value '{}': {}", it->second, e.what()));
    }
  }

  return std::make_shared<RotateLogHandler>(std::move(path), max_rotate_size, max_files, async_buffer_size);
}

LoggerManager::LoggerManager() {
  db_.getCategory("")->clearHandlers();
  LogConfig default_config_ = LogConfig{FLAGS_default_logging_file, FLAGS_default_logging_level};
  // register log handler
  db_.registerHandlerFactory(std::make_unique<RotateHandlerFactory>(), true);

  UpdateConfig("default", default_config_);
}

LoggerManager &LoggerManager::Instance() {
  static LoggerManager log_manager;
  return log_manager;
}

folly::Logger LoggerManager::getLogger(const std::string &module) {
  auto configs = config_map_.rlock();
  [[maybe_unused]] const LogConfig *conf = &configs->at("default");
  if (auto it = configs->find(module); it != configs->end()) {
    conf = &it->second;
  }
  auto *category = db_.getCategory(module.c_str());
  return folly::Logger(category);
}

void LoggerManager::updateCategory(folly::LogCategory *category, const LogConfig *conf) {
  if (category->getLevel() != folly::stringToLogLevel(conf->log_level_)) {
    category->setLevel(folly::stringToLogLevel(conf->log_level_));
  }

  logger_mutex_.lock();
  if (need_update.load(std::memory_order_acquire)) {
    category->clearHandlers();
    folly::LogHandlerFactory::Options options;
    options["path"] = conf->log_path;
    options["max_size"] = std::to_string(conf->max_rotate_size_);
    options["max_files"] = std::to_string(conf->max_log_num_);
    RotateHandlerFactory factory;
    std::vector<std::shared_ptr<folly::LogHandler>> handlers;
    auto handler = factory.createHandler(options);
    handlers.push_back(std::move(handler));
    category->replaceHandlers(handlers);
    need_update.store(false, std::memory_order_release);
  }
  logger_mutex_.unlock();
}

void LoggerManager::UpdateConfig(std::string module, LogConfig &config) {
  if (folly::stringToLogLevel(config.log_level_) == folly::LogLevel::MAX_LEVEL) {
    throw std::invalid_argument("Invalid log level: " + config.log_level_);
  }

  auto wlock = config_map_.wlock();
  (*wlock)[module] = config;
  need_update.store(true, std::memory_order_release);

  auto *category = db_.getCategory(module.c_str());
  updateCategory(category, &config);
}

std::string vsnprintf_args(const char *fmt, ...) {
  va_list va;
  va_start(va, fmt);
  char buffer[1024];
  vsnprintf(buffer, 1024, fmt, va);
  va_end(va);
  std::string str = buffer;
  return str;
}

}  // namespace logging
}  // namespace simm
