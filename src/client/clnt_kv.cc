#include <chrono>

#include <folly/Likely.h>

#include "simm/simm_common.h"
#include "simm/simm_kv.h"

#include "clnt_mempool.h"
#include "clnt_messenger.h"
#include "common/base/assert.h"
#include "common/base/common_types.h"
#include "common/base/consts.h"
#include "common/errcode/errcode_def.h"
#include "common/logging/logging.h"
#include "common/metrics/metrics.h"

#include "common/context/context.h"

DECLARE_string(clnt_log_file);

DECLARE_LOG_MODULE("simm_client");

namespace simm {
namespace clnt {

KVStore::KVStore() {
#ifdef NDEBUG
  simm::logging::LogConfig clnt_log_config = simm::logging::LogConfig{FLAGS_clnt_log_file, "INFO"};
#else
  simm::logging::LogConfig clnt_log_config = simm::logging::LogConfig{FLAGS_clnt_log_file, "DEBUG"};
#endif
  simm::logging::LoggerManager::Instance().UpdateConfig("simm_client", clnt_log_config);

  auto res = simm::clnt::ClientMessenger::Instance().Init();
  SIMM_ASSERT(res == CommonErr::OK, "Init client messenger failed: {}", res);

  res = simm::clnt::MempoolBase::Instance().Init();
  SIMM_ASSERT(res == CommonErr::OK, "Init client mempool failed: {}", res);
}

Data KVStore::Allocate(size_t block_size) const {
  if (FOLLY_UNLIKELY(block_size > simm::MAX_VALUE_SIZE)) {
    MLOG_WARN("Allocating data block of size {} B, exceeds max value size {} B", block_size, simm::MAX_VALUE_SIZE);
  }
  return Data(block_size);
}

int32_t KVStore::Get(const std::string &key, DataView get_data) {
  if (FOLLY_UNLIKELY(key.size() > simm::MAX_KEY_SIZE)) {
    MLOG_ERROR("Get key {} size {} B exceeds max key size {} B", key, key.size(), simm::MAX_KEY_SIZE);
    return CommonErr::InvalidArgument;
  }

  auto ctx = std::make_shared<simm::common::SimmContext>();

  auto res = simm::clnt::ClientMessenger::Instance().Get(key, get_data.GetMemp(), ctx);
  simm::common::Metrics::Instance("client").ObserveRequestDuration("Get",
      static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - ctx->GetReqStartTs()).count()));
  simm::common::Metrics::Instance("client").IncRequestsTotal("Get");

  if (res < 0) {
    simm::common::Metrics::Instance("client").IncErrorsTotal("Get");
    MLOG_ERROR("Get kv {} failed: {}", key, res);
  }

  return res;
}

int16_t KVStore::Put(const std::string &key, DataView put_data) {
  if (FOLLY_UNLIKELY(key.size() > simm::MAX_KEY_SIZE)) {
    MLOG_ERROR("Put key {} size {} B exceeds max key size {} B", key, key.size(), simm::MAX_KEY_SIZE);
    return CommonErr::InvalidArgument;
  } else if (FOLLY_UNLIKELY(put_data.AsRef().size_bytes() > simm::MAX_VALUE_SIZE)) {
    MLOG_ERROR("Put key {} value size {} B exceeds max value size {} B",
               key,
               put_data.AsRef().size_bytes(),
               simm::MAX_VALUE_SIZE);
    return CommonErr::InvalidArgument;
  }

  auto ctx = std::make_shared<simm::common::SimmContext>();
  SIMM_TRACE_POINT(*ctx, simm::trace::TracePointType::CLIENT_PUT_START);

  auto res = simm::clnt::ClientMessenger::Instance().Put(key, put_data.GetMemp(), ctx);
  simm::common::Metrics::Instance("client").ObserveRequestDuration("Put",
      static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - ctx->GetReqStartTs()).count()));
  simm::common::Metrics::Instance("client").IncRequestsTotal("Put");

  if (res == CommonErr::OK) {
    return res;
  } else if (res == DsErr::DataAlreadyExists) {
    // FIXME(szzhao): remove workaround after it's implemented in data server
    MLOG_WARN("Put kv {} will override existing entry", key);
    res = Delete(key);
    if (res != CommonErr::OK) {
    simm::common::Metrics::Instance("client").IncErrorsTotal("Put");
      MLOG_ERROR("Put kv {} by deleting existing one failed: {}", key, res);
      return res;
    }

    res = simm::clnt::ClientMessenger::Instance().Put(key, put_data.GetMemp(), ctx);
    // NOTE(zxliao): assume same value already put if entry still exist after deleting
    if (res == CommonErr::OK || res == DsErr::DataAlreadyExists) {
      SIMM_TRACE_POINT(*ctx, simm::trace::TracePointType::CLIENT_PUT_END);
      return CommonErr::OK;
    }
    // fallthrough
  }
  simm::common::Metrics::Instance("client").IncErrorsTotal("Put");
  MLOG_ERROR("Put kv {} failed: {}", key, res);
  return res;
}

int16_t KVStore::Exists(const std::string &key) {
  if (FOLLY_UNLIKELY(key.size() > simm::MAX_KEY_SIZE)) {
    MLOG_ERROR("Exists key {} size {} B exceeds max key size {} B", key, key.size(), simm::MAX_KEY_SIZE);
    return CommonErr::InvalidArgument;
  }

  auto ctx = std::make_shared<simm::common::SimmContext>();

  auto res = simm::clnt::ClientMessenger::Instance().Exists(key, ctx);
  simm::common::Metrics::Instance("client").ObserveRequestDuration("Lookup",
      static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - ctx->GetReqStartTs()).count()));
  simm::common::Metrics::Instance("client").IncRequestsTotal("Lookup");

  if (res != CommonErr::OK) {
    simm::common::Metrics::Instance("client").IncErrorsTotal("Lookup");
    MLOG_ERROR("Exists kv {} failed: {}", key, res);
    return res;
  }

  return CommonErr::OK;
}

int16_t KVStore::Delete(const std::string &key) {
  if (FOLLY_UNLIKELY(key.size() > simm::MAX_KEY_SIZE)) {
    MLOG_ERROR("Delete key {} size {} B exceeds max key size {} B", key, key.size(), simm::MAX_KEY_SIZE);
    return CommonErr::InvalidArgument;
  }

  auto ctx = std::make_shared<simm::common::SimmContext>();

  auto res = simm::clnt::ClientMessenger::Instance().Delete(key, ctx);
  simm::common::Metrics::Instance("client").ObserveRequestDuration("Delete",
      static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - ctx->GetReqStartTs()).count()));
  simm::common::Metrics::Instance("client").IncRequestsTotal("Delete");

  if (res != CommonErr::OK) {
    simm::common::Metrics::Instance("client").IncErrorsTotal("Delete");
    MLOG_ERROR("Delete kv {} failed: {}", key, res);
    return res;
  }

  return CommonErr::OK;
}

int16_t KVStore::AsyncGet(const std::string &key, DataView get_data, AsyncCallback callback) {
  auto ctx = std::make_shared<simm::common::SimmContext>();

  if (FOLLY_UNLIKELY(key.size() > simm::MAX_KEY_SIZE)) {
    MLOG_ERROR("AsyncGet key {} size {} B exceeds max key size {} B", key, key.size(), simm::MAX_KEY_SIZE);
    return CommonErr::InvalidArgument;
  }

  auto res = simm::clnt::ClientMessenger::Instance().AsyncGet(
      key, get_data.GetMemp(), [cb = std::move(callback)](int result) {
        simm::common::Metrics::Instance("client").IncRequestsTotal("AsyncGet");
        if (result != CommonErr::OK) {
          simm::common::Metrics::Instance("client").IncErrorsTotal("AsyncGet");
        }
        cb(result);
      }, ctx);
  auto end_time = std::chrono::steady_clock::now();
  simm::common::Metrics::Instance("client").ObserveRequestDuration(
      "AsyncGet", static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(end_time - ctx->GetReqStartTs()).count()));
  simm::common::Metrics::Instance("client").IncRequestsTotal("AsyncGet");
  if (res != CommonErr::OK) {
    simm::common::Metrics::Instance("client").IncErrorsTotal("AsyncGet");
    MLOG_ERROR("AsyncGet kv {} failed: {}", key, res);
    return res;
  }
#ifdef SIMM_APIPERF
  MLOG_INFO(
      "Perf-aget-kv key:{} Lat:{} us", key, std::chrono::duration_cast<std::chrono::microseconds>(end_time - ctx->GetReqStartTs()).count());
#endif

  return CommonErr::OK;
}

int16_t KVStore::AsyncPut(const std::string &key, DataView put_data, AsyncCallback callback) {
  auto ctx = std::make_shared<simm::common::SimmContext>();

  if (FOLLY_UNLIKELY(key.size() > simm::MAX_KEY_SIZE)) {
    MLOG_ERROR("AsyncPut key {} size {} B exceeds max key size {} B", key, key.size(), simm::MAX_KEY_SIZE);
    return CommonErr::InvalidArgument;
  } else if (FOLLY_UNLIKELY(put_data.AsRef().size_bytes() > simm::MAX_VALUE_SIZE)) {
    MLOG_ERROR("AsyncPut key {} value size {} B exceeds max value size {} B",
               key,
               put_data.AsRef().size_bytes(),
               simm::MAX_VALUE_SIZE);
    return CommonErr::InvalidArgument;
  }

  auto res = simm::clnt::ClientMessenger::Instance().AsyncPut(
      key, put_data.GetMemp(), [cb = std::move(callback)](int result) {
        simm::common::Metrics::Instance("client").IncRequestsTotal("AsyncPut");
        if (result != CommonErr::OK) {
          simm::common::Metrics::Instance("client").IncErrorsTotal("AsyncPut");
        }
        cb(result);
      }, ctx);
  auto end_time = std::chrono::steady_clock::now();
  simm::common::Metrics::Instance("client").ObserveRequestDuration(
      "AsyncPut", static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(end_time - ctx->GetReqStartTs()).count()));
  simm::common::Metrics::Instance("client").IncRequestsTotal("AsyncPut");
  if (res != CommonErr::OK) {
    simm::common::Metrics::Instance("client").IncErrorsTotal("AsyncPut");
    MLOG_ERROR("AsyncPut kv {} failed: {}", key, res);
    return res;
  }
#ifdef SIMM_APIPERF
  MLOG_INFO(
      "Perf-aput-kv key:{} Lat:{} us", key, std::chrono::duration_cast<std::chrono::microseconds>(end_time - ctx->GetReqStartTs()).count());
#endif

  return CommonErr::OK;
}

int16_t KVStore::AsyncDelete(const std::string &key, AsyncCallback callback) {
  auto ctx = std::make_shared<simm::common::SimmContext>();

  if (FOLLY_UNLIKELY(key.size() > simm::MAX_KEY_SIZE)) {
    MLOG_ERROR("AsyncDelete key {} size {} B exceeds max key size {} B", key, key.size(), simm::MAX_KEY_SIZE);
    return CommonErr::InvalidArgument;
  }

  auto res = simm::clnt::ClientMessenger::Instance().AsyncDelete(
      key, [cb = std::move(callback)](int result) {
        simm::common::Metrics::Instance("client").IncRequestsTotal("AsyncDelete");
        if (result != CommonErr::OK) {
          simm::common::Metrics::Instance("client").IncErrorsTotal("AsyncDelete");
        }
        cb(result);
      }, ctx);
  simm::common::Metrics::Instance("client").ObserveRequestDuration(
      "AsyncDelete", static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - ctx->GetReqStartTs()).count()));
  simm::common::Metrics::Instance("client").IncRequestsTotal("AsyncDelete");
  if (res != CommonErr::OK) {
    simm::common::Metrics::Instance("client").IncErrorsTotal("AsyncDelete");
    MLOG_ERROR("AsyncDelete kv {} failed: {}", key, res);
    return res;
  }
  return CommonErr::OK;
}

int16_t KVStore::AsyncExists(const std::string &key, AsyncCallback callback) {
  auto ctx = std::make_shared<simm::common::SimmContext>();

  if (FOLLY_UNLIKELY(key.size() > simm::MAX_KEY_SIZE)) {
    MLOG_ERROR("AsyncExists key {} size {} B exceeds max key size {} B", key, key.size(), simm::MAX_KEY_SIZE);
    return CommonErr::InvalidArgument;
  }

  auto res = simm::clnt::ClientMessenger::Instance().AsyncExists(
      key, [cb = std::move(callback)](int result) {
        simm::common::Metrics::Instance("client").IncRequestsTotal("AsyncLookup");
        if (result != CommonErr::OK) {
          simm::common::Metrics::Instance("client").IncErrorsTotal("AsyncLookup");
        }
        cb(result);
      }, ctx);
  simm::common::Metrics::Instance("client").ObserveRequestDuration(
      "AsyncLookup", static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - ctx->GetReqStartTs()).count()));
  simm::common::Metrics::Instance("client").IncRequestsTotal("AsyncLookup");
  if (res != CommonErr::OK) {
    simm::common::Metrics::Instance("client").IncErrorsTotal("AsyncLookup");
    MLOG_ERROR("AsyncExists kv {} failed: {}", key, res);
    return res;
  }

  return CommonErr::OK;
}

std::vector<int16_t> KVStore::MPut(const std::vector<std::string> &keys, std::vector<DataView> datas) {
  auto key_cnt = keys.size();
  auto dv_cnt = datas.size();
  if (key_cnt != dv_cnt) {
    MLOG_ERROR("MPut keys count {} not equal to datas count {}", key_cnt, dv_cnt);
    return std::vector<int16_t>(key_cnt, CommonErr::InvalidArgument);
  }
  for (size_t i = 0; i < key_cnt; ++i) {
    // if any kv with invalid lengths found, return errors directly without writing any datas
    if (FOLLY_UNLIKELY(keys[i].size() > simm::MAX_KEY_SIZE)) {
      MLOG_ERROR("MPut found key {} size {} B exceeds max key size {} B", keys[i], keys[i].size(), simm::MAX_KEY_SIZE);
      return std::vector<int16_t>(key_cnt, CommonErr::InvalidArgument);
    } else if (FOLLY_UNLIKELY(datas[i].AsRef().size_bytes() > simm::MAX_VALUE_SIZE)) {
      MLOG_ERROR("MPut found key {} value size {} B exceeds max value size {} B",
                 keys[i],
                 datas[i].AsRef().size_bytes(),
                 simm::MAX_VALUE_SIZE);
      return std::vector<int16_t>(dv_cnt, CommonErr::InvalidArgument);
    }
  }

  std::vector<std::shared_ptr<simm::common::MemBlock>> metadatas;
  for (size_t i = 0; i < key_cnt; ++i) {
    metadatas.emplace_back(datas[i].GetMemp());
  }
  auto rets = simm::clnt::ClientMessenger::Instance().MultiPut(keys, metadatas);
  for (size_t i = 0; i < key_cnt; ++i) {
    if (rets[i] < 0) {
      MLOG_ERROR("MPut kv {} failed: {}", keys[i], rets[i]);
    }
  }

  return rets;
}

std::vector<int32_t> KVStore::MGet(const std::vector<std::string> &keys, std::vector<DataView> datas) {
#ifdef SIMM_APIPERF
  auto ctx = std::make_shared<simm::common::SimmContext>();
#endif

  auto key_cnt = keys.size();
  auto dv_cnt = datas.size();
  if (key_cnt != dv_cnt) {
    MLOG_ERROR("MGet keys count {} not equal to datas count {}", key_cnt, dv_cnt);
    return std::vector<int32_t>(key_cnt, CommonErr::InvalidArgument);
  }
  for (size_t i = 0; i < key_cnt; ++i) {
    // if any kv with invalid lengths found, return errors directly without getting any datas
    if (FOLLY_UNLIKELY(keys[i].size() > simm::MAX_KEY_SIZE)) {
      MLOG_ERROR("MGet found key {} size {} B exceeds max key size {} B", keys[i], keys[i].size(), simm::MAX_KEY_SIZE);
      return std::vector<int32_t>(key_cnt, CommonErr::InvalidArgument);
    }
  }

  std::vector<std::shared_ptr<simm::common::MemBlock>> metadatas;
  for (size_t i = 0; i < key_cnt; ++i) {
    metadatas.emplace_back(datas[i].GetMemp());
  }
  auto rets = simm::clnt::ClientMessenger::Instance().MultiGet(keys, metadatas);
  for (size_t i = 0; i < key_cnt; ++i) {
    if (rets[i] < 0) {
      MLOG_ERROR("MGet kv {} failed: {}", keys[i], rets[i]);
    }
  }

#ifdef SIMM_APIPERF
  MLOG_INFO("Perf-mget-kv key number:{} Lat:{} us",
            key_cnt,
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - ctx->GetReqStartTs()).count());
#endif

  return rets;
}

std::vector<int16_t> KVStore::MExists(const std::vector<std::string> &keys) {
#ifdef SIMM_APIPERF
  auto ctx = std::make_shared<simm::common::SimmContext>();
#endif

  auto key_cnt = keys.size();
  for (size_t i = 0; i < key_cnt; ++i) {
    // if any kv with invalid lengths found, return errors directly without getting any datas
    if (FOLLY_UNLIKELY(keys[i].size() > simm::MAX_KEY_SIZE)) {
      MLOG_ERROR(
          "MExists found key {} size {} B exceeds max key size {} B", keys[i], keys[i].size(), simm::MAX_KEY_SIZE);
      return std::vector<int16_t>(key_cnt, CommonErr::InvalidArgument);
    }
  }

  auto rets = simm::clnt::ClientMessenger::Instance().MultiExists(keys);
  for (size_t i = 0; i < key_cnt; ++i) {
    if (rets[i] < 0) {
      MLOG_ERROR("MExists kv {} failed: {}", keys[i], rets[i]);
    }
  }

#ifdef SIMM_APIPERF
  MLOG_INFO("Perf-mexists-kv key number:{} Lat:{} us",
            key_cnt,
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - ctx->GetReqStartTs()).count());
#endif

  return rets;
}

}  // namespace clnt
}  // namespace simm
