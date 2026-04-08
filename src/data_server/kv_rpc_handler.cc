#include <chrono>
#include <memory>
#include <string>

#include "data_server/kv_hash_table.h"
#include "transport/types.h"

#include "common/context/context.h"
#include "common/errcode/errcode_def.h"
#include "common/logging/logging.h"
#include "common/metrics/metrics.h"
#include "data_server/kv_cache_pool.h"
#include "data_server/kv_rpc_handler.h"
#include "proto/ds_clnt_rpcs.pb.h"
#include "proto/ds_cm_rpcs.pb.h"

DECLARE_LOG_MODULE("data_server");

namespace simm {
namespace ds {

void KVGetHandler::Work(const std::shared_ptr<sicl::rpc::RpcContext> ctx,
                        const std::shared_ptr<sicl::rpc::Connection> conn,
                        const google::protobuf::Message *request) const {
  auto simm_ctx = std::make_shared<simm::common::SimmContext>();

  auto req = dynamic_cast<const KVGetRequestPB *>(request);
  auto rsp = std::make_shared<KVGetResponsePB>();
  KVEntryIntrusivePtr kv_entry;
  auto ret = service_->KVGet(simm_ctx, req, kv_entry);
  if (ret == CommonErr::OK) {
    auto [meta, data] = KVCachePool::GetBufferPair(&kv_entry->slab_info);
    if (req->buf_len() < meta->value_len) {
      MLOG_ERROR("KVGetHandler::Work client buffer too small for key {}, buf_len:{}, value_len:{}",
                 req->key(),
                 req->buf_len(),
                 meta->value_len);
      service_->KVGetCallback(kv_entry);
      rsp->set_ret_code(CommonErr::InvalidArgument);
      simm::common::Metrics::Instance("data_server")
          .ObserveRequestDuration("Get",
                                  static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(
                                                          std::chrono::steady_clock::now() - simm_ctx->GetReqStartTs())
                                                          .count()));
      simm::common::Metrics::Instance("data_server").IncRequestsTotal("Get");
      simm::common::Metrics::Instance("data_server").IncErrorsTotal("Get");
      conn->SendResponse(*rsp, ctx, [rsp](std::shared_ptr<sicl::rpc::RpcContext> ctx) {
        if (ctx->Failed()) {
          MLOG_ERROR("{} response failed: {}({})", rsp->GetTypeName(), ctx->ErrorText(), ctx->ErrorCode());
        }
      });
      return;
    }
    sicl::transport::RequestParam param{
        .mem_desc = static_cast<sicl::transport::MemDesc *>(kv_entry->slab_info.block_addr->descr)};
    std::vector<uint32_t> rkeys(req->buf_rkey().begin(), req->buf_rkey().end());
    sicl::transport::WriteCallback done = [this, kv_entry, rsp, conn, ctx, meta, simm_ctx](
                                              sicl::transport::Status status) mutable {
      error_code_t ret = CommonErr::OK;
      if (status.isOk()) {
        // return actual value length to client
        rsp->set_val_len(meta->value_len);
        // record bytes written on successful write
        simm::common::Metrics::Instance("data_server").IncWrittenTotal(static_cast<double>(meta->value_len));
      } else {
        MLOG_ERROR("KVGetHandler::Work connection write failed, err_code:{}, err_msg:{}",
                   std::to_string(status.errCode()),
                   status.errMsg());
        ret = DsErr::DataRDMATransportFailed;
        simm::common::Metrics::Instance("data_server").IncErrorsTotal("Get");
      }
      service_->KVGetCallback(kv_entry);
      rsp->set_ret_code(ret);
      simm::common::Metrics::Instance("data_server")
          .ObserveRequestDuration("Get",
                                  static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(
                                                          std::chrono::steady_clock::now() - simm_ctx->GetReqStartTs())
                                                          .count()));
      simm::common::Metrics::Instance("data_server").IncRequestsTotal("Get");
      conn->SendResponse(*rsp, ctx, [rsp](std::shared_ptr<sicl::rpc::RpcContext> ctx) {
        if (ctx->Failed()) {
          MLOG_ERROR("{} response failed: {}({})", rsp->GetTypeName(), ctx->ErrorText(), ctx->ErrorCode());
        }
      });
    };
    auto res = conn->write(data, meta->value_len, req->buf_addr(), rkeys, done, param);
    if (res != sicl::transport::Result::SICL_SUCCESS) {
      done(sicl::transport::Status(res));  // synchronized return
    }
    return;
  }
  rsp->set_ret_code(ret);
  simm::common::Metrics::Instance("data_server")
      .ObserveRequestDuration("Get",
                              static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(
                                                      std::chrono::steady_clock::now() - simm_ctx->GetReqStartTs())
                                                      .count()));
  simm::common::Metrics::Instance("data_server").IncRequestsTotal("Get");
  if (ret != CommonErr::OK) {
    simm::common::Metrics::Instance("data_server").IncErrorsTotal("Get");
  }
  conn->SendResponse(*rsp, ctx, [rsp](std::shared_ptr<sicl::rpc::RpcContext> ctx) {
    if (ctx->Failed()) {
      MLOG_ERROR("{} response failed: {}({})", rsp->GetTypeName(), ctx->ErrorText(), ctx->ErrorCode());
    }
  });
}

void KVPutHandler::Work(const std::shared_ptr<sicl::rpc::RpcContext> ctx,
                        const std::shared_ptr<sicl::rpc::Connection> conn,
                        const google::protobuf::Message *request) const {
  auto simm_ctx = std::make_shared<simm::common::SimmContext>();

  auto req = dynamic_cast<const KVPutRequestPB *>(request);
  auto rsp = std::make_shared<KVPutResponsePB>();
  uint32_t shard_id = req->shard_id();
  KVEntryIntrusivePtr kv_entry;
  auto ret = CommonErr::OK;
  try {
    // KVPUT op will trigger low-layer containers to auto-extend, when system has no
    // enought memory, those containers will throw exceptions and make ds coredump.
    ret = service_->KVPut(simm_ctx, req, kv_entry);
  } catch (std::bad_alloc &e) {
    MLOG_ERROR("KVPutHandler::Work exception caught, std::bad_alloc: {}", e.what());
    ret = CommonErr::OutOfMemory;
  } catch (std::system_error &e) {
    // folly containers will throw std::system_error when ENOMEM
    MLOG_ERROR("KVPutHandler::Work exception caught, std::system_error: {}", e.what());
    ret = CommonErr::OutOfMemory;
  } catch (std::exception &e) {
    MLOG_ERROR("KVPutHandler::Work exception caught: {}", e.what());
    ret = CommonErr::InternalError;
  } catch (...) {
    MLOG_ERROR("KVPutHandler::Work unknown exception");
    ret = CommonErr::UnknownError;
  }
  if (ret == CommonErr::OK) {
    auto [meta, data] = KVCachePool::GetBufferPair(&kv_entry->slab_info);
    sicl::transport::RequestParam param{
        .mem_desc = static_cast<sicl::transport::MemDesc *>(kv_entry->slab_info.block_addr->descr)};
    std::vector<uint32_t> rkeys(req->buf_rkey().begin(), req->buf_rkey().end());
    sicl::transport::ReadCallback done = [this, shard_id, kv_entry, rsp, conn, ctx, simm_ctx, meta](
                                             sicl::transport::Status status) mutable {
      error_code_t ret;
      if (status.isOk()) {
        service_->KVPutSuccessHooks(kv_entry);
        ret = CommonErr::OK;
        // record bytes read when Put succeeded
        simm::common::Metrics::Instance("data_server").IncReadTotal(static_cast<double>(meta->value_len));
      } else {
        service_->KVPutFailedRewind(shard_id, kv_entry);
        MLOG_ERROR("KVPutHandler::Work connection read failed: err_code:{}, err_msg:{}",
                   std::to_string(status.errCode()),
                   status.errMsg());
        ret = DsErr::DataRDMATransportFailed;
        simm::common::Metrics::Instance("data_server").IncErrorsTotal("Put");
      }
      rsp->set_ret_code(ret);
      simm::common::Metrics::Instance("data_server")
          .ObserveRequestDuration("Put",
                                  static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(
                                                          std::chrono::steady_clock::now() - simm_ctx->GetReqStartTs())
                                                          .count()));
      simm::common::Metrics::Instance("data_server").IncRequestsTotal("Put");
      conn->SendResponse(*rsp, ctx, [rsp](std::shared_ptr<sicl::rpc::RpcContext> ctx) {
        if (ctx->Failed()) {
          MLOG_ERROR("{} response failed: {}({})", rsp->GetTypeName(), ctx->ErrorText(), ctx->ErrorCode());
        }
      });
    };
    auto res = conn->read(data, meta->value_len, req->buf_addr(), rkeys, done, param);
    if (res != sicl::transport::Result::SICL_SUCCESS) {
      done(sicl::transport::Status(res));  // synchronized return
    }
    return;
  }
  rsp->set_ret_code(ret);
  simm::common::Metrics::Instance("data_server")
      .ObserveRequestDuration("Put",
                              static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(
                                                      std::chrono::steady_clock::now() - simm_ctx->GetReqStartTs())
                                                      .count()));
  simm::common::Metrics::Instance("data_server").IncRequestsTotal("Put");
  if (ret != CommonErr::OK) {
    simm::common::Metrics::Instance("data_server").IncErrorsTotal("Put");
  }
  conn->SendResponse(*rsp, ctx, [rsp](std::shared_ptr<sicl::rpc::RpcContext> ctx) {
    if (ctx->Failed()) {
      MLOG_ERROR("{} response failed: {}({})", rsp->GetTypeName(), ctx->ErrorText(), ctx->ErrorCode());
    }
  });
}

void KVDelHandler::Work(const std::shared_ptr<sicl::rpc::RpcContext> ctx,
                        const std::shared_ptr<sicl::rpc::Connection> conn,
                        const google::protobuf::Message *request) const {
  auto simm_ctx = std::make_shared<simm::common::SimmContext>();

  auto req = dynamic_cast<const KVDelRequestPB *>(request);
  auto rsp = std::make_shared<KVDelResponsePB>();
  auto ret = service_->KVDel(simm_ctx, req);
  rsp->set_ret_code(ret);
  simm::common::Metrics::Instance("data_server")
      .ObserveRequestDuration("Delete",
                              static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(
                                                      std::chrono::steady_clock::now() - simm_ctx->GetReqStartTs())
                                                      .count()));
  simm::common::Metrics::Instance("data_server").IncRequestsTotal("Delete");
  if (ret != CommonErr::OK) {
    simm::common::Metrics::Instance("data_server").IncErrorsTotal("Delete");
  }
  conn->SendResponse(*rsp, ctx, [rsp](std::shared_ptr<sicl::rpc::RpcContext> ctx) {
    if (ctx->Failed()) {
      MLOG_ERROR("{} response failed: {}({})", rsp->GetTypeName(), ctx->ErrorText(), ctx->ErrorCode());
    }
  });
}

void KVLookupHandler::Work(const std::shared_ptr<sicl::rpc::RpcContext> ctx,
                           const std::shared_ptr<sicl::rpc::Connection> conn,
                           const google::protobuf::Message *request) const {
  auto simm_ctx = std::make_shared<simm::common::SimmContext>();

  auto req = dynamic_cast<const KVLookupRequestPB *>(request);
  auto rsp = std::make_shared<KVLookupResponsePB>();
  auto ret = service_->KVLookup(simm_ctx, req);
  rsp->set_ret_code(ret);
  simm::common::Metrics::Instance("data_server")
      .ObserveRequestDuration("Lookup",
                              static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(
                                                      std::chrono::steady_clock::now() - simm_ctx->GetReqStartTs())
                                                      .count()));
  simm::common::Metrics::Instance("data_server").IncRequestsTotal("Lookup");
  if (ret != CommonErr::OK) {
    simm::common::Metrics::Instance("data_server").IncErrorsTotal("Lookup");
  }
  conn->SendResponse(*rsp, ctx, [rsp](std::shared_ptr<sicl::rpc::RpcContext> ctx) {
    if (ctx->Failed()) {
      MLOG_ERROR("{} response failed: {}({})", rsp->GetTypeName(), ctx->ErrorText(), ctx->ErrorCode());
    }
  });
}

void MgtResourceHandler::Work(const std::shared_ptr<sicl::rpc::RpcContext> ctx,
                              const std::shared_ptr<sicl::rpc::Connection> conn,
                              const google::protobuf::Message *request) const {
  auto req = dynamic_cast<const DataServerResourceRequestPB *>(request);
  auto rsp = std::make_shared<DataServerResourceResponsePB>();
  service_->GetResourceStats(req, rsp.get());
  conn->SendResponse(*rsp, ctx, [rsp](std::shared_ptr<sicl::rpc::RpcContext> ctx) {
    if (ctx->Failed()) {
      MLOG_ERROR("{} response failed: {}({})", rsp->GetTypeName(), ctx->ErrorText(), ctx->ErrorCode());
    }
  });
}

}  // namespace ds
}  // namespace simm
