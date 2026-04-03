#pragma once

#include "data_server/kv_rpc_service.h"
#include "rpc/connection.h"
#include "rpc/rpc.h"
#include "rpc/rpc_context.h"
#include "transport/types.h"

#include "data_server/kv_cache_pool.h"
#include "data_server/kv_hash_table.h"

namespace simm {
namespace ds {

enum class KVServerRpcType : uint64_t {
  RPC_CLIENT_KV_GET = 0,
  RPC_CLIENT_KV_PUT,
  RPC_CLIENT_KV_DEL,
  RPC_CLIENT_KV_LOOKUP,
};

class KVRpcService;

class KVGetHandler : public sicl::rpc::HandlerBase {
 public:
  explicit KVGetHandler(KVRpcService *service, google::protobuf::Message *request)
      : HandlerBase(service->GetIOService(), request), service_(service) {}

  virtual void Work(const std::shared_ptr<sicl::rpc::RpcContext> ctx,
                    const std::shared_ptr<sicl::rpc::Connection> conn,
                    const google::protobuf::Message *request) const override;

 private:
  KVRpcService *service_;
};

class KVPutHandler : public sicl::rpc::HandlerBase {
 public:
  explicit KVPutHandler(KVRpcService *service, google::protobuf::Message *request)
      : HandlerBase(service->GetIOService(), request), service_(service) {}

  virtual void Work(const std::shared_ptr<sicl::rpc::RpcContext> ctx,
                    const std::shared_ptr<sicl::rpc::Connection> conn,
                    const google::protobuf::Message *request) const override;

 private:
  KVRpcService *service_;
};

class KVDelHandler : public sicl::rpc::HandlerBase {
 public:
  explicit KVDelHandler(KVRpcService *service, google::protobuf::Message *request)
      : HandlerBase(service->GetIOService(), request), service_(service) {}

  virtual void Work(const std::shared_ptr<sicl::rpc::RpcContext> ctx,
                    const std::shared_ptr<sicl::rpc::Connection> conn,
                    const google::protobuf::Message *request) const override;

 private:
  KVRpcService *service_;
};

class KVLookupHandler : public sicl::rpc::HandlerBase {
 public:
  explicit KVLookupHandler(KVRpcService *service, google::protobuf::Message *request)
      : HandlerBase(service->GetIOService(), request), service_(service) {}

  virtual void Work(const std::shared_ptr<sicl::rpc::RpcContext> ctx,
                    const std::shared_ptr<sicl::rpc::Connection> conn,
                    const google::protobuf::Message *request) const override;

 private:
  KVRpcService *service_;
};

class MgtResourceHandler : public sicl::rpc::HandlerBase {
 public:
  explicit MgtResourceHandler(KVRpcService *service, google::protobuf::Message *request)
      : HandlerBase(service->GetMgtService(), request), service_(service) {}

  virtual void Work(const std::shared_ptr<sicl::rpc::RpcContext> ctx,
                    const std::shared_ptr<sicl::rpc::Connection> conn,
                    const google::protobuf::Message *request) const override;

 private:
  KVRpcService *service_;
};

}  // namespace ds
}  // namespace simm
