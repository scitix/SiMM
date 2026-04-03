#pragma once

#include <memory>

#include "rpc/connection.h"
#include "rpc/rpc.h"
#include "rpc/rpc_context.h"
#include "transport/types.h"

namespace simm {
namespace common {

enum class CommonRpcType {
  RPC_GET_GFLAG_REQ = 0,
  RPC_SET_GFLAG_REQ,
  RPC_LIST_GFLAGS_REQ,
  RPC_LIST_SHARD_REQ,
  RPC_LIST_NODE_REQ,
  RPC_SET_NODE_STATUS_REQ,
  RPC_TRACE_TOGGLE_REQ,

  //.....
};

class GetGFlagHandler : public sicl::rpc::HandlerBase {
 public:
  explicit GetGFlagHandler(
        sicl::rpc::SiRPC* service,
        google::protobuf::Message* request):
        HandlerBase(service, request) {}

  virtual void Work(const std::shared_ptr<sicl::rpc::RpcContext> ctx,
                    const std::shared_ptr<sicl::rpc::Connection> conn,
                    const google::protobuf::Message *request) const override;
};

class SetGFlagHandler : public sicl::rpc::HandlerBase {
 public:
  explicit SetGFlagHandler(
        sicl::rpc::SiRPC* service,
        google::protobuf::Message* request):
        HandlerBase(service, request) {}

  virtual void Work(const std::shared_ptr<sicl::rpc::RpcContext> ctx,
                    const std::shared_ptr<sicl::rpc::Connection> conn,
                    const google::protobuf::Message *request) const override;
};

class ListGFlagsHandler : public sicl::rpc::HandlerBase {
 public:
  explicit ListGFlagsHandler(
        sicl::rpc::SiRPC* service,
        google::protobuf::Message* request):
        HandlerBase(service, request) {}

  virtual void Work(const std::shared_ptr<sicl::rpc::RpcContext> ctx,
                    const std::shared_ptr<sicl::rpc::Connection> conn,
                    const google::protobuf::Message *request) const override;
};


#ifdef SIMM_ENABLE_TRACE
class TraceToggleHandler : public sicl::rpc::HandlerBase {
 public:
  explicit TraceToggleHandler(
        sicl::rpc::SiRPC* service,
        google::protobuf::Message* request):
        HandlerBase(service, request) {}

  virtual void Work(const std::shared_ptr<sicl::rpc::RpcContext> ctx,
                    const std::shared_ptr<sicl::rpc::Connection> conn,
                    const google::protobuf::Message *request) const override;
};
#endif

} // namespace common
} // namespace simm