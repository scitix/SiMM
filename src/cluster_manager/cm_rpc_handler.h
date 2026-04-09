#pragma once

#include "rpc/connection.h"
#include "rpc/rpc.h"
#include "rpc/rpc_context.h"
#include "transport/types.h"

#include "cm_hb_monitor.h"
#include "cm_shard_manager.h"
#include "cm_node_manager.h"

namespace simm {
namespace cm {

enum class ClusterManagerRpcType {
  RPC_NEW_NODE_HANDSHAKE = 0,      // hello msg from new dataserver who wants to join cluster
  RPC_NODE_HEARTBEAT,              // heartbeat msg from existed dataservers
  RPC_DATASERVER_RESOURCE_QUERY,   // cluster manager query dataserver resource usage
  RPC_ROUTING_TABLE_UPDATE,        // routing table update msg from dataserver node
  RPC_ROUTING_TABLE_QUERY_SINGLE,  // single routing table entry request from client
  RPC_ROUTING_TABLE_QUERY_BATCH,   // batch routing table entries request from client
  RPC_ROUTING_TABLE_QUERY_ALL,     // whole routing table get request from client
  //.....
};

class NewNodeHandshakeHandler : public sicl::rpc::HandlerBase {
 public:
  explicit NewNodeHandshakeHandler(
        sicl::rpc::SiRPC* service,
        google::protobuf::Message* request,
        std::shared_ptr<simm::cm::ClusterManagerNodeManager> node_manager,
        std::shared_ptr<simm::cm::ClusterManagerShardManager> shard_manager,
        std::shared_ptr<simm::cm::ClusterManagerHBMonitor> hb_monitor):
        HandlerBase(service, request), node_manager_(node_manager),
        shard_manager_(shard_manager), hb_monitor_(hb_monitor) {}

  virtual void Work(const std::shared_ptr<sicl::rpc::RpcContext> ctx,
                    const std::shared_ptr<sicl::rpc::Connection> conn,
                    const google::protobuf::Message *request) const override;

 private:
  std::shared_ptr<simm::cm::ClusterManagerNodeManager> node_manager_{nullptr};
  std::shared_ptr<simm::cm::ClusterManagerShardManager> shard_manager_{nullptr};
  std::shared_ptr<simm::cm::ClusterManagerHBMonitor> hb_monitor_{nullptr};
};

class NodeHeartBeatHandler : public sicl::rpc::HandlerBase {
 public:
  explicit NodeHeartBeatHandler(
        sicl::rpc::SiRPC *service,
        google::protobuf::Message *request,
        std::shared_ptr<simm::cm::ClusterManagerHBMonitor> hb_monitor):
        HandlerBase(service, request), hb_monitor_(hb_monitor) {}

  virtual void Work(const std::shared_ptr<sicl::rpc::RpcContext> ctx,
                    const std::shared_ptr<sicl::rpc::Connection> conn,
                    const google::protobuf::Message *request) const override;

 private:
  std::shared_ptr<simm::cm::ClusterManagerHBMonitor> hb_monitor_{nullptr};
};

// Won't implement in cm, will implement in ds 
// TODO(ytji): remove later
class DataServerResourceReportHandler : public sicl::rpc::HandlerBase {
 public:
  explicit DataServerResourceReportHandler(
        sicl::rpc::SiRPC *service,
        google::protobuf::Message *request):
        HandlerBase(service, request) {}

  virtual void Work(const std::shared_ptr<sicl::rpc::RpcContext> ctx,
                    const std::shared_ptr<sicl::rpc::Connection> conn,
                    const google::protobuf::Message *request) const override;
};

class RoutingTableUpdateHandler : public sicl::rpc::HandlerBase {
 public:
  explicit RoutingTableUpdateHandler(
        sicl::rpc::SiRPC *service,
        google::protobuf::Message *request):
        HandlerBase(service, request) {}

  virtual void Work(const std::shared_ptr<sicl::rpc::RpcContext> ctx,
                    const std::shared_ptr<sicl::rpc::Connection> conn,
                    const google::protobuf::Message *request) const override;
};

class RoutingTableQuerySingleHandler : public sicl::rpc::HandlerBase {
 public:
  explicit RoutingTableQuerySingleHandler(
        sicl::rpc::SiRPC *service,
        google::protobuf::Message *request,
        std::shared_ptr<simm::cm::ClusterManagerShardManager> shard_manager):
        HandlerBase(service, request), shard_manager_(shard_manager) {}

  virtual void Work(const std::shared_ptr<sicl::rpc::RpcContext> ctx,
                    const std::shared_ptr<sicl::rpc::Connection> conn,
                    const google::protobuf::Message *request) const override;

 private:
  std::shared_ptr<simm::cm::ClusterManagerShardManager> shard_manager_{nullptr};
};

class RoutingTableQueryBatchHandler : public sicl::rpc::HandlerBase {
 public:
  explicit RoutingTableQueryBatchHandler(
        sicl::rpc::SiRPC *service,
        google::protobuf::Message *request,
        std::shared_ptr<simm::cm::ClusterManagerShardManager> shard_manager):
        HandlerBase(service, request), shard_manager_(shard_manager) {}

  virtual void Work(const std::shared_ptr<sicl::rpc::RpcContext> ctx,
                    const std::shared_ptr<sicl::rpc::Connection> conn,
                    const google::protobuf::Message *request) const override;

 private:
  std::shared_ptr<simm::cm::ClusterManagerShardManager> shard_manager_{nullptr};
};

// also used in admin rpc service
class RoutingTableQueryAllHandler : public sicl::rpc::HandlerBase {
 public:
  explicit RoutingTableQueryAllHandler(
        sicl::rpc::SiRPC *service,
        google::protobuf::Message *request,
        std::shared_ptr<simm::cm::ClusterManagerShardManager> shard_manager):
        HandlerBase(service, request), shard_manager_(shard_manager) {}

  virtual void Work(const std::shared_ptr<sicl::rpc::RpcContext> ctx,
                    const std::shared_ptr<sicl::rpc::Connection> conn,
                    const google::protobuf::Message *request) const override;

 private:
  std::shared_ptr<simm::cm::ClusterManagerShardManager> shard_manager_{nullptr};
};

class ListNodesHandler : public sicl::rpc::HandlerBase {
 public:
  explicit ListNodesHandler(
        sicl::rpc::SiRPC *service,
        google::protobuf::Message *request,
        std::shared_ptr<simm::cm::
        ClusterManagerNodeManager> node_manager):
        HandlerBase(service, request), node_manager_(node_manager) {}

  virtual void Work(const std::shared_ptr<sicl::rpc::RpcContext> ctx,
                    const std::shared_ptr<sicl::rpc::Connection> conn,
                    const google::protobuf::Message *request) const override;

 private:
  std::shared_ptr<simm::cm::ClusterManagerNodeManager> node_manager_{nullptr};
};

class SetNodeStatusHandler : public sicl::rpc::HandlerBase {
 public:
  explicit SetNodeStatusHandler(
        sicl::rpc::SiRPC *service,
        google::protobuf::Message *request,
        std::shared_ptr<simm::cm::ClusterManagerNodeManager> node_manager):
        HandlerBase(service, request), node_manager_(node_manager) {}

  virtual void Work(const std::shared_ptr<sicl::rpc::RpcContext> ctx,
                    const std::shared_ptr<sicl::rpc::Connection> conn,
                    const google::protobuf::Message *request) const override;

 private:
  std::shared_ptr<simm::cm::ClusterManagerNodeManager> node_manager_{nullptr};
};
}  // namespace cm
}  // namespace simm
