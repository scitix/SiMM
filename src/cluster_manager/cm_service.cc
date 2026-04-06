#include <exception>
#include <string>
#include <string_view>

#include <gflags/gflags.h>

#include "cm_rpc_handler.h"
#include "cm_service.h"
#include "common/base/common_types.h"
#include "common/logging/logging.h"
#include "common/rpc_handlers/common_rpc_handlers.h"
#include "proto/cm_clnt_rpcs.pb.h"
#include "proto/common.pb.h"
#include "proto/ds_cm_rpcs.pb.h"

DECLARE_int32(cm_rpc_intra_port);
DECLARE_int32(cm_rpc_inter_port);
DECLARE_int32(cm_rpc_admin_port);
DECLARE_string(cm_rpc_intra_name);
DECLARE_string(cm_rpc_inter_name);
DECLARE_string(cm_rpc_admin_name);
DECLARE_uint32(cm_cluster_init_grace_period_inSecs);

DECLARE_LOG_MODULE("cluster_manager");

namespace simm {
namespace cm {
ClusterManagerService::~ClusterManagerService() {
  Stop();
}

error_code_t ClusterManagerService::Init() {
  return CommonErr::OK;
}

error_code_t ClusterManagerService::Start() {
  // cm service should be STOPPPED when current interface is called
  bool expected_start_state = false;
  if (is_running_.compare_exchange_strong(expected_start_state, true)) {
    error_code_t ret = StartRPCServices();
    if (ret != CommonErr::OK) {
      MLOG_ERROR("Failed to Start RPC services, ret:{}", ret);
      is_running_.store(false);  // revert state
      return ret;
    }

    // wait for grace period to allow all new dataservers to join cluster
    std::this_thread::sleep_for(std::chrono::seconds(FLAGS_cm_cluster_init_grace_period_inSecs));

    auto registered_dataservers_vec = node_manager_->GetAllNodeAddress(/* alive = true */);
    for (const auto &ds_entry : registered_dataservers_vec) {
      MLOG_INFO("DS({}:{}) joined into cluster", ds_entry->node_ip_, ds_entry->node_port_);
    }
    ret = shard_manager_->InitShardRoutingTable(registered_dataservers_vec);
    if (ret != CommonErr::OK) {
      MLOG_ERROR("Failed to init global shard routing table, ret:{}", ret);
      StopRPCServices();
      is_running_.store(false);  // revert state
      return ret;
    }

    // after global shard routing table inited, the cluster service is ready
    simm::common::ModuleServiceState::GetInstance().MarkServiceReady();

    // start heartbeat monitor background thread
    hb_monitor_->Start();

    // start dataservers resource query background thread
    // FIXME(ytji): for v0930 version, background thread(to query resource stats from ds) in
    // node manager module is not actived yet, so just comment init action
    // node_manager_->Init();

    MLOG_INFO("ClusterManager service starts successfully!");
  } else {
    // is_running_ is already true
    MLOG_WARN("ClusterManager service already started!");
    return CmErr::ClusterManagerAlreadyStarted;
  }

  return CommonErr::OK;
}

error_code_t ClusterManagerService::Stop() {
  // cm service should be STARTED when current interface is called
  bool expected_start_state = true;
  if (is_running_.compare_exchange_strong(expected_start_state, false)) {
    error_code_t ret = StopRPCServices();
    if (ret != CommonErr::OK) {
      MLOG_ERROR("Failed to Stop RPC services, ret:{}", ret);
      is_running_.store(true);  // revert state
      return ret;
    }

    // stop heartbeat monitor background thread
    hb_monitor_->Stop();

    // shard manager stop?

    // stop node manager background thread
    // FIXME(ytji): for v0930 version, background thread(to query resource stats from ds) in
    // node manager module is not actived yet, so just comment stop action
    // node_manager_->Stop();

    MLOG_INFO("Cluster Manager service stopped successfully...");
  } else {
    // is_running_ is already false
    MLOG_WARN("Cluster Manager service already stopped!");
    return CmErr::ClusterManagerAlreadyStopped;
  }

  return CommonErr::OK;
}

error_code_t ClusterManagerService::StartRPCServices() {
  // start RPC services for intra-cm, in-cluster and admin usages
  auto create_and_start_rpc_fn =
      [](auto &rpc_ptr, int32_t port, const std::string_view &name) -> sicl::transport::Result {
    sicl::rpc::SiRPC *rpc_raw = nullptr;
    auto res = sicl::rpc::SiRPC::newInstance(rpc_raw, true /*is_server*/);
    if (res != sicl::transport::SICL_SUCCESS) {
      MLOG_ERROR("Failed to initialize {} RPC service instance, res:{}", name, std::to_string(res));
      return res;
    }
    rpc_ptr.reset(rpc_raw);
    res = static_cast<sicl::transport::Result>(rpc_ptr->Start(port));
    if (res != sicl::transport::SICL_SUCCESS) {
      MLOG_ERROR("Failed to start {} RPC service on local port {}, res:{}", name, port, std::to_string(res));
    }
    return res;
  };
  if (auto res = create_and_start_rpc_fn(intra_rpc_service_, FLAGS_cm_rpc_intra_port, FLAGS_cm_rpc_intra_name);
      res != sicl::transport::SICL_SUCCESS) {
    return CmErr::InitIntraRPCServiceFailed;
  }
  if (auto res = create_and_start_rpc_fn(inter_rpc_service_, FLAGS_cm_rpc_inter_port, FLAGS_cm_rpc_inter_name);
      res != sicl::transport::SICL_SUCCESS) {
    StopRPCServices();
    return CmErr::InitInterRPCServiceFailed;
  }
  if (auto res = create_and_start_rpc_fn(admin_rpc_service_, FLAGS_cm_rpc_admin_port, FLAGS_cm_rpc_admin_name);
      res != sicl::transport::SICL_SUCCESS) {
    StopRPCServices();
    return CmErr::InitAdminRPCServiceFailed;
  }

  // Register handlers for in-cluster RPC requests
  inter_rpc_service_->RegisterHandler(
      static_cast<sicl::rpc::ReqType>(simm::cm::ClusterManagerRpcType::RPC_NEW_NODE_HANDSHAKE),
      new NewNodeHandshakeHandler(
          inter_rpc_service_.get(), new NewNodeHandShakeRequestPB, node_manager_, shard_manager_));
  inter_rpc_service_->RegisterHandler(
      static_cast<sicl::rpc::ReqType>(simm::cm::ClusterManagerRpcType::RPC_NODE_HEARTBEAT),
      new NodeHeartBeatHandler(inter_rpc_service_.get(), new DataServerHeartBeatRequestPB, hb_monitor_));
  inter_rpc_service_->RegisterHandler(
      static_cast<sicl::rpc::ReqType>(simm::cm::ClusterManagerRpcType::RPC_DATASERVER_RESOURCE_QUERY),
      new DataServerResourceReportHandler(inter_rpc_service_.get(), new DataServerShardMemInfoPB));
  // TODO(ytji): will activate after v0930 version
  // inter_rpc_service_->RegisterHandler(static_cast<sicl::rpc::ReqType>(
  //     simm::cm::ClusterManagerRpcType::RPC_ROUTING_TABLE_UPDATE),
  //     new RoutingTableUpdateHandler(inter_rpc_service_.get(), new
  //     RoutingTableUpdateRequestPB));
  inter_rpc_service_->RegisterHandler(
      static_cast<sicl::rpc::ReqType>(simm::cm::ClusterManagerRpcType::RPC_ROUTING_TABLE_QUERY_SINGLE),
      new RoutingTableQuerySingleHandler(
          inter_rpc_service_.get(), new QueryShardRoutingTableSingleRequestPB, shard_manager_));
  inter_rpc_service_->RegisterHandler(
      static_cast<sicl::rpc::ReqType>(simm::cm::ClusterManagerRpcType::RPC_ROUTING_TABLE_QUERY_BATCH),
      new RoutingTableQueryBatchHandler(
          inter_rpc_service_.get(), new QueryShardRoutingTableBatchRequestPB, shard_manager_));
  inter_rpc_service_->RegisterHandler(
      static_cast<sicl::rpc::ReqType>(simm::cm::ClusterManagerRpcType::RPC_ROUTING_TABLE_QUERY_ALL),
      new RoutingTableQueryAllHandler(
          inter_rpc_service_.get(), new QueryShardRoutingTableAllRequestPB, shard_manager_));

  // Register handlers for admin RPC requests
  admin_rpc_service_->RegisterHandler(
      static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_GET_GFLAG_REQ),
      new simm::common::GetGFlagHandler(admin_rpc_service_.get(), new proto::common::GetGFlagValueRequestPB));
  admin_rpc_service_->RegisterHandler(
      static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_SET_GFLAG_REQ),
      new simm::common::SetGFlagHandler(admin_rpc_service_.get(), new proto::common::SetGFlagValueRequestPB));
  admin_rpc_service_->RegisterHandler(
      static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_LIST_GFLAGS_REQ),
      new simm::common::ListGFlagsHandler(admin_rpc_service_.get(), new proto::common::ListAllGFlagsRequestPB));
  admin_rpc_service_->RegisterHandler(
      static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_LIST_SHARD_REQ),
      new RoutingTableQueryAllHandler(
          admin_rpc_service_.get(), new QueryShardRoutingTableAllRequestPB, shard_manager_));
  admin_rpc_service_->RegisterHandler(
      static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_LIST_NODE_REQ),
      new ListNodesHandler(admin_rpc_service_.get(), new ListNodesRequestPB, node_manager_));
  admin_rpc_service_->RegisterHandler(
      static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_SET_NODE_STATUS_REQ),
      new SetNodeStatusHandler(admin_rpc_service_.get(), new SetNodeStatusRequestPB, node_manager_));

  return CommonErr::OK;
}

error_code_t ClusterManagerService::StopRPCServices() {
  auto destory_rpc_fn = [](auto &rpc_ptr) {
    if (rpc_ptr) {
      // SiRPCImpl dctor will do all cleanup actions
      rpc_ptr.reset();
    }
  };
  destory_rpc_fn(intra_rpc_service_);
  destory_rpc_fn(inter_rpc_service_);
  destory_rpc_fn(admin_rpc_service_);

  return CommonErr::OK;
}

error_code_t ClusterManagerService::RegisterAdminHandlers(
    simm::common::AdminServer* admin_server) {
  if (admin_server == nullptr || !admin_server->isRunning()) {
    MLOG_ERROR("RegisterAdminHandlers: AdminServer is null or not running");
    return CommonErr::InvalidState;
  }

  admin_server->registerHandler(
      simm::common::AdminMsgType::CM_STATUS,
      [this](const std::string& /* payload */) -> std::string {
        proto::common::AdmCmStatusResponsePB resp;
        resp.set_ret_code(CommonErr::OK);
        resp.set_is_running(is_running_.load());
        resp.set_service_ready(
            simm::common::ModuleServiceState::GetInstance().IsServiceReady());

        auto allStatus = node_manager_->GetAllNodeStatus();
        uint32_t aliveCount = 0;
        uint32_t deadCount = 0;
        for (const auto& [addr, status] : allStatus) {
          if (status == simm::common::RUNNING) {
            ++aliveCount;
          } else if (status == simm::common::DEAD) {
            ++deadCount;
          }
        }
        resp.set_alive_node_count(aliveCount);
        resp.set_dead_node_count(deadCount);
        resp.set_total_shard_count(shard_manager_->GetTotalShardNum());

        std::string buf;
        resp.SerializeToString(&buf);
        return buf;
      });

  admin_server->registerHandler(
      simm::common::AdminMsgType::NODE_LIST,
      [this](const std::string& /* payload */) -> std::string {
        ListNodesResponsePB resp;

        if (!simm::common::ModuleServiceState::GetInstance().IsServiceReady()) {
          resp.set_ret_code(CmErr::InitInGracePeriod);
          std::string buf;
          resp.SerializeToString(&buf);
          return buf;
        }

        const auto nodeStatList = node_manager_->GetAllNodeStatus();
        const auto nodeResList = node_manager_->GetAllNodeResource();

        std::unordered_map<std::string,
                           std::shared_ptr<simm::common::NodeResource>> resMap;
        for (const auto& [addrStr, resource] : nodeResList) {
          resMap[addrStr] = resource;
        }

        for (const auto& [addrStr, status] : nodeStatList) {
          auto nodeAddr = simm::common::NodeAddress::ParseFromString(addrStr);
          if (!nodeAddr) {
            continue;
          }
          auto* nodeInfo = resp.add_nodes();
          auto* addrPb = nodeInfo->mutable_node_address();
          addrPb->set_ip(nodeAddr->node_ip_);
          addrPb->set_port(nodeAddr->node_port_);
          nodeInfo->set_node_status(static_cast<int32_t>(status));

          auto resIt = resMap.find(addrStr);
          if (resIt != resMap.end() && resIt->second) {
            auto* resPb = nodeInfo->mutable_resource();
            resPb->set_mem_free_bytes(resIt->second->mem_free_bytes_);
            resPb->set_mem_total_bytes(resIt->second->mem_total_bytes_);
            resPb->set_mem_used_bytes(resIt->second->mem_used_bytes_);
          }
        }

        resp.set_ret_code(CommonErr::OK);
        std::string buf;
        resp.SerializeToString(&buf);
        return buf;
      });

  admin_server->registerHandler(
      simm::common::AdminMsgType::SHARD_LIST,
      [this](const std::string& /* payload */) -> std::string {
        QueryShardRoutingTableAllResponsePB resp;

        if (!simm::common::ModuleServiceState::GetInstance().IsServiceReady()) {
          resp.set_ret_code(CmErr::InitInGracePeriod);
          std::string buf;
          resp.SerializeToString(&buf);
          return buf;
        }

        auto queryRes = shard_manager_->QueryAllShardRoutingInfos();

        // Group shards by data server address
        using NodeAddrPtr = std::shared_ptr<simm::common::NodeAddress>;
        std::unordered_map<NodeAddrPtr, std::vector<shard_id_t>> dsShards;
        for (const auto& [shardId, nodeAddr] : queryRes) {
          if (nodeAddr) {
            dsShards[nodeAddr].push_back(shardId);
          }
        }
        for (const auto& [nodeAddr, shardIds] : dsShards) {
          auto* entry = resp.add_shard_info();
          auto* dsAddr = entry->mutable_data_server_address();
          dsAddr->set_ip(nodeAddr->node_ip_);
          dsAddr->set_port(nodeAddr->node_port_);
          for (auto sid : shardIds) {
            entry->add_shard_ids(sid);
          }
        }

        resp.set_ret_code(CommonErr::OK);
        std::string buf;
        resp.SerializeToString(&buf);
        return buf;
      });

  MLOG_INFO("CM admin handlers registered");
  return CommonErr::OK;
}

}  // namespace cm
}  // namespace simm
