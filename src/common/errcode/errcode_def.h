#pragma once

#include <cstdint>
#include <string_view>

// number range [-32768,32767], it's enough to define negative error codes
using error_code_t = int16_t;

#define DEFINE_COMMON_ERRCODE(name, value, msg) \
  namespace CommonErr {                         \
  inline constexpr error_code_t name = value;   \
  }

// module name
//   Cm   - ClusterManager, namespace CmErr
//   Ds   - DataServer    , namespace DsErr
//   Clnt - Client        , namespace ClntErr
//   more......
#define DEFINE_MODULE_ERRCODE(module, name, value, msg) \
  namespace module##Err {                               \
    inline constexpr error_code_t name = value;         \
  }

#define ERR_STR(error_code) ([]() -> std::string_view { return #error_code; }())

//////////////////////////////////////////////////////////
// Common error codes definition
// [-1000, -2000]
//////////////////////////////////////////////////////////
DEFINE_COMMON_ERRCODE(OK, 0, "success")
DEFINE_COMMON_ERRCODE(InvalidArgument, -1000, "invalid argument")
DEFINE_COMMON_ERRCODE(NotImplemented, -1001, "the operation is not implemented")
DEFINE_COMMON_ERRCODE(Timeout, -1002, "operation timeout")
DEFINE_COMMON_ERRCODE(OutOfMemory, -1003, "out of memory")
DEFINE_COMMON_ERRCODE(InvalidState, -1004, "invalid state")
// RPC related error codes
DEFINE_COMMON_ERRCODE(CmRegisterNewNodeFailed, -1100, "used in hand-shake rpc response from cm to sds")
DEFINE_COMMON_ERRCODE(CmTargetShardIdNotFound, -1101, "target shard id requested by client not found in routing table")
DEFINE_COMMON_ERRCODE(CmRoutingInfoNotComplete, -1102, "result of all routing info query is not complete")
// GFlags related error codes
DEFINE_COMMON_ERRCODE(GFlagNotFound, -1200, "Gflag name want to get/set not found in target node")
DEFINE_COMMON_ERRCODE(GFlagSetFailed, -1201, "Failed to set Gflag value to target value")
// K8S related error codes
DEFINE_COMMON_ERRCODE(GetPodIpFromK8SFailed, -1300, "Get pod IP from K8S failed")
// Other error codes (admin, internal, uknown, etc.)
DEFINE_COMMON_ERRCODE(TargetNotFound, -1900, "Target resource not found")
DEFINE_COMMON_ERRCODE(TargetUnavailable, -1901, "Target resource unavailable")
DEFINE_COMMON_ERRCODE(InternalError, -1999, "internal error")
DEFINE_COMMON_ERRCODE(UnknownError, -2000, "unknown error")

//////////////////////////////////////////////////////////
// Client SDK internal codes definition
// [-2001, -3000]
//////////////////////////////////////////////////////////
DEFINE_MODULE_ERRCODE(Clnt, GetRoutingTableTimeout, -2000, "get routing table from Cluster Manager timeout")
DEFINE_MODULE_ERRCODE(Clnt, GetRoutingTableFailed, -2001, "get routing table from Cluster Manager failed")
DEFINE_MODULE_ERRCODE(Clnt, ClntLookupShardFailed, -2002, "look up data server for shard failed")
// IO related error codes
DEFINE_MODULE_ERRCODE(Clnt, ClntPutObjectFailed, -2100, "put object into data server failed")
DEFINE_MODULE_ERRCODE(Clnt, ClntGetObjectFailed, -2101, "get object from data server failed")
DEFINE_MODULE_ERRCODE(Clnt, ClntDelObjectFailed, -2102, "delete object from data server failed")
DEFINE_MODULE_ERRCODE(Clnt, ClntLookupObjectFailed, -2103, "lookup object from data server failed")
// Mempool related error codes
DEFINE_MODULE_ERRCODE(Clnt, ClntMempoolInitFailed, -2200, "client mempool init failed")
DEFINE_MODULE_ERRCODE(Clnt, ClntMempoolAllocateFailed, -2201, "client mempool allocate failed")
DEFINE_MODULE_ERRCODE(Clnt, ClntMempoolReleaseFailed, -2202, "client mempool release failed")
DEFINE_MODULE_ERRCODE(Clnt, ClntMempoolRegisterMrFailed, -2203, "client mempool register mr failed")
DEFINE_MODULE_ERRCODE(Clnt, ClntMempoolDeregisterMrFailed, -2204, "client mempool deregister mr failed")
// Network related error codes
DEFINE_MODULE_ERRCODE(Clnt, ClntSendRPCFailed, -2300, "send rpc request failed")
DEFINE_MODULE_ERRCODE(Clnt, BuildConnectionFailed, -2301, "build connection with server failed")
DEFINE_MODULE_ERRCODE(Clnt, AddNewChannelsFailed, -2302, "Add more channels(QP) to connection failed")

//////////////////////////////////////////////////////////
// Cluster Manager internal error codes definition
// [-3001, -4000]
//////////////////////////////////////////////////////////
DEFINE_MODULE_ERRCODE(Cm, InitFailed, -3000, "")
DEFINE_MODULE_ERRCODE(Cm, ClusterManagerAlreadyStarted, -3001, "")
DEFINE_MODULE_ERRCODE(Cm, ClusterManagerAlreadyStopped, -3002, "")
DEFINE_MODULE_ERRCODE(Cm, InitIntraRPCServiceFailed, -3003, "")
DEFINE_MODULE_ERRCODE(Cm, InitInterRPCServiceFailed, -3004, "")
DEFINE_MODULE_ERRCODE(Cm, InitAdminRPCServiceFailed, -3005, "")
DEFINE_MODULE_ERRCODE(Cm, InitShardManagerFailed, -3006, "")
DEFINE_MODULE_ERRCODE(Cm, InitNodeManagerFailed, -3007, "")
DEFINE_MODULE_ERRCODE(Cm, InitShardPlacementSchedulerFailed, -3008, "")
DEFINE_MODULE_ERRCODE(Cm, InitHBMonitorFailed, -3009, "")
DEFINE_MODULE_ERRCODE(Cm, InitResourceMonitorFailed, -3010, "")
DEFINE_MODULE_ERRCODE(Cm, InitDataSyncerFailed, -3011, "")
DEFINE_MODULE_ERRCODE(Cm,
                      InitInGracePeriod,
                      -3012,
                      "cluster manager still in grace period"
                      "to wait for new dataservers to join into cluster, the simm service is not ready")

DEFINE_MODULE_ERRCODE(Cm, InitDataserverNodesTooFew, -3100, "cluster manager init with too few dataserver nodes")
DEFINE_MODULE_ERRCODE(Cm, InitDataserverNodesTooMany, -3101, "cluster manager init with too many dataserver nodes")

DEFINE_MODULE_ERRCODE(Cm, InvalidShardId, -3200, "shard id should be in range [0, FLAGS_shard_total_num)")
DEFINE_MODULE_ERRCODE(Cm, NoAvailableDataservers, -3201, "no available dataservers to assign shard")
DEFINE_MODULE_ERRCODE(Cm, InsufficientDataservers, -3202, "insufficient dataservers to rebalance shards")

// Node Manager related error codes
DEFINE_MODULE_ERRCODE(Cm, NodeManagerAddNodeFailed, -3300, "add node in Node Manager failed")
DEFINE_MODULE_ERRCODE(Cm, NodeManagerGetNodeResourceFailed, -3301, "get node resource info in node manager failed")

//////////////////////////////////////////////////////////
// Data Server error internal codes definition
// [-4001, -5000]
/////////////////////////////////////////////////////////
DEFINE_MODULE_ERRCODE(Ds, InitFailed, -4000, "")
DEFINE_MODULE_ERRCODE(Ds, InitRPCServiceFailed, -4001, "")
DEFINE_MODULE_ERRCODE(Ds, RegisterRPCHandlerFailed, -4002, "")
DEFINE_MODULE_ERRCODE(Ds, KeyNotFound, -4003, "")
DEFINE_MODULE_ERRCODE(Ds, KVStatusNotValid, -4004, "")
DEFINE_MODULE_ERRCODE(Ds, DataAlreadyExists, -4005, "")
DEFINE_MODULE_ERRCODE(Ds, CachePoolAllocateFailed, -4006, "")
DEFINE_MODULE_ERRCODE(Ds, KeyLengthExceedLimit, -4007, "")
DEFINE_MODULE_ERRCODE(Ds, DataRDMATransportFailed, -4008, "")
DEFINE_MODULE_ERRCODE(Ds, HashTableOperationError, -4009, "")
DEFINE_MODULE_ERRCODE(Ds, NoEntriesToEvict, -4010, "no entries available for eviction")
DEFINE_MODULE_ERRCODE(Ds, ConcurrentDataRace, -4011, "unexpected data race")
