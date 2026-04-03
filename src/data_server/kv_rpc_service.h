#pragma once

#include <gflags/gflags_declare.h>
#include <atomic>
#include <csignal>
#include <cstdlib>
#include <functional>
#include <memory>
#include <unordered_map>

#if defined(SIMM_UNIT_TEST)
#include <gtest/gtest_prod.h>
#endif

#include "common/base/common_types.h"
#include "common/base/consts.h"
#include "common/context/context.h"
#include "common/errcode/errcode_def.h"
#include "folly/synchronization/Baton.h"
#include "rpc/connection.h"
#include "rpc/rpc.h"
#include "rpc/rpc_context.h"
#include "transport/types.h"

#include "common/admin/admin_server.h"
#include "data_server/kv_cache_evictor.h"
#include "data_server/kv_cache_pool.h"
#include "data_server/kv_hash_table.h"
#include "data_server/kv_object_pool.h"
#include "proto/ds_clnt_rpcs.pb.h"
#include "proto/ds_cm_rpcs.pb.h"

DECLARE_int32(io_service_port);
DECLARE_int32(mgt_service_port);
DECLARE_int32(ds_rpc_admin_port);
DECLARE_int64(ds_hash_seed);
DECLARE_uint32(shard_total_num);
DECLARE_int32(register_cooldown_sec);
DECLARE_int32(heartbeat_cooldown_sec);
DECLARE_string(cm_primary_node_ip);
DECLARE_int32(cm_rpc_inter_port);
DECLARE_uint32(busy_wait_timeout_us);

namespace simm {
namespace ds {

class KVRpcService {
 public:
  KVRpcService();
  virtual ~KVRpcService();

  error_code_t Init();
  error_code_t Start();
  error_code_t Stop();

  sicl::rpc::SiRPC *GetIOService() { return io_service_.get(); }
  sicl::rpc::SiRPC *GetMgtService() { return mgt_service_.get(); }
  KVHashTable *GetHashTable(shard_id_t shard_id) { return all_tables_[shard_id]; }

  // KV Get Handler invokes
  error_code_t KVGet(std::shared_ptr<simm::common::SimmContext> ctx,
                     const KVGetRequestPB *req,
                     KVEntryIntrusivePtr &entry);
  void KVGetCallback(KVEntryIntrusivePtr &entry);

  // KV Put Handler invokes
  error_code_t KVPut(std::shared_ptr<simm::common::SimmContext> ctx,
                     const KVPutRequestPB *req,
                     KVEntryIntrusivePtr &entry);
  void KVPutFailedRewind(uint32_t shard_id, KVEntryIntrusivePtr &entry);
  void KVPutSuccessHooks(KVEntryIntrusivePtr &entry);

  // KV Del Handler invokes
  error_code_t KVDel(std::shared_ptr<simm::common::SimmContext> ctx, const KVDelRequestPB *req);

  // KV Lookup Handler invokes
  error_code_t KVLookup(std::shared_ptr<simm::common::SimmContext> ctx, const KVLookupRequestPB *req);

  // Get resource stats info
  void GetResourceStats(const DataServerResourceRequestPB *req, DataServerResourceResponsePB *rsp);

 private:
  error_code_t StartRPCServices();
  error_code_t StopRPCServices();
  error_code_t RegisterHandlers();

  void SetCMAddressFromK8S();
  void KeepAlive();
  void RegisterOnCluster();
  void RegisterToRestartedManager();
  void HeartBeatToCluster();
  void OnHeartbeatResult(bool rpc_failed, error_code_t ret_code);
  void HandleClusterManagerDisconnect();

 private:
  std::unique_ptr<sicl::rpc::SiRPC> io_service_{nullptr};         // for client io requests
  std::unique_ptr<sicl::rpc::SiRPC> mgmt_client_{nullptr};        // for service controls
  std::unique_ptr<sicl::rpc::SiRPC> mgt_service_{nullptr};        // for service controls
  std::unique_ptr<sicl::rpc::SiRPC> admin_rpc_service_{nullptr};  // for maintainence scenario
  std::unique_ptr<simm::common::AdminServer> admin_server_{nullptr};  // UDS admin server

  std::unique_ptr<KVObjectPool> object_pool_{nullptr};
  std::unique_ptr<KVCachePool> cache_pool_{nullptr};
  std::unique_ptr<KVCacheEvictor> cache_evictor_{nullptr};
  std::mutex all_table_mtx_;
  std::unordered_map<shard_id_t, KVHashTable *> all_tables_;

  std::atomic<bool> is_running_{false};
  std::atomic<bool> initialized_{false};

  std::atomic<bool> is_registered_{false};
  std::atomic<bool> cm_ready_{true};
  folly::Baton<> register_condv_;
  std::mutex heartbeat_mutex_;
  std::condition_variable heartbeat_condv_;
  std::atomic<uint32_t> heartbeat_failure_count_{0};
  std::unique_ptr<std::thread> keepalive_thread_{nullptr};
  std::function<void()> cluster_disconnect_handler_{[]() {
    // raise SIGTERM to trigger handler to do data_server clean destruction
    if (std::raise(SIGTERM) != 0) {
      std::_Exit(EXIT_FAILURE);
    }
  }};

  std::string local_ip_;
  std::deque<std::atomic<size_t>> shard_used_bytes_;

  friend class KVCacheEvictor;
#if defined(SIMM_UNIT_TEST)
  FRIEND_TEST(KVServiceTest, TestClientHandlers);
  FRIEND_TEST(KVServiceLightTest, TestHeartbeatFailureCountResetOnSuccess);
  FRIEND_TEST(KVServiceLightTest, TestClusterManagerDisconnectHandlerInvokedOnToleranceReached);
  FRIEND_TEST(KVServiceLightTest, TestHeartbeatFailureToleranceTriggersReconnectWhenExitDisabled);
  FRIEND_TEST(KVServiceLightTest, TestClusterManagerDisconnectSignalPathRaisesSigterm);
  FRIEND_TEST(KVServiceLightTest, TestShmAllocatorDestructorReleasesSharedMemory);
#endif
};

}  // namespace ds
}  // namespace simm
