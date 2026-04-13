#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <unordered_set>

#include <folly/concurrency/AtomicSharedPtr.h>
#include <folly/concurrency/ConcurrentHashMap.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/experimental/coro/BlockingWait.h>
#include <folly/experimental/coro/Collect.h>
#include <folly/experimental/coro/Task.h>
#include <folly/synchronization/Baton.h>

#include "common/context/context.h"
#include "rpc/connection.h"
#include "rpc/rpc.h"
#include "transport/ibv_manager.h"

#include "common/base/common_types.h"
#include "common/base/memory.h"
#include "common/errcode/errcode_def.h"
#include "common/hashkit/hashkit.h"
#include "proto/cm_clnt_rpcs.pb.h"

#include "common/trace/trace_server.h"

namespace simm {
namespace clnt {

using Callback =
    std::function<void(const google::protobuf::Message *rsp, const std::shared_ptr<sicl::rpc::RpcContext> ctx)>;

#if defined(SIMM_UNIT_TEST)
class ClientMessengerTestPeer;
#endif

class ClientMessenger {
 public:
  explicit ClientMessenger();
  virtual ~ClientMessenger();
  static ClientMessenger &Instance();

  // Get Routing table from Cluster Manager, init shard_route_map and build
  // connections with data server
  error_code_t Init();

  // Calculate shard_id from key using hashkit, and get server address of shard_id from local
  // routing table or Cluster Manager.
  std::shared_ptr<simm::common::NodeAddress> GetServerAddress(const std::string &key);

  // IO Messages
  error_code_t Put(const std::string &key,
                   std::shared_ptr<simm::common::MemBlock> memp,
                   std::shared_ptr<simm::common::SimmContext> ctx);
  error_code_t AsyncPut(const std::string &key,
                        std::shared_ptr<simm::common::MemBlock> memp,
                        std::function<void(int)> callback,
                        std::shared_ptr<simm::common::SimmContext> ctx);
  // return get bytes num when succeed, return error_code_t when failed
  int32_t Get(const std::string &key,
              std::shared_ptr<simm::common::MemBlock> memp,
              std::shared_ptr<simm::common::SimmContext> ctx);
  error_code_t AsyncGet(const std::string &key,
                        std::shared_ptr<simm::common::MemBlock> memp,
                        std::function<void(int)> callback,
                        std::shared_ptr<simm::common::SimmContext> ctx);
  error_code_t Exists(const std::string &key, std::shared_ptr<simm::common::SimmContext> ctx);
  error_code_t AsyncExists(const std::string &key,
                           std::function<void(int)> callback,
                           std::shared_ptr<simm::common::SimmContext> ctx);
  error_code_t Delete(const std::string &key, std::shared_ptr<simm::common::SimmContext> ctx);
  error_code_t AsyncDelete(const std::string &key,
                           std::function<void(int)> callback,
                           std::shared_ptr<simm::common::SimmContext> ctx);

  std::vector<error_code_t> MultiPut(const std::vector<std::string> &keys,
                                     std::vector<std::shared_ptr<simm::common::MemBlock>> datas);
  // return get bytes num when succeed, return error_code_t when failed
  std::vector<int32_t> MultiGet(const std::vector<std::string> &keys,
                                std::vector<std::shared_ptr<simm::common::MemBlock>> datas);
  std::vector<error_code_t> MultiExists(const std::vector<std::string> &keys);

 private:
  // get cluster manager address from k8s or etcd
  std::string get_cm_address();

  // (re)initialize
  error_code_t ReInit();

  // Register RPC handlers
  error_code_t RegisterHandlers();

  // build sicl connection
  enum class BuildConnWaitMode {
    kWaitForInflight,
    kNoWait,
  };
  error_code_t build_connection(const std::string &addr,
                                BuildConnWaitMode wait_mode = BuildConnWaitMode::kWaitForInflight);

  // RPC options in threadpool
  template <typename RequestType, typename ResponseType>
  error_code_t call_sync(uint16_t shard_id,
                         const sicl::rpc::ReqType req_type,
                         const RequestType &req,
                         std::shared_ptr<ResponseType> resp,
                         std::shared_ptr<simm::common::SimmContext> ctx);
  template <typename RequestType, typename ResponseType>
  error_code_t call_async(uint16_t shard_id,
                          const sicl::rpc::ReqType req_type,
                          const RequestType &req,
                          std::shared_ptr<ResponseType> resp,
                          std::shared_ptr<simm::common::SimmContext> ctx,
                          Callback callback);
  template <typename RequestType, typename ResponseType>
  error_code_t execute(const std::string &addr,
                       const sicl::rpc::ReqType req_type,
                       const RequestType &req,
                       std::shared_ptr<ResponseType> resp,
                       std::shared_ptr<simm::common::SimmContext> ctx,
                       Callback callback);

  // Send rpc request to cluster_manager ip:port, get newest routing table.
  std::pair<error_code_t, std::shared_ptr<QueryShardRoutingTableAllResponsePB>> update_all_route_table(
      const std::string &ip_port);

  struct ConnectionContext;  // forward declaration
  // Trigger reconnect to data server upon specified rpc errors
  void ReconnectByErrors(std::shared_ptr<sicl::rpc::RpcContext> rpc_ctx,
                         std::shared_ptr<ConnectionContext> ds_ctx,
                         uint16_t shard_id,
                         size_t old_conn_gen_num);
  std::shared_ptr<ConnectionContext> GetOrCreateConnectionContext(const std::string &addr);
  void PruneStaleConnectionContexts(const std::unordered_set<std::string> &live_servers);
  void HandleAsyncRequestFailure(std::shared_ptr<sicl::rpc::RpcContext> rpc_ctx,
                                 std::shared_ptr<ConnectionContext> request_ds_ctx,
                                 uint16_t shard_id,
                                 size_t old_conn_gen_num);

  // Convert timeout setting from user in milliseconds to sicl TimerTick type
  sicl::transport::TimerTick convert_timeout_setting_to_timer_tick(int32_t timeout_ms);

  void ReleaseConnectionContext(const std::shared_ptr<ConnectionContext> &ds_ctx);
  error_code_t ApplyRouteTableDiff(const QueryShardRoutingTableAllResponsePB &routing);

#if defined(SIMM_UNIT_TEST)
  friend class ClientMessengerTestPeer;
#endif

 private:
  struct ConnectionContext {
    std::string ip_port;
    std::atomic<bool> active{false};
    /// monotonic to prevent ABA (race condition)
    std::atomic<size_t> gen_num{0};

    ConnectionContext(const std::string &_ip_port) : ip_port(_ip_port), active(false), gen_num(0) {}

    void StoreConnection(std::shared_ptr<sicl::rpc::Connection> new_connection) {
      std::unique_lock lock(connection_mutex_);
      connection_ = std::move(new_connection);
    }

    std::shared_ptr<sicl::rpc::Connection> LoadConnection() const {
      std::shared_lock lock(connection_mutex_);
      return connection_;
    }

   private:
    friend class ClientMessenger;

    mutable std::shared_mutex connection_mutex_;
    std::shared_ptr<sicl::rpc::Connection> connection_{nullptr};
    std::mutex connect_wait_mutex_;
    std::condition_variable connect_cv_;
    std::atomic<bool> connecting_{false};
  };

  // cluster manager address
  std::string cm_addr_{""};

  // high performance concurrent hash table to store [shard_id, ip_port_str]
  // use atomic shared_ptr to avoid long update when replace entire table
  folly::ConcurrentHashMap<uint16_t, std::shared_ptr<ConnectionContext>> shard_table_;
  folly::ConcurrentHashMap<std::string, std::shared_ptr<ConnectionContext>> ds_conn_ctxs_;
  uint32_t shard_num_{0};

  bool initialized_{false};
  simm::hashkit::HashkitBase *hashkit_{nullptr};

  uint32_t thread_pool_size_{1};
  folly::IOThreadPoolExecutor executor_;

  // sicl objects
  sicl::rpc::SiRPC *rpc_client_{nullptr};
  sicl::rpc::SiRPC *admin_rpc_service_{nullptr};
  sicl::transport::IbvDeviceManager *ibv_mgr_{nullptr};
  sicl::transport::Mempool *mempool_{nullptr};

  // simm client request timeout settings
  sicl::transport::TimerTick sync_req_timeout_ms_{sicl::transport::TimerTick::TIMER_3S};
  sicl::transport::TimerTick async_req_timeout_ms_{sicl::transport::TimerTick::TIMER_3S};

  /// Failover thread.
  /// It will:
  /// - detect changes of the cluster manager
  /// - reconnect to data servers on request
  /// - reinitialize everything if cluster manager changes
  std::unique_ptr<std::thread> failover_thread_{nullptr};
  std::atomic<bool> failover_flag_{true};
  std::mutex failover_mutex_;
  std::condition_variable failover_condv_;

  // track when each DS was first seen as dead, used for failover wait window
  std::mutex ds_dead_since_mtx_;
  std::unordered_map<std::string, std::chrono::steady_clock::time_point> ds_dead_since_;

  std::unique_ptr<simm::trace::TraceServer> trace_server_{nullptr};

#if defined(SIMM_UNIT_TEST)
 private:
  // ONLY for mock UT cases
  std::function<std::string()> test_get_cm_address_hook_{nullptr};
  std::function<std::pair<error_code_t, std::shared_ptr<QueryShardRoutingTableAllResponsePB>>(const std::string &)>
      test_route_query_hook_{nullptr};
  std::function<error_code_t(const std::string &)> test_build_connection_hook_{nullptr};
#endif
};

}  // namespace clnt
}  // namespace simm
