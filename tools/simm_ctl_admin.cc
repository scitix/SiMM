/*
 * Copyright (c) 2026 Scitix Tech PTE. LTD.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Simm RPC Management Tool -- SiMMCTL
 *
 */

#include <errno.h>
#include <sys/socket.h>
#include <sys/un.h> 
#include <algorithm>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <latch>
#include <memory>
#include <string>
#include <unistd.h>
#include <cstring>
#include <arpa/inet.h>
#include <cstdint>

#include <gflags/gflags.h>
#include <google/protobuf/message.h>
#include "boost/program_options.hpp"

#include "common/base/common_types.h"
#include "rpc/connection.h"
#include "rpc/rpc.h"
#include "rpc/rpc_context.h"
#include "transport/types.h"

#include "common/errcode/errcode_def.h"
#include "common/logging/logging.h"
#include "common/trace/trace_server.h"
#include "common/rpc_handlers/common_rpc_handlers.h"
#include "common/utils/tabulate.hpp"
#include "proto/cm_clnt_rpcs.pb.h"
#include "proto/common.pb.h"

namespace po = boost::program_options;

enum class ResourceType { Node, Shard, GFlag };

// Low-level channel interface
class AdminChannel {
 public:
  virtual ~AdminChannel() = default;

  // Initialize underlying transport (e.g., connect socket or RPC client)
  virtual bool Init() = 0;

  // Unified protobuf-centric API: send req, optionally receive resp.
  // If resp == nullptr, it is treated as one-way.
  virtual bool Call(const google::protobuf::Message &req,
                    google::protobuf::Message *resp,
                    std::function<void(const google::protobuf::Message *,
                                       const std::shared_ptr<sicl::rpc::RpcContext> &)> done_cb) = 0;
};

// Message type for UDS admin channel
// XXX: Should keep in sync with common/admin/admin_msg_types.h
enum class AdminMsgType : uint16_t {
  TRACE_TOGGLE = 1,
  GFLAG_LIST   = 2,
  GFLAG_GET    = 3,
  GFLAG_SET    = 4,
  DS_STATUS    = 5,
  CM_STATUS    = 6,
};

// Unix domain socket implementation
class UdsChannel : public AdminChannel {
 public:
  explicit UdsChannel(std::string path,
                      AdminMsgType type)
      : path_(std::move(path)), type_(type), fd_(-1) {}
  ~UdsChannel() override {
    if (fd_ >= 0) {
      ::close(fd_);
      fd_ = -1;
    }
  }

  bool Init() override {
    fd_ = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd_ < 0) {
      std::cerr << "trace uds: socket(AF_UNIX) failed\n";
      return false;
    }

    sockaddr_un addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    std::strncpy(addr.sun_path, path_.c_str(), sizeof(addr.sun_path) - 1);
    addr.sun_path[sizeof(addr.sun_path) - 1] = '\0';

    socklen_t addr_len = static_cast<socklen_t>(
        offsetof(sockaddr_un, sun_path) + std::strlen(addr.sun_path));

    if (::connect(fd_, reinterpret_cast<sockaddr *>(&addr), addr_len) < 0) {
      std::cerr << "trace uds: failed to connect to " << path_ << "\n";
      ::close(fd_);
      fd_ = -1;
      return false;
    }
    return true;
  }

 private:
  // Low-level send/recv helpers used only inside UdsChannel.
  bool Send(const void *buf, size_t len) {
    if (fd_ < 0) {
      return false;
    }
    const char *p = static_cast<const char *>(buf);
    size_t remaining = len;
    while (remaining > 0) {
      ssize_t n = ::write(fd_, p, remaining);
      if (n < 0) {
        if (errno == EINTR) {
          continue;
        }
        std::cerr << "trace uds: write() failed\n";
        return false;
      }
      if (n == 0) {
        std::cerr << "trace uds: write() returned 0, peer closed?\n";
        return false;
      }
      remaining -= static_cast<size_t>(n);
      p += n;
    }
    return true;
  }

  bool Recv(void *buf, size_t max_len, size_t &nread) {
    nread = 0;
    if (fd_ < 0) {
      return false;
    }
    ssize_t n = ::read(fd_, buf, max_len);
    if (n < 0) {
      if (errno == EINTR) {
        // caller can decide whether to call again
        return false;
      }
      std::cerr << "trace uds: read() failed\n";
      return false;
    }
    nread = static_cast<size_t>(n);
    return true;
  }

  // Helper to read exactly len bytes or fail.
  bool RecvAll(void *buf, size_t len) {
    char *p = static_cast<char *>(buf);
    size_t remaining = len;
    while (remaining > 0) {
      size_t nread = 0;
      if (!Recv(p, remaining, nread)) {
        return false;
      }
      if (nread == 0) {
        // Peer closed or no more data.
        return false;
      }
      remaining -= nread;
      p += nread;
    }
    return true;
  }

 public:
  bool Call(const google::protobuf::Message &req,
            google::protobuf::Message *resp,
            std::function<void(const google::protobuf::Message *,
                               const std::shared_ptr<sicl::rpc::RpcContext> &)> done_cb) override {
    // Extended framing: [uint32_t len][uint16_t type][payload]
    std::string buf;
    if (!req.SerializeToString(&buf)) {
      std::cerr << "uds: failed to serialize protobuf message" << std::endl;
      return false;
    }

    uint16_t type_raw = static_cast<uint16_t>(type_);
    uint16_t type_net = htons(type_raw);

    // len covers type + payload so that server can read one frame by len
    uint32_t len = static_cast<uint32_t>(sizeof(type_net) + buf.size());
    uint32_t len_net = htonl(len);

    // Send length
    if (!Send(&len_net, sizeof(len_net))) {
      std::cerr << "uds: failed to send message length" << std::endl;
      return false;
    }
    // Send type
    if (!Send(&type_net, sizeof(type_net))) {
      std::cerr << "uds: failed to send message type" << std::endl;
      return false;
    }
    // Send payload
    if (!Send(buf.data(), buf.size())) {
      std::cerr << "uds: failed to send message payload" << std::endl;
      return false;
    }

    // One-way case
    if (resp == nullptr) {
      if (done_cb) {
        done_cb(nullptr, nullptr);
      }
      return true;
    }

    // Read response: [uint32_t len][uint16_t type][payload]
    uint32_t resp_len_net = 0;
    if (!RecvAll(&resp_len_net, sizeof(resp_len_net))) {
      std::cerr << "uds: failed to read response length" << std::endl;
      return false;
    }
    uint32_t resp_len = ntohl(resp_len_net);
    if (resp_len < sizeof(uint16_t)) {
      std::cerr << "uds: invalid response length " << resp_len << std::endl;
      return false;
    }

    uint16_t resp_type_net = 0;
    if (!RecvAll(&resp_type_net, sizeof(resp_type_net))) {
      std::cerr << "uds: failed to read response type" << std::endl;
      return false;
    }
    uint16_t resp_type = ntohs(resp_type_net);
    (void)resp_type;  // client side currently ignores response type

    uint32_t payload_len = resp_len - static_cast<uint32_t>(sizeof(resp_type_net));
    std::string resp_buf(payload_len, '\0');
    if (!RecvAll(resp_buf.data(), payload_len)) {
      std::cerr << "uds: failed to read full response payload" << std::endl;
      return false;
    }

    if (!resp->ParseFromArray(resp_buf.data(), static_cast<int>(resp_buf.size()))) {
      std::cerr << "uds: failed to parse response protobuf" << std::endl;
      return false;
    }

    if (done_cb) {
      done_cb(resp, nullptr);
    }
    return true;
  }

 private:
  std::string path_;
  AdminMsgType type_;
  int fd_;
};

// SiRPC-based RPC implementation
class RpcChannel : public AdminChannel {
 public:
  RpcChannel(std::string ip,
             int port,
             sicl::rpc::ReqType req_type,
             std::shared_ptr<sicl::rpc::RpcContext> ctx,
             std::unique_ptr<sicl::rpc::SiRPC> client)
      : ip_(std::move(ip)),
        port_(port),
        req_type_(req_type),
        ctx_(std::move(ctx)),
        client_(std::move(client)) {}

  bool Init() override { return client_ != nullptr; }

  bool Call(const google::protobuf::Message &req,
            google::protobuf::Message *resp,
            std::function<void(const google::protobuf::Message *,
                               const std::shared_ptr<sicl::rpc::RpcContext> &)> done_cb) override {
    if (!client_) {
      return false;
    }
    google::protobuf::Message *resp_ptr = resp;
    client_->SendRequest(ip_,
                         port_,
                         req_type_,
                         req,
                         resp_ptr,
                         ctx_,
                         [done_cb](const google::protobuf::Message *rsp,
                                   const std::shared_ptr<sicl::rpc::RpcContext> ctx) {
                           if (done_cb) {
                             done_cb(rsp, ctx);
                           }
                         });
    return true;
  }

 private:
  std::string ip_;
  int port_;
  sicl::rpc::ReqType req_type_;
  std::shared_ptr<sicl::rpc::RpcContext> ctx_;
  std::unique_ptr<sicl::rpc::SiRPC> client_;
};

// Helper function to initialize RPC client and context
static void InitRpcClientAndContext(std::unique_ptr<sicl::rpc::SiRPC> &rpc_client,
                                    std::shared_ptr<sicl::rpc::RpcContext> &ctx_shared) {
  sicl::rpc::SiRPC *rpc_raw_client = nullptr;
  sicl::rpc::SiRPC::newInstance(rpc_raw_client, false);
  rpc_client.reset(rpc_raw_client);
  sicl::rpc::RpcContext *ctx_p;
  sicl::rpc::RpcContext::newInstance(ctx_p);
  ctx_shared = std::shared_ptr<sicl::rpc::RpcContext>(ctx_p);
}

static void CallbackNode(const std::string &operation,
                         const std::string &name,
                         const std::string &value,
                         const std::string &ip,
                         int port,
                         bool verbose) {
  // Initialize RPC client and context
  std::latch done_latch(1);
  std::unique_ptr<sicl::rpc::SiRPC> rpc_client{nullptr};
  std::shared_ptr<sicl::rpc::RpcContext> ctx_shared;
  InitRpcClientAndContext(rpc_client, ctx_shared);

  if (operation == "list") {
    auto done_cb = [&](const google::protobuf::Message *rsp, const std::shared_ptr<sicl::rpc::RpcContext> ctx) {
      if (ctx->Failed()) {
        std::cerr << "Error: RPC failed, err: " << ctx->ErrorText() << "\n";
      } else {
        auto *response = dynamic_cast<const ListNodesResponsePB *>(rsp);
        if (response->ret_code() == CommonErr::OK) {
          tabulate::Table tbl;
          tbl.format().locale("C");

          if (verbose) {
            // Verbose mode: show detailed information
            tbl.add_row({"Node Address", "Status", "Total Memory (MB)", "Free Memory (MB)", "Used Memory (MB)"})
                .format()
                .width(20);

            for (int i = 0; i < response->nodes_size(); ++i) {
              const auto &node_info = response->nodes(i);
              const auto &node_addr = node_info.node_address();
              std::string addr_str = node_addr.ip() + ":" + std::to_string(node_addr.port());

              // Convert node status to string
              std::string_view status_str =
                  simm::common::NodeStatusToString(static_cast<simm::common::NodeStatus>(node_info.node_status()));

              // Convert memory bytes to MB
              std::string total_mem = std::to_string(node_info.resource().mem_total_bytes() / (1024 * 1024));
              std::string free_mem = std::to_string(node_info.resource().mem_free_bytes() / (1024 * 1024));
              std::string used_mem = std::to_string(node_info.resource().mem_used_bytes() / (1024 * 1024));

              tbl.add_row({addr_str, status_str, total_mem, free_mem, used_mem});
            }

            tbl.column(0).format().width(20).font_style({tabulate::FontStyle::bold});
            tbl.column(1).format().width(12);
            tbl.column(2).format().width(18);
            tbl.column(3).format().width(18);
            tbl.column(4).format().width(18);
            tbl.row(0).format().font_style({tabulate::FontStyle::bold});
          } else {
            // Normal mode: show simple information
            tbl.add_row({"Node Address", "Status"}).format().width(20);

            for (int i = 0; i < response->nodes_size(); ++i) {
              const auto &node_info = response->nodes(i);
              const auto &node_addr = node_info.node_address();
              std::string addr_str = node_addr.ip() + ":" + std::to_string(node_addr.port());

              // Convert node status to string
              std::string status_str = (node_info.node_status() == 1) ? "RUNNING" : "DEAD";

              tbl.add_row({addr_str, status_str});
            }

            tbl.column(0).format().width(20).font_style({tabulate::FontStyle::bold});
            tbl.column(1).format().width(12);
            tbl.row(0).format().font_style({tabulate::FontStyle::bold});
          }

          std::cout << tbl << std::endl;
        } else {
          std::cerr << "Error: ListNodes RPC failed with ret_code: " << response->ret_code() << "\n";
        }
      }
      done_latch.count_down();
    };

    ListNodesRequestPB req;
    auto resp = new ListNodesResponsePB();
    rpc_client->SendRequest(ip,
                            port,
                            static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_LIST_NODE_REQ),
                            req,
                            resp,
                            ctx_shared,
                            done_cb);
  } else if (operation == "set") {
    // Parse node address from name (format: IP:PORT)
    size_t colon_pos = name.find(':');
    if (colon_pos == std::string::npos) {
      std::cerr << "Invalid node address format. Expected IP:PORT but got: " << name << "\n";
      exit(1);
    }
    std::string node_ip = name.substr(0, colon_pos);
    std::string node_port_str = name.substr(colon_pos + 1);

    // Parse status value: first try to match from kNodeStatusStrVec, then fallback to stoi
    int status_value = -1;

    // Try to match status string from kNodeStatusStrVec
    for (size_t i = 0; i < simm::common::kNodeStatusStrVec.size(); ++i) {
      if (simm::common::kNodeStatusStrVec[i] == value) {
        status_value = i;
        break;
      }
    }

    // Fallback to parsing as integer if not found in enum
    if (status_value == -1) {
      try {
        status_value = std::stoi(value);
        // Validate range (0=DEAD, 1=RUNNING)
        if (status_value < 0 || status_value >= static_cast<int>(simm::common::kNodeStatusStrVec.size())) {
          std::cerr << "Error: Invalid node status value. Expected 0 (DEAD), 1 (RUNNING), or status name but got: "
                    << value << "\n";
          exit(1);
        }
      } catch (const std::exception &e) {
        std::cerr << "Error: Failed to parse status value: " << value << "\n";
        std::cerr << "Expected: status name (e.g., RUNNING, DEAD) or numeric value (0, 1)\n";
        exit(1);
      }
    }

    auto done_cb = [&](const google::protobuf::Message *rsp, const std::shared_ptr<sicl::rpc::RpcContext> ctx) {
      if (ctx->Failed()) {
        LOG_ERROR("RPC failed, err:{}", ctx->ErrorText());
      } else {
        auto *response = dynamic_cast<const SetNodeStatusResponsePB *>(rsp);
        if (response->ret_code() == CommonErr::OK) {
          tabulate::Table tbl;
          tbl.format().locale("C");
          std::string_view status_str =
              simm::common::NodeStatusToString(static_cast<simm::common::NodeStatus>(status_value));
          tbl.add_row({"Node Address", name});
          tbl.add_row({"Status", status_str});
          tbl.column(0).format().width(20).font_style({tabulate::FontStyle::bold});
          tbl.column(1).format().width(30);
          std::cout << tbl << std::endl;
        } else {
          std::cerr << "Error: SetNodeStatus RPC failed with ret_code: " << response->ret_code() << "\n";
        }
      }
      done_latch.count_down();
    };

    SetNodeStatusRequestPB req;
    auto *node_addr = req.mutable_node();
    node_addr->set_ip(node_ip);
    node_addr->set_port(std::stoi(node_port_str));
    req.set_node_status(status_value);

    auto resp = new SetNodeStatusResponsePB();
    rpc_client->SendRequest(ip,
                            port,
                            static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_SET_NODE_STATUS_REQ),
                            req,
                            resp,
                            ctx_shared,
                            done_cb);
  } else {
    std::cerr << "Unsupported operation for node: " << operation << "\n";
    exit(1);
  }

  done_latch.wait();
}

static void CallbackDsStatus(AdminChannel &channel) {
  // Query DS internal status via UDS (is_registered, cm_ready, heartbeat_failure_count)
  proto::common::AdmDsStatusRequestPB req;
  auto *resp = new proto::common::AdmDsStatusResponsePB();
  std::latch done_latch(1);

  if (!channel.Call(
          req,
          resp,
          [&](const google::protobuf::Message *rsp,
              const std::shared_ptr<sicl::rpc::RpcContext> &ctx) {
            const auto *response = dynamic_cast<const proto::common::AdmDsStatusResponsePB *>(rsp);
            if (ctx && ctx->Failed()) {
              std::cerr << "Error: UDS call failed, err: " << ctx->ErrorText() << "\n";
            } else if (response && response->ret_code() == CommonErr::OK) {
              tabulate::Table tbl;
              tbl.format().locale("C");
              tbl.add_row({"Field", "Value"});
              tbl.add_row({"is_registered", response->is_registered() ? "true" : "false"});
              tbl.add_row({"cm_ready", response->cm_ready() ? "true" : "false"});
              tbl.add_row({"heartbeat_failure_count",
                           std::to_string(response->heartbeat_failure_count())});
              tbl.column(0).format().width(28).font_style({tabulate::FontStyle::bold});
              tbl.column(1).format().width(20);
              tbl.row(0).format().font_style({tabulate::FontStyle::bold});
              std::cout << tbl << std::endl;
            } else {
              std::cerr << "Error: DsStatus failed with ret_code: "
                        << (response ? response->ret_code() : -1) << "\n";
            }
            done_latch.count_down();
            delete resp;
          })) {
    std::cerr << "ds status: channel.Call() failed\n";
    delete resp;
    return;
  }

  done_latch.wait();
}

static void CallbackCmStatus(AdminChannel &channel) {
  proto::common::AdmCmStatusRequestPB req;
  auto *resp = new proto::common::AdmCmStatusResponsePB();
  std::latch done_latch(1);

  if (!channel.Call(
          req,
          resp,
          [&](const google::protobuf::Message *rsp,
              const std::shared_ptr<sicl::rpc::RpcContext> &ctx) {
            const auto *response = dynamic_cast<const proto::common::AdmCmStatusResponsePB *>(rsp);
            if (ctx && ctx->Failed()) {
              std::cerr << "Error: UDS call failed, err: " << ctx->ErrorText() << "\n";
            } else if (response && response->ret_code() == CommonErr::OK) {
              tabulate::Table tbl;
              tbl.format().locale("C");
              tbl.add_row({"Field", "Value"});
              tbl.add_row({"is_running", response->is_running() ? "true" : "false"});
              tbl.add_row({"service_ready", response->service_ready() ? "true" : "false"});
              tbl.add_row({"alive_node_count",
                           std::to_string(response->alive_node_count())});
              tbl.add_row({"dead_node_count",
                           std::to_string(response->dead_node_count())});
              tbl.add_row({"total_shard_count",
                           std::to_string(response->total_shard_count())});
              tbl.column(0).format().width(28).font_style({tabulate::FontStyle::bold});
              tbl.column(1).format().width(20);
              tbl.row(0).format().font_style({tabulate::FontStyle::bold});
              std::cout << tbl << std::endl;
            } else {
              std::cerr << "Error: CmStatus failed with ret_code: "
                        << (response ? response->ret_code() : -1) << "\n";
            }
            done_latch.count_down();
            delete resp;
          })) {
    std::cerr << "cm status: channel.Call() failed\n";
    delete resp;
    return;
  }

  done_latch.wait();
}

static void CallbackShard(const std::string &operation,
                          [[maybe_unused]] const std::string &name,
                          [[maybe_unused]] const std::string &value,
                          const std::string &ip,
                          int port,
                          bool verbose) {
  // Initialize RPC client and context
  std::latch done_latch(1);
  std::unique_ptr<sicl::rpc::SiRPC> rpc_client{nullptr};
  std::shared_ptr<sicl::rpc::RpcContext> ctx_shared;
  InitRpcClientAndContext(rpc_client, ctx_shared);

  if (operation == "list") {
    auto done_cb = [&](const google::protobuf::Message *rsp, const std::shared_ptr<sicl::rpc::RpcContext> ctx) {
      if (ctx->Failed()) {
        LOG_ERROR("RPC failed, err:{}", ctx->ErrorText());
      } else {
        auto *response = dynamic_cast<const QueryShardRoutingTableAllResponsePB *>(rsp);
        if (response->ret_code() == CommonErr::OK) {
          tabulate::Table tbl;
          tbl.format().locale("C");
          if (verbose) {
            tbl.add_row({"Data Server", "Shard IDs"}).format().width(30);
            // Use map to merge shard IDs by data server address
            std::map<std::string, std::vector<uint32_t>> ds_shards;
            for (int i = 0; i < response->shard_info_size(); ++i) {
              const auto &shard_entry = response->shard_info(i);
              const auto &ds_addr = shard_entry.data_server_address();
              std::string ds_str = ds_addr.ip() + ":" + std::to_string(ds_addr.port());

              for (int j = 0; j < shard_entry.shard_ids_size(); ++j) {
                ds_shards[ds_str].push_back(shard_entry.shard_ids(j));
              }
            }

            for (const auto &[ds_str, shard_ids] : ds_shards) {
              std::string shard_ids_str;
              for (size_t j = 0; j < shard_ids.size(); ++j) {
                if (j > 0)
                  shard_ids_str += ", ";
                shard_ids_str += std::to_string(shard_ids[j]);
              }
              tbl.add_row({ds_str, shard_ids_str});
            }
            tbl.column(0).format().width(20).font_style({tabulate::FontStyle::bold});
            tbl.column(1).format().width(50);
            tbl.row(0).format().font_style({tabulate::FontStyle::bold});
            std::cout << tbl << std::endl;
          } else {
            tbl.add_row({"Data Server", "Shard Count"}).format().width(30);
            // Use map to aggregate shard count by data server address
            std::map<std::string, int> ds_shard_count;
            for (int i = 0; i < response->shard_info_size(); ++i) {
              const auto &shard_entry = response->shard_info(i);
              const auto &ds_addr = shard_entry.data_server_address();
              std::string ds_str = ds_addr.ip() + ":" + std::to_string(ds_addr.port());
              ds_shard_count[ds_str] += shard_entry.shard_ids_size();
            }

            for (const auto &[ds_str, count] : ds_shard_count) {
              tbl.add_row({ds_str, std::to_string(count)});
            }
            tbl.column(0).format().width(20).font_style({tabulate::FontStyle::bold});
            tbl.column(1).format().width(15);
            tbl.row(0).format().font_style({tabulate::FontStyle::bold});
            std::cout << tbl << std::endl;
          }
        } else {
          LOG_ERROR("QueryShardRoutingTableAllResponsePB not ok, ret_code:{}", response->ret_code());
          std::cerr << "Error: ListShards RPC failed with ret_code: " << response->ret_code() << "\n";
        }
      }
      done_latch.count_down();
    };
    QueryShardRoutingTableAllRequestPB req;
    auto resp = new QueryShardRoutingTableAllResponsePB();
    rpc_client->SendRequest(ip,
                            port,
                            static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_LIST_SHARD_REQ),
                            req,
                            resp,
                            ctx_shared,
                            done_cb);
  } else {
    std::cerr << "Unsupported operation for shard: " << operation << "\n";
    exit(1);
  }

  done_latch.wait();
}

static void CallbackGFlag(AdminChannel &channel,
                              const std::string &operation,
                              const std::string &name,
                              const std::string &value,
                              bool verbose) {
  std::latch done_latch(1);

  if (operation == "list") {
    auto *resp = new proto::common::ListAllGFlagsResponsePB();
    proto::common::ListAllGFlagsRequestPB req;

    if (!channel.Call(
            req,
            resp,
            [&](const google::protobuf::Message *rsp,
                const std::shared_ptr<sicl::rpc::RpcContext> &ctx) {
              const auto *response = dynamic_cast<const proto::common::ListAllGFlagsResponsePB *>(rsp);
              if (ctx && ctx->Failed()) {
                LOG_ERROR("RPC failed, err:{}", ctx->ErrorText());
              } else if (response && response->ret_code() == CommonErr::OK) {
                tabulate::Table tbl;
                tbl.format().locale("C");
                if (verbose) {
                  tbl.add_row({"Flag Name", "VALUE", "Default Value", "TYPE", "Description"}).format().width(15);
                  for (int i = 0; i < response->flags_size(); ++i) {
                    const auto &f = response->flags(i);
                    tbl.add_row({f.flag_name(),
                                 f.flag_value(),
                                 f.flag_default_value(),
                                 f.flag_type(),
                                 f.flag_description()});
                  }
                  tbl.column(0).format().width(30).font_style({tabulate::FontStyle::bold});
                  tbl.column(4).format().width(30);
                } else {
                  tbl.add_row({"Flag Name", "VALUE"}).format().width(30);
                  for (int i = 0; i < response->flags_size(); ++i) {
                    const auto &f = response->flags(i);
                    tbl.add_row({f.flag_name(), f.flag_value()});
                  }
                  tbl.row(0).format().font_style({tabulate::FontStyle::bold});
                }
                std::cout << tbl << std::endl;
              } else {
                LOG_ERROR("ListAllGFlagsResponsePB not ok, ret_code:{}",
                          response ? response->ret_code() : -1);
                std::cerr << "Error: ListGFlags RPC failed" << std::endl;
              }
              done_latch.count_down();
              delete resp;
            })) {
      std::cerr << "gflag list: channel.Call() failed" << std::endl;
      delete resp;
      return;
    }
  } else if (operation == "get") {
    auto *resp = new proto::common::GetGFlagValueResponsePB();
    proto::common::GetGFlagValueRequestPB req;
    req.set_flag_name(name);

    if (!channel.Call(
            req,
            resp,
            [&](const google::protobuf::Message *rsp,
                const std::shared_ptr<sicl::rpc::RpcContext> &ctx) {
              const auto *response = dynamic_cast<const proto::common::GetGFlagValueResponsePB *>(rsp);
              if (ctx && ctx->Failed()) {
                LOG_ERROR("RPC failed, err:{}", ctx->ErrorText());
              } else if (response && response->ret_code() == CommonErr::OK) {
                const auto &flag_info = response->flag_info();
                tabulate::Table tbl;
                tbl.format().locale("C");
                tbl.add_row({"Flag Name", flag_info.flag_name()});
                tbl.add_row({"VALUE", flag_info.flag_value()});
                if (verbose) {
                  tbl.add_row({"Default Value", flag_info.flag_default_value()});
                  tbl.add_row({"TYPE", flag_info.flag_type()});
                  tbl.add_row({"Description", flag_info.flag_description()});
                }
                tbl.column(0).format().width(20).font_style({tabulate::FontStyle::bold});
                tbl.column(1).format().width(50);
                std::cout << tbl << std::endl;
              } else {
                LOG_ERROR("GetGFlagValueResponsePB not ok, ret_code:{}",
                          response ? response->ret_code() : -1);
                std::cerr << "Error: GetGFlagValue RPC failed" << std::endl;
              }
              done_latch.count_down();
              delete resp;
            })) {
      std::cerr << "gflag get: channel.Call() failed" << std::endl;
      delete resp;
      return;
    }
  } else if (operation == "set") {
    auto *resp = new proto::common::SetGFlagValueResponsePB();
    proto::common::SetGFlagValueRequestPB req;
    req.set_flag_name(name);
    req.set_flag_value(value);

    if (!channel.Call(
            req,
            resp,
            [&](const google::protobuf::Message *rsp,
                const std::shared_ptr<sicl::rpc::RpcContext> &ctx) {
              const auto *response = dynamic_cast<const proto::common::SetGFlagValueResponsePB *>(rsp);
              if (ctx && ctx->Failed()) {
                LOG_ERROR("RPC failed, err:{}", ctx->ErrorText());
              } else if (response && response->ret_code() == CommonErr::OK) {
                tabulate::Table tbl;
                tbl.format().locale("C");
                tbl.add_row({"Flag Name", name});
                tbl.add_row({"VALUE", value});
                tbl.column(0).format().width(20).font_style({tabulate::FontStyle::bold});
                tbl.column(1).format().width(50);
                std::cout << tbl << std::endl;
              } else {
                LOG_ERROR("SetGFlagValueResponsePB not ok, ret_code:{}",
                          response ? response->ret_code() : -1);
                std::cerr << "Error: SetGFlagValue RPC failed" << std::endl;
              }
              done_latch.count_down();
              delete resp;
            })) {
      std::cerr << "gflag set: channel.Call() failed" << std::endl;
      delete resp;
      return;
    }
  } else {
    std::cerr << "Unsupported operation for gflag (tmp): " << operation << "\n";
    return;
  }

  done_latch.wait();
}

static void CallbackTrace(AdminChannel &channel,
                               const std::string &value,
                               bool verbose) {
  (void)verbose;

  proto::common::TraceToggleRequestPB req;
  auto *resp = new proto::common::TraceToggleResponsePB();
  req.set_enable_trace(value == "1");

  std::latch done_latch(1);
  if (!channel.Call(req,
                    resp,
                    [&](const google::protobuf::Message *rsp,
                        const std::shared_ptr<sicl::rpc::RpcContext> &ctx) {
              const auto *response = dynamic_cast<const proto::common::TraceToggleResponsePB *>(rsp);
              if (ctx && ctx->Failed()) {
                LOG_ERROR("RPC failed, err:{}", ctx->ErrorText());
              } else if (!response || response->ret_code() != CommonErr::OK) {
                LOG_ERROR("SetGFlagValueResponsePB not ok, ret_code:{}",
                          response ? response->ret_code() : -1);
                std::cerr << "Error: SetGFlagValue RPC failed" << std::endl;
              }
              done_latch.count_down();
              delete resp;
            })) {
    std::cerr << "trace: channel.Call() failed" << std::endl;
    return;
  }
  done_latch.wait();
}

int main(int argc, char *argv[]) {
  std::string ip;
  int port;
  int pid;
  std::string subcommand;
  std::string resource_type;
  std::string operation;
  std::vector<std::string> args;
  bool verbose = false;

  po::options_description desc("SiMMCtl - SiMM RPC Management Tool");
  desc.add_options()("help,h", "Show help message")(
      "ip,i", po::value<std::string>(&ip)->default_value(""), "Target IP address for RPC-based commands")(
      "port,p", po::value<int>(&port)->default_value(30002), "Target port for RPC-based commands")(
      "pid,P", po::value<int>(&pid)->default_value(-1), "Target process PID for UDS-based commands")(
      "verbose,v", po::bool_switch(&verbose), "Enable verbose output");

  po::options_description hidden("Hidden options");
  hidden.add_options()("subcommand", po::value<std::string>(&subcommand))("args",
                                                                          po::value<std::vector<std::string>>(&args));

  po::options_description all;
  all.add(desc).add(hidden);

  po::positional_options_description p;
  p.add("subcommand", 1).add("args", -1);

  try {
    po::variables_map vm;
    po::store(po::command_line_parser(argc, argv).options(all).positional(p).run(), vm);

    if (vm.count("help")) {
      std::cout << "Usage: " << argv[0] << " [OPTIONS] SUBCOMMAND [ARGS]\n\n";
      std::cout << "OPTIONS:\n" << desc << "\n";
      std::cout << "SUBCOMMANDS:\n"
                << "  node list [OPTIONS]           List all nodes\n"
                << "  node set <IP:PORT> <STATUS>   Set node status (0=DEAD, 1=RUNNING)\n"
                << "  cm status --pid <PID>          Query CM internal status via UDS\n"
                << "  ds status --pid <PID>          Query DS internal status via UDS\n"
                << "  shard list [OPTIONS]          List all shards\n"
                << "  gflag list [OPTIONS]          List all gflags\n"
                << "  gflag get <NAME> [OPTIONS]    Get a gflag value\n"
                << "  gflag set <NAME> <VALUE>      Set a gflag value\n"
                << "  trace <STATUS>                Set tracing status (0=OFF, 1=ON)\n";
      return 0;
    }

    po::notify(vm);

    if (!vm.count("subcommand")) {
      std::cerr << "Error: No subcommand specified\n";
      std::cerr << "Use 'simmctl -h' for help\n";
      return 1;
    }

    // Parse subcommand format: "resource_type operation"
    if (subcommand == "node") {
      if (args.empty()) {
        std::cerr << "Error: node subcommand requires an operation (list or set)\n";
        return 1;
      }
      operation = args[0];
      if (operation == "list") {
        CallbackNode("list", "", "", ip, port, verbose);
      } else if (operation == "set") {
        if (args.size() < 3) {
          std::cerr << "Error: node set requires arguments: <IP:PORT> <STATUS>\n";
          return 1;
        }
        std::string node_addr = args[1];
        std::string node_status = args[2];
        CallbackNode("set", node_addr, node_status, ip, port, verbose);
      } else {
        std::cerr << "Error: Unknown node operation: " << operation << "\n";
        return 1;
      }
    } else if (subcommand == "cm") {
      if (args.empty()) {
        std::cerr << "Error: cm subcommand requires an operation (status)\n";
        return 1;
      }
      operation = args[0];
      if (operation == "status") {
        if (pid == -1) {
          std::cerr << "Error: cm status requires --pid <CM_PID>\n";
          return 1;
        }
        std::string socket_path = "/run/simm/admin_cm." + std::to_string(pid) + ".sock";
        auto uds_channel = std::make_unique<UdsChannel>(socket_path, AdminMsgType::CM_STATUS);
        if (!uds_channel->Init()) {
          std::cerr << "Error: failed to connect to admin socket: " << socket_path << "\n";
          return 1;
        }
        CallbackCmStatus(*uds_channel);
      } else {
        std::cerr << "Error: Unknown cm operation: " << operation << "\n";
        return 1;
      }
    } else if (subcommand == "ds") {
      if (args.empty()) {
        std::cerr << "Error: ds subcommand requires an operation (status)\n";
        return 1;
      }
      operation = args[0];
      if (operation == "status") {
        if (pid == -1) {
          std::cerr << "Error: ds status requires --pid <DS_PID>\n";
          return 1;
        }
        std::string socket_path = "/run/simm/admin_ds." + std::to_string(pid) + ".sock";
        auto uds_channel = std::make_unique<UdsChannel>(socket_path, AdminMsgType::DS_STATUS);
        if (!uds_channel->Init()) {
          std::cerr << "Error: failed to connect to admin socket: " << socket_path << "\n";
          return 1;
        }
        CallbackDsStatus(*uds_channel);
      } else {
        std::cerr << "Error: Unknown ds operation: " << operation << "\n";
        return 1;
      }
    } else if (subcommand == "shard") {
      if (args.empty()) {
        std::cerr << "Error: shard subcommand requires an operation (list)\n";
        return 1;
      }
      operation = args[0];
      if (operation == "list") {
        CallbackShard("list", "", "", ip, port, verbose);
      } else {
        std::cerr << "Error: Unknown shard operation: " << operation << "\n";
        return 1;
      }
    } else if(subcommand == "gflag" || subcommand == "trace"){
      std::unique_ptr<AdminChannel> channel_ptr{nullptr};
      operation = args[0];
      if (pid == -1) {
        sicl::rpc::ReqType req_type;
        if (subcommand == "gflag") {
          if (args.empty()) {
            std::cerr << "Error: gflag subcommand requires an operation (list, get, or set)\n";
            return 1;
          }
          if (operation == "list") {
            req_type = static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_LIST_GFLAGS_REQ);
          } else if (operation == "get") {
            req_type = static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_GET_GFLAG_REQ);
          } else if (operation == "set") {
            req_type = static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_SET_GFLAG_REQ);
          } else {
            std::cerr << "Error: Unknown gflag operation: " << operation << "\n";
            return 1;
          }
        } else {
          // trace
          req_type = static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_TRACE_TOGGLE_REQ);
        }

        std::unique_ptr<sicl::rpc::SiRPC> rpc_client{nullptr};
        std::shared_ptr<sicl::rpc::RpcContext> ctx_shared;
        InitRpcClientAndContext(rpc_client, ctx_shared);

        auto rpc_channel = std::make_unique<RpcChannel>(ip, port, req_type, ctx_shared, std::move(rpc_client));
        if (!rpc_channel->Init()) {
          std::cerr << "Error: failed to init RpcChannel\n";
          return 1;
        }
        channel_ptr = std::move(rpc_channel);
      } else {
        std::string socket_path = "/run/simm/simm_trace." + std::to_string(pid) + ".sock";
        AdminMsgType msg_type;
        if (subcommand == "trace") {
          msg_type = AdminMsgType::TRACE_TOGGLE;
        } else {  // gflag over UDS
          if (operation == "list") {
            msg_type = AdminMsgType::GFLAG_LIST;
          } else if (operation == "get") {
            msg_type = AdminMsgType::GFLAG_GET;
          } else if (operation == "set") {
            msg_type = AdminMsgType::GFLAG_SET;
          } else {
            std::cerr << "Error: Unknown gflag operation: " << operation << "\n";
            return 1;
          }
        }
        auto uds_channel = std::make_unique<UdsChannel>(socket_path, msg_type);
        if (!uds_channel->Init()) {
          std::cerr << "Error: failed to init UdsChannel\n";
          return 1;
        }
        channel_ptr = std::move(uds_channel);
      }

      if (subcommand == "gflag") {
        if (operation == "list") {
          CallbackGFlag(*channel_ptr, "list", "", "", verbose);
        } else if (operation == "get") {
          if (args.size() < 2) {
            std::cerr << "Error: gflag get requires argument: <NAME>\n";
            return 1;
          }
          std::string flag_name = args[1];
          CallbackGFlag(*channel_ptr, "get", flag_name, "", verbose);
        } else if (operation == "set"){  // set
          if (args.size() < 3) {
            std::cerr << "Error: gflag set requires arguments: <NAME> <VALUE>\n";
            return 1;
          }
          std::string flag_name = args[1];
          std::string flag_value = args[2];
          CallbackGFlag(*channel_ptr, "set", flag_name, flag_value, verbose);
        } else {
          std::cerr << "Error: Unknown gflag operation: " << operation << "\n";
          return 1;
        }
      } else if (subcommand == "trace") {
        if (args.empty()) {
          std::cerr << "Error: trace subcommand requires argument: <STATUS>\n";
          return 1;
        }
        std::string trace_status = args[0];
        if (trace_status != "0" && trace_status != "1") {
          std::cerr << "Error: Invalid trace status: " << trace_status << ". Expected 0 (OFF) or 1 (ON)\n";
          return 1;
        }
        CallbackTrace(*channel_ptr, trace_status, verbose);
      } else {
        std::cerr << "Error: Unknown subcommand: " << subcommand << "\n";
        std::cerr << "Use 'simmctl -h' for help\n";
        return 1;
      }
    } else {
      std::cerr << "Error: Unknown subcommand: " << subcommand << "\n";
      std::cerr << "Use 'simmctl -h' for help\n";
      return 1;
    }
  } catch (const po::error &e) {
    std::cerr << "Error: " << e.what() << "\n";
    std::cerr << "Use 'simmctl -h' for help\n";
    return 1;
  }

  return 0;
}