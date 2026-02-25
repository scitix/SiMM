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
 */

#include <errno.h>
#include <algorithm>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <latch>
#include <memory>
#include <string>

#include <gflags/gflags.h>
#include <google/protobuf/message.h>

#include "rpc/connection.h"
#include "rpc/rpc.h"
#include "rpc/rpc_context.h"
#include "transport/types.h"

#include "common/errcode/errcode_def.h"
#include "common/logging/logging.h"
#include "common/rpc_handlers/common_rpc_handlers.h"
#include "common/utils/tabulate.hpp"
#include "proto/common.pb.h"

static const std::string sInvalidIp{"invalid_ip"};
static constexpr int32_t sInvalidPort{-1};
static const std::string sInvalidMethod{"invalid_method"};

DEFINE_string(ip, sInvalidIp, "target node ip, can be simm clnt/ds/cm nodes");
DEFINE_int32(port, sInvalidPort, "target node port to handle rpc requests");
DEFINE_string(method, sInvalidMethod, "only support get/set/list methods");
DEFINE_bool(verbose, false, "Show verbose output with default value, type and description");
DEFINE_string(flag, "", "target flag name to get/set, ignored when list");
DEFINE_string(value, "", "value to set, ignored when get/list");

void usage() {
  std::cout << "simm_flags_admin --ip=192.168.1.1 --port=30001 --method=[get|set|list] [--flag=...] [--value=...]"
            << std::endl;
}

void check_args() {
  bool args_valid{false};

  if (FLAGS_ip == sInvalidIp || FLAGS_port == sInvalidPort) {
    LOG_ERROR("Invalid ip({}) or port({})", FLAGS_ip, FLAGS_port);
  } else if (FLAGS_method != "get" && FLAGS_method != "set" && FLAGS_method != "list") {
    LOG_ERROR("Invalid method({}), should be get/set/list", FLAGS_method);
  } else if (FLAGS_flag.empty() && (FLAGS_method == "get" || FLAGS_method == "set")) {
    LOG_ERROR("Empty flag name({}), should pass in valid name");
  } else if (FLAGS_value.empty() && FLAGS_method == "set") {
    LOG_ERROR("Empty value({}), should pass in valid value to set");
  } else {
    args_valid = true;
  }

  if (!args_valid) {
    usage();
    std::exit(EINVAL);
  }
}

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  check_args();

  std::latch done_latch(1);

  // rpc client
  std::unique_ptr<sicl::rpc::SiRPC> rpc_client{nullptr};
  sicl::rpc::SiRPC *rpc_raw_client = nullptr;
  sicl::rpc::SiRPC::newInstance(rpc_raw_client, false /*is_server*/);
  rpc_client.reset(rpc_raw_client);
  // rpc context and callback function
  sicl::rpc::RpcContext *ctx_p;
  sicl::rpc::RpcContext::newInstance(ctx_p);
  std::shared_ptr<sicl::rpc::RpcContext> ctx = std::shared_ptr<sicl::rpc::RpcContext>(ctx_p);
  auto done_cb = [&](const google::protobuf::Message *rsp, const std::shared_ptr<sicl::rpc::RpcContext> ctx) {
    if (ctx->Failed()) {
      LOG_ERROR("RPC request failed, err:{}", ctx->ErrorText());
    } else {
      if (FLAGS_method == "get") {
        auto *response = dynamic_cast<const proto::common::GetGFlagValueResponsePB *>(rsp);
        if (response->ret_code() == CommonErr::OK) {
          const auto &flag = response->flag_info();

          tabulate::Table tbl;
          tbl.format().locale("C");

          tbl.add_row({"Flag Name", flag.flag_name()});
          tbl.add_row({"Value", flag.flag_value()});

          if (FLAGS_verbose) {
            tbl.add_row({"Default Value", flag.flag_default_value()});
            tbl.add_row({"Type", flag.flag_type()});
            tbl.add_row({"Description", flag.flag_description()});
          }

          tbl.column(0).format().width(20).font_style({tabulate::FontStyle::bold});
          tbl.column(1).format().width(50);

          std::cout << tbl << std::endl;
        } else {
          LOG_ERROR("GetGFlagValueResponsePB return not OK, ret_code:{}", response->ret_code());
        }
      } else if (FLAGS_method == "set") {
        auto *response = dynamic_cast<const proto::common::SetGFlagValueResponsePB *>(rsp);

        if (response->ret_code() == CommonErr::OK) {
          tabulate::Table tbl;
          tbl.format().locale("C");

          tbl.add_row({"Flag Name", FLAGS_flag});
          tbl.add_row({"Value", FLAGS_value});

          tbl.column(0).format().width(20).font_style({tabulate::FontStyle::bold});
          tbl.column(1).format().width(50);

          std::cout << tbl << std::endl;
        } else {
          LOG_ERROR("SetGFlagValueResponsePB return not OK, ret_code:{}", response->ret_code());
        }
      } else if (FLAGS_method == "list") {
        auto *response = dynamic_cast<const proto::common::ListAllGFlagsResponsePB *>(rsp);

        tabulate::Table tbl;
        tbl.format().locale("C");

        if (FLAGS_verbose) {
          tbl.add_row({"Flag Name", "Value", "Default Value", "Type", "Description"}).format().width(15);
          for (int i = 0; i < response->flags_size(); i++) {
            const auto &flag = response->flags(i);
            tbl.add_row({flag.flag_name(),
                         flag.flag_value(),
                         flag.flag_default_value(),
                         flag.flag_type(),
                         flag.flag_description()});
          }

          tbl.column(0).format().width(30).font_style({tabulate::FontStyle::bold});
          tbl.column(4).format().width(30);
        } else {
          tbl.add_row({"Flag Name", "Value"}).format().width(30);
          for (int i = 0; i < response->flags_size(); i++) {
            const auto &flag = response->flags(i);
            tbl.add_row({flag.flag_name(), flag.flag_value()});
          }
        }

        tbl.row(0).format().font_style({tabulate::FontStyle::bold});

        std::cout << tbl << std::endl;
      }
    }
    // decrease the waiting counter for every set req done
    done_latch.count_down();
  };

  if (FLAGS_method == "get") {
    proto::common::GetGFlagValueRequestPB req;
    auto resp = new proto::common::GetGFlagValueResponsePB();
    req.set_flag_name(FLAGS_flag);
    rpc_client->SendRequest(FLAGS_ip,
                            FLAGS_port,
                            static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_GET_GFLAG_REQ),
                            req,
                            resp,
                            ctx,
                            done_cb);
  } else if (FLAGS_method == "set") {
    proto::common::SetGFlagValueRequestPB req;
    auto resp = new proto::common::SetGFlagValueResponsePB();
    req.set_flag_name(FLAGS_flag);
    req.set_flag_value(FLAGS_value);
    rpc_client->SendRequest(FLAGS_ip,
                            FLAGS_port,
                            static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_SET_GFLAG_REQ),
                            req,
                            resp,
                            ctx,
                            done_cb);
  } else if (FLAGS_method == "list") {
    proto::common::ListAllGFlagsRequestPB req;
    auto resp = new proto::common::ListAllGFlagsResponsePB();
    rpc_client->SendRequest(FLAGS_ip,
                            FLAGS_port,
                            static_cast<sicl::rpc::ReqType>(simm::common::CommonRpcType::RPC_LIST_GFLAGS_REQ),
                            req,
                            resp,
                            ctx,
                            done_cb);
  } else {
    LOG_ERROR("Unknown method({}), should be get/set/list", FLAGS_method);
    usage();
    std::exit(EINVAL);
  }

  done_latch.wait();

  return 0;
}