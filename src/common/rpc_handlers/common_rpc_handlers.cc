#include <gflags/gflags.h>
#include <string>

#include "common/errcode/errcode_def.h"
#include "common/logging/logging.h"
#include "common/rpc_handlers/common_rpc_handlers.h"
#include "common/trace/trace.h"
#include "proto/common.pb.h"

// FIXME(ytji): how to decide which log module to logging
// then use the MLOG_XXX() interfaces
#define SEND_RESPONSE(ctx, resp)                                                                                     \
  conn->SendResponse(*resp, ctx, [](std::shared_ptr<sicl::rpc::RpcContext> ctx) {                                    \
    if (ctx->ErrorCode() != sicl::transport::SICL_SUCCESS) {                                                         \
      LOG_ERROR(                                                                                                     \
          "Failed to send response, func:{} err_msg:{}, err_code:{}", __func__, ctx->ErrorText(), ctx->ErrorCode()); \
    } else {                                                                                                         \
      LOG_INFO("{}: Response sent successfully", __func__);                                                          \
    }                                                                                                                \
  });

namespace simm {
namespace common {

void GetGFlagHandler::Work(const std::shared_ptr<sicl::rpc::RpcContext> ctx,
                           const std::shared_ptr<sicl::rpc::Connection> conn,
                           const google::protobuf::Message *request) const {
  auto req = dynamic_cast<const proto::common::GetGFlagValueRequestPB *>(request);
  auto resp = std::make_shared<proto::common::GetGFlagValueResponsePB>();
  gflags::CommandLineFlagInfo info;
  if (!gflags::GetCommandLineFlagInfo(req->flag_name().c_str(), &info)) {
    // TODO(ytji): add error log
    resp->set_ret_code(CommonErr::GFlagNotFound);
    SEND_RESPONSE(ctx, resp);
    return;
  }
  resp->set_ret_code(CommonErr::OK);
  auto *resp_gflag_info = resp->mutable_flag_info();
  resp_gflag_info->set_flag_name(info.name);
  resp_gflag_info->set_flag_value(info.current_value);
  resp_gflag_info->set_flag_default_value(info.default_value);
  resp_gflag_info->set_flag_type(info.type);
  resp_gflag_info->set_flag_description(info.description);
  SEND_RESPONSE(ctx, resp);
}

void SetGFlagHandler::Work(const std::shared_ptr<sicl::rpc::RpcContext> ctx,
                           const std::shared_ptr<sicl::rpc::Connection> conn,
                           const google::protobuf::Message *request) const {
  auto req = dynamic_cast<const proto::common::SetGFlagValueRequestPB *>(request);
  auto resp = std::make_shared<proto::common::SetGFlagValueResponsePB>();
  gflags::CommandLineFlagInfo info;
  if (!gflags::GetCommandLineFlagInfo(req->flag_name().c_str(), &info)) {
    // TODO(ytji): add error log
    resp->set_ret_code(CommonErr::GFlagNotFound);
    SEND_RESPONSE(ctx, resp);
    return;
  }
  resp->set_ret_code(CommonErr::OK);
  auto res = gflags::SetCommandLineOption(req->flag_name().c_str(), req->flag_value().c_str());
  // if set failed, SetCommandLineOption will return empty string
  if (res.empty()) {
    // TODO(ytji): add error log
    resp->set_ret_code(CommonErr::GFlagSetFailed);
  }
  SEND_RESPONSE(ctx, resp);
}

void ListGFlagsHandler::Work(const std::shared_ptr<sicl::rpc::RpcContext> ctx,
                             const std::shared_ptr<sicl::rpc::Connection> conn,
                             [[maybe_unused]] const google::protobuf::Message *request) const {
  // auto req = dynamic_cast<const proto::common::ListAllGFlagsRequestPB *>(request);
  auto resp = std::make_shared<proto::common::ListAllGFlagsResponsePB>();
  std::vector<gflags::CommandLineFlagInfo> all_flags;
  gflags::GetAllFlags(&all_flags);
  for (const auto &f : all_flags) {
    auto *resp_flag = resp->add_flags();
    resp_flag->set_flag_name(f.name);
    resp_flag->set_flag_value(f.current_value);
    resp_flag->set_flag_default_value(f.default_value);
    resp_flag->set_flag_type(f.type);
    resp_flag->set_flag_description(f.description);
  }
  resp->set_ret_code(CommonErr::OK);
  SEND_RESPONSE(ctx, resp);
}


#ifdef SIMM_ENABLE_TRACE
void TraceToggleHandler::Work(const std::shared_ptr<sicl::rpc::RpcContext> ctx,
                             const std::shared_ptr<sicl::rpc::Connection> conn,
                             [[maybe_unused]] const google::protobuf::Message *request) const {
  // auto req = dynamic_cast<const proto::common::ListAllGFlagsRequestPB *>(request);
  auto resp = std::make_shared<proto::common::TraceToggleResponsePB>();
  auto req = dynamic_cast<const proto::common::TraceToggleRequestPB *>(request);

  simm::trace::TraceManager::SetEnabled(req->enable_trace());

  std::cerr << "DEBUG: TraceToggleHandler set enable_trace to " << req->enable_trace() << std::endl;
  resp->set_ret_code(CommonErr::OK);
  SEND_RESPONSE(ctx, resp);
}
#endif

}  // namespace common
}  // namespace simm