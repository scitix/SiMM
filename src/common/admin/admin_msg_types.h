#pragma once

#include <cstdint>

namespace simm {
namespace common {

// UDS socket base path constants.
// Socket path = <basePath>.<pid>.sock
inline constexpr const char* kCmAdminUdsBasePath = "/run/simm/admin_cm";
inline constexpr const char* kDsAdminUdsBasePath = "/run/simm/admin_ds";

// Shared message types for UDS admin protocol.
// Used by AdminServer (server side) and simm_ctl_admin (client side).
// Wire format: [uint32_t frame_len][uint16_t type][payload]
enum class AdminMsgType : uint16_t {
  TRACE_TOGGLE = 1,
  GFLAG_LIST   = 2,
  GFLAG_GET    = 3,
  GFLAG_SET    = 4,
  DS_STATUS    = 5,
  CM_STATUS    = 6,
  NODE_LIST    = 7,
  SHARD_LIST   = 8,
};

}  // namespace common
}  // namespace simm
