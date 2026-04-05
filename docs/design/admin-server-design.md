# AdminServer Design Document

## 1. Overview

AdminServer is a UDS (Unix Domain Socket) based admin interface embedded in CM and DS processes. It provides a local channel for querying process-internal state (gflags, trace, DS heartbeat status, etc.) without going through the RDMA-based SiCL RPC stack.

The design uses RAII lifecycle, self-pipe shutdown, and handler registration by the owning service.

## 2. Motivation

- **SiCL is RDMA-only**: The existing admin RPC service (`admin_rpc_service_`) runs on SiCL, which requires RDMA hardware. UDS works on any Linux system.
- **Local observability**: DS internal state (`is_registered`, `cm_ready`, `heartbeat_failure_count`) was previously inaccessible from outside the process. AdminServer exposes it via a simple local protocol.
- **Test framework integration**: The cluster integration test framework needs to query DS state for protocol correctness verification. UDS + `simm_ctl_admin --pid` provides a non-invasive path.

## 3. Architecture

```
┌──────────────────────────────────────────────┐
│  simm_ctl_admin (client)                     │
│  --pid 12345 ds status     │
└──────────────┬───────────────────────────────┘
               │  UDS: /run/simm/simm_ds.12345.sock
               ▼
┌──────────────────────────────────────────────┐
│  AdminServer (inside DS process)             │
│                                              │
│  ServeLoop (background thread)               │
│    poll(listen_fd, shutdown_pipe)             │
│    accept → read frame → dispatch → respond  │
│                                              │
│  Handlers:                                   │
│    GFLAG_LIST/GET/SET  (built-in)            │
│    TRACE_TOGGLE        (built-in)            │
│    DS_STATUS           (registered by DS)     │
└──────────────────────────────────────────────┘
```

CM has the same structure, with its own socket (`simm_cm.<pid>.sock`) and CM-specific handlers registered by `ClusterManagerService`.

## 4. Socket Naming

```
<basePath>.<pid>.sock
```

Constructor takes a `basePath` parameter (same pattern as TraceServer). The socket path is `<basePath>.<pid>.sock`.

| Component | `basePath` | Example |
|-----------|-----------|---------|
| CM | `/run/simm/simm_cm` | `/run/simm/simm_cm.12345.sock` |
| DS | `/run/simm/simm_ds` | `/run/simm/simm_ds.67890.sock` |


## 5. Wire Protocol

Identical to the existing TraceServer UDS protocol for backward compatibility.

### Request frame

```
[uint32_t frame_len (network byte order)]
[uint16_t msg_type  (network byte order)]
[payload bytes ...]
```

`frame_len` = `sizeof(msg_type) + payload_size`

### Response frame

Same format. `msg_type` in response echoes the request type.

### Message Types (AdminMsgType)

```cpp
enum class AdminMsgType : uint16_t {
  TRACE_TOGGLE = 1,   // toggle IO tracing on/off
  GFLAG_LIST   = 2,   // list all gflags
  GFLAG_GET    = 3,   // get a single gflag value
  GFLAG_SET    = 4,   // set a single gflag value
  DS_STATUS    = 5,   // query DS internal status
  CM_STATUS    = 6,   // query CM internal status
};
```

Defined in `src/common/admin/admin_msg_types.h`. The same enum is duplicated in `simm_ctl_admin.cc` and `trace_server.cc` (with a comment to keep in sync).

## 6. AdminServer Lifecycle

AdminServer uses RAII — no explicit `Start()`/`Stop()`.

### Construction

```cpp
AdminServer::AdminServer(std::string basePath)
    : basePath_(std::move(basePath)), listenFd_(-1), running_(false) {
  // 1. Derive and mkdir base directory from basePath_
  // 2. socketPath_ = basePath_ + "." + pid + ".sock"
  // 3. unlink(socketPath_)  -- remove stale socket
  // 4. pipe(shutdownPipe_)  -- self-pipe for clean shutdown
  // 5. socket(AF_UNIX) + bind() + listen()
  // 6. Register built-in handlers (gflag, trace)
  // 7. running_ = true, spawn serve thread
}
```

### Serve Loop

```cpp
void AdminServer::ServeLoop() {
  while (running_) {
    poll(listen_fd, shutdown_pipe[0]);  // block until event
    if (shutdown_pipe triggered) break;
    if (listen_fd readable) {
      client_fd = accept();
      HandleClient(client_fd);   // read frame → dispatch → respond
      close(client_fd);
    }
  }
}
```

The self-pipe trick (write a byte to `shutdown_pipe_[1]`) cleanly wakes the blocking `poll()` during shutdown.

### Destruction

```cpp
AdminServer::~AdminServer() {
  Shutdown();
}

void AdminServer::Shutdown() {
  running_ = false;
  write(shutdown_pipe_[1], "x");  // wake serve loop
  worker_.join();                 // wait for thread exit
  close(listen_fd_);
  close(shutdown_pipe_[0/1]);
  unlink(socket_path_);           // remove socket file
  handlers_.clear();
}
```

## 7. Handler Registration

### Built-in Handlers

Registered automatically in the constructor. Available for all processes (CM and DS):

| Type | Request PB | Response PB | Source |
|------|-----------|------------|--------|
| `GFLAG_LIST` | `ListAllGFlagsRequestPB` | `ListAllGFlagsResponsePB` | gflags API |
| `GFLAG_GET` | `GetGFlagValueRequestPB` | `GetGFlagValueResponsePB` | gflags API |
| `GFLAG_SET` | `SetGFlagValueRequestPB` | `SetGFlagValueResponsePB` | gflags API |
| `TRACE_TOGGLE` | `TraceToggleRequestPB` | `TraceToggleResponsePB` | TraceManager |

### Custom Handler Registration

Services register their handlers after construction via `RegisterHandler()`:

```cpp
// In KVRpcService::RegisterAdminHandlers()
admin_server->RegisterHandler(
    AdminMsgType::DS_STATUS,
    [this](const std::string& payload) -> std::string {
      DsStatusResponsePB resp;
      resp.set_ret_code(CommonErr::OK);
      resp.set_is_registered(is_registered_.load());
      resp.set_cm_ready(cm_ready_.load());
      resp.set_heartbeat_failure_count(heartbeat_failure_count_.load());
      std::string buf;
      resp.SerializeToString(&buf);
      return buf;
    });
```

The lambda captures `this`, giving the handler access to the service's internal state without requiring friend declarations or public getters.

## 8. Integration with CM and DS

### CM Integration (`cm_main.cc`)

```
1. Parse flags, init logging
2. Create AdminServer(ResolveAdminName())     ← early, before signals
3. Register signal handlers (SIGINT/SIGTERM)
4. Create and Start ClusterManagerService
5. cm_service->RegisterAdminHandlers(admin_server)
6. Main loop (wait for quit signal)
7. admin_server destructor → Shutdown()       ← automatic on scope exit
```

`ClusterManagerService::RegisterAdminHandlers()` is a centralized function for registering all CM-specific admin handlers. Currently a placeholder for future CM status queries.

### DS Integration (`kv_server_main.cc`)

```
1. Parse flags, init logging
2. Create AdminServer(ResolveAdminName())     ← early, before signals
3. Register signal handlers
4. Create, Init, Start KVRpcService
5. server->RegisterAdminHandlers(admin_server)
6. Main loop (wait for quit signal)
7. admin_server destructor → Shutdown()       ← automatic on scope exit
```

`KVRpcService::RegisterAdminHandlers()` registers the `DS_STATUS` handler.

### Ownership

AdminServer is owned by `main()` (as a `unique_ptr`). Services receive a raw pointer for handler registration only. The AdminServer outlives the service — it is created before and destroyed after the service.

## 9. DS_STATUS Message

### Purpose

Expose DS-internal heartbeat protocol state for observability and testing.

### Proto Definition (`common.proto`)

```protobuf
message DsStatusRequestPB {
  // empty
}

message DsStatusResponsePB {
  sint32 ret_code = 1;
  bool is_registered = 2;            // DS registered with CM
  bool cm_ready = 3;                 // DS considers CM reachable
  uint32 heartbeat_failure_count = 4; // consecutive HB failure counter
}
```

### Fields

| Field | Source | Meaning |
|-------|--------|---------|
| `is_registered` | `KVRpcService::is_registered_` | `true` after successful handshake with CM |
| `cm_ready` | `KVRpcService::cm_ready_` | `false` when consecutive HB failures reach `cm_hb_tolerance_count` |
| `heartbeat_failure_count` | `KVRpcService::heartbeat_failure_count_` | Resets to 0 on successful HB or re-registration |

### CLI Usage

```bash
simm_ctl_admin --pid 12345 ds status
```

Output:
```
+----------------------------+-------+
| Field                      | Value |
+----------------------------+-------+
| is_registered              | true  |
| cm_ready                   | true  |
| heartbeat_failure_count    | 0     |
+----------------------------+-------+
```

## 10. Relationship to Existing Components

### vs TraceServer

| Aspect | TraceServer | AdminServer |
|--------|-------------|-------------|
| Location | `src/common/trace/` | `src/common/admin/` |
| Socket path | `/run/simm/simm_trace.<pid>.sock` | `/run/simm/simm_<role>.<pid>.sock` |
| Dispatch | `switch` hardcoded in `serveLoop` | `handlers_` map, external registration |
| Used by | Client only | CM and DS |
| Shutdown | Manual `stop()` in destructor | Self-pipe + RAII destructor |

TraceServer continues to serve the Client process. AdminServer is used by CM and DS. They share the same wire protocol and `AdminMsgType` enum, so `simm_ctl_admin` can talk to both.

### vs Admin RPC Service (`admin_rpc_service_`)

The SiCL-based admin RPC service (`ds_rpc_admin_port` / `cm_rpc_admin_port`) remains for remote admin operations (node list, shard list, set node status). AdminServer handles local-only queries that do not need RDMA transport.

## 11. File List

| File | Type | Description |
|------|------|-------------|
| `src/common/admin/admin_msg_types.h` | New | `AdminMsgType` enum |
| `src/common/admin/admin_server.h` | New | `AdminServer` class declaration |
| `src/common/admin/admin_server.cc` | New | Implementation: RAII lifecycle, serve loop, built-in handlers |
| `src/proto/common.proto` | Modified | `DsStatusRequestPB`, `DsStatusResponsePB` |
| `src/cluster_manager/cm_main.cc` | Modified | Create AdminServer early, call `RegisterAdminHandlers` |
| `src/cluster_manager/cm_service.h/.cc` | Modified | Add `RegisterAdminHandlers()` |
| `src/data_server/kv_server_main.cc` | Modified | Create AdminServer early, call `RegisterAdminHandlers` |
| `src/data_server/kv_rpc_service.h/.cc` | Modified | Add `RegisterAdminHandlers()` with DS_STATUS lambda |
| `tools/simm_ctl_admin.cc` | Modified | `ds status` via UdsChannel; `--name` flag; `DS_STATUS=5` |
| `src/common/trace/trace_server.cc` | Modified | `DS_STATUS=5` added to local enum |
