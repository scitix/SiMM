add_subdirectory(errcode)
add_subdirectory(flags)
add_subdirectory(logging)
add_subdirectory(trace)
add_subdirectory(rpc_handlers)
add_subdirectory(utils)
add_subdirectory(version)
add_subdirectory(hashkit)
add_subdirectory(metrics)

set(SIMM_COMMON_SOURCES
    ${ERRCODE_SRC}
    ${COMMON_FLAG_SRC}
    ${COMMON_LOGGING_SRC}
    ${COMMON_TRACE_SRC}
    ${COMMON_RPC_HANDLERS_SRC}
    ${COMMON_UTILS_SRC}
    ${COMMON_HASHKIT_SRC}
    ${VERSION_SRC}
    ${COMMON_METRICS_SRC}
)

add_library(simm_common STATIC ${SIMM_COMMON_SOURCES})
# link default dependencies
target_link_libraries(simm_common PRIVATE fmt folly acl curl stdc++fs Threads::Threads rt dl prometheus-cpp::pull)
