set(MODULE_NAME simm_tools)

set(TOOLS_COMMON_LIBS simm_common gflags folly fmt)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/tools")

set(FLAG_ADMIN_TOOL "simm_flags_admin.cc")
get_filename_component(FLAG_MAIN_TOOL_NAME ${FLAG_ADMIN_TOOL} NAME_WE)
add_executable(${FLAG_MAIN_TOOL_NAME} ${FLAG_ADMIN_TOOL})
target_link_libraries(${FLAG_MAIN_TOOL_NAME} PRIVATE
  ${TOOLS_COMMON_LIBS}
  common_proto
  sict
  protobuf)
# tools binary output path is build/${mode}/bin/tools
set_target_properties(${FLAG_MAIN_TOOL_NAME} PROPERTIES
  BUILD_RPATH "\$ORIGIN/../../../../third_party/sict/lib"
  INSTALL_RPATH "\$ORIGIN/../../../../third_party/sict/lib"
)

set(KVIO_BENCH_TOOL "simm_kvio_bench.cc")
get_filename_component(KVIO_BENCH_TOOL_NAME ${KVIO_BENCH_TOOL} NAME_WE)
add_executable(${KVIO_BENCH_TOOL_NAME} ${KVIO_BENCH_TOOL})
target_link_libraries(${KVIO_BENCH_TOOL_NAME} PRIVATE
  ${TOOLS_COMMON_LIBS}
  client_static)
# tools binary output path is build/${mode}/bin/tools
set_target_properties(${KVIO_BENCH_TOOL_NAME} PROPERTIES
  BUILD_RPATH "\$ORIGIN/../../../../third_party/sict/lib"
  INSTALL_RPATH "\$ORIGIN/../../../../third_party/sict/lib"
)

set(KVIO_BENCH_V2_TOOL "simm_kvio_bench_v2.cc")
get_filename_component(KVIO_BENCH_V2_TOOL_NAME ${KVIO_BENCH_V2_TOOL} NAME_WE)
add_executable(${KVIO_BENCH_V2_TOOL_NAME} ${KVIO_BENCH_V2_TOOL})
target_link_libraries(${KVIO_BENCH_V2_TOOL_NAME} PRIVATE
  ${TOOLS_COMMON_LIBS}
  client_static)
set_target_properties(${KVIO_BENCH_V2_TOOL_NAME} PROPERTIES
  BUILD_RPATH "\$ORIGIN/../../../../third_party/sict/lib"
  INSTALL_RPATH "\$ORIGIN/../../../../third_party/sict/lib"
)

set(KVIO_STABLE_TEST "simm_stable_test.cc")
get_filename_component(KVIO_STABLE_TEST_NAME ${KVIO_STABLE_TEST} NAME_WE)
add_executable(${KVIO_STABLE_TEST_NAME} ${KVIO_STABLE_TEST})
target_link_libraries(${KVIO_STABLE_TEST_NAME} PRIVATE
  ${TOOLS_COMMON_LIBS}
  client_static)
# tools binary output path is build/${mode}/bin/tools
set_target_properties(${KVIO_STABLE_TEST_NAME} PROPERTIES
  BUILD_RPATH "\$ORIGIN/../../../../third_party/sict/lib"
  INSTALL_RPATH "\$ORIGIN/../../../../third_party/sict/lib"
)

set(CTL_ADMIN_TOOL "simm_ctl_admin.cc")
set(CTL_ADMIN_TOOL_NAME simmctl)
add_executable(${CTL_ADMIN_TOOL_NAME} ${CTL_ADMIN_TOOL})
target_link_libraries(${CTL_ADMIN_TOOL_NAME} PRIVATE
  ${TOOLS_COMMON_LIBS}
  common_proto
  cm_clnt_proto
  sict
  protobuf)
# tools binary output path is build/${mode}/bin/tools
set_target_properties(${CTL_ADMIN_TOOL_NAME} PROPERTIES
  BUILD_RPATH "\$ORIGIN/../../../../third_party/sict/lib"
  INSTALL_RPATH "\$ORIGIN/../../../../third_party/sict/lib"
)
