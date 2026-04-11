set(MODULE_NAME cluster_manager)

file(GLOB MODULE_HEADERS 
  "*.h" 
  "*.hpp"
)

file(GLOB MODULE_SRC 
  "*.cc" 
  "*.hpp" 
  "*.c" 
  "*.cpp")

set(MODULE_SRC_LIB ${MODULE_SRC})
list(REMOVE_ITEM MODULE_SRC_LIB 
  "${CMAKE_CURRENT_SOURCE_DIR}/cm_main.cc"
)

set(DS_SHARED_FLAG_SRC
  "${PROJECT_SOURCE_DIR}/src/data_server/ds_flags.cc"
)

add_executable(${MODULE_NAME} ${MODULE_SRC} ${MODULE_HEADERS} ${DS_SHARED_FLAG_SRC})
target_link_libraries(${MODULE_NAME} PRIVATE 
  gflags
  folly
  fmt
  protobuf
  ds_cm_proto
  cm_clnt_proto
  sict
  simm_common)
# clustermanager executable binary output path is build/${mode}/bin/
set_target_properties(${MODULE_NAME} PROPERTIES
  BUILD_RPATH "\$ORIGIN/../../../third_party/sict/lib"
  INSTALL_RPATH "\$ORIGIN/../../../third_party/sict/lib"
)

add_library(${MODULE_NAME}_static STATIC ${MODULE_SRC_LIB} ${DS_SHARED_FLAG_SRC})
# PRIVATE means that the libraries are only used by this target and 
# not propagated to targets that link against this one.
#
# **Caution** about sict, it maybe loaded by shared library at runtime, so
# the user who links cluster_manager_static should also download sict shared library
# and set LD_LIBRARY_PATH correctly.
target_link_libraries(${MODULE_NAME}_static PRIVATE
  gflags
  ds_cm_proto
  cm_clnt_proto
  sict
  simm_common
)

if(ENABLE_TESTS)
  target_compile_definitions(${MODULE_NAME}_static PRIVATE SIMM_UNIT_TEST)
  target_include_directories(${MODULE_NAME}_static PRIVATE
    ${PROJECT_SOURCE_DIR}/third_party/gtest/googletest/include
  )
endif()
