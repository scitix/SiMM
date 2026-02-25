set(MODULE_NAME data_server)

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
list(FILTER MODULE_SRC_LIB EXCLUDE REGEX ".*_main.cc$")

add_executable(${MODULE_NAME} ${MODULE_SRC} ${MODULE_HEADERS})
target_link_libraries(${MODULE_NAME} PRIVATE
  gflags
  folly
  fmt
  protobuf
  ds_clnt_proto
  ds_cm_proto
  sict
  simm_common
)
# dataserver executable binary output path is build/${mode}/bin/
set_target_properties(${MODULE_NAME} PROPERTIES
  BUILD_RPATH "\$ORIGIN/../../../third_party/sict/lib"
  INSTALL_RPATH "\$ORIGIN/../../../third_party/sict/lib"
)

# **Caution** about sict, it maybe loaded by shared library at runtime, so
# the user who links data_server_static should also download sict shared library
# and set LD_LIBRARY_PATH correctly.
add_library(${MODULE_NAME}_static STATIC ${MODULE_SRC_LIB})
target_link_libraries(${MODULE_NAME}_static PRIVATE
  gflags
  ds_clnt_proto
  ds_cm_proto
  sict
  simm_common
)
