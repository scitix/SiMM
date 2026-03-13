set(COMMON_PROTO_FILES
  ${CMAKE_CURRENT_SOURCE_DIR}/common.proto
)

set(DS_CM_PROTO_FILES
  ${CMAKE_CURRENT_SOURCE_DIR}/common.proto
  ${CMAKE_CURRENT_SOURCE_DIR}/ds_cm_rpcs.proto
)

set(DS_CLNT_PROTO_FILES
  ${CMAKE_CURRENT_SOURCE_DIR}/common.proto
  ${CMAKE_CURRENT_SOURCE_DIR}/ds_clnt_rpcs.proto
)

set(CM_CLNT_PROTO_FILES
  ${CMAKE_CURRENT_SOURCE_DIR}/common.proto
  ${CMAKE_CURRENT_SOURCE_DIR}/cm_clnt_rpcs.proto
)

set(GENERATED_PROTO_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated/proto)
file(MAKE_DIRECTORY ${GENERATED_PROTO_DIR})

protobuf_generate(
  #TARGET common_proto
  LANGUAGE cpp
  PROTOS ${COMMON_PROTO_FILES}
  OUT_VAR GENERATED_COMMON_RPCS
)
protobuf_generate(
  #TARGET ds_cm_proto
  LANGUAGE cpp
  PROTOS ${DS_CM_PROTO_FILES}
  OUT_VAR GENERATED_DS_CM_RPCS
)
protobuf_generate(
  #TARGET ds_clnt_proto
  LANGUAGE cpp
  PROTOS ${DS_CLNT_PROTO_FILES}
  OUT_VAR GENERATED_DS_CLNT_RPCS
)
protobuf_generate(
  #TARGET cm_clnt_proto
  LANGUAGE cpp
  PROTOS ${CM_CLNT_PROTO_FILES}
  OUT_VAR GENERATED_CM_CLNT_RPCS
)

add_library(common_proto   ${GENERATED_COMMON_RPCS})
add_library(ds_cm_proto   ${GENERATED_DS_CM_RPCS})
add_library(ds_clnt_proto ${GENERATED_DS_CLNT_RPCS})
add_library(cm_clnt_proto ${GENERATED_CM_CLNT_RPCS})
target_include_directories(common_proto  PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
target_include_directories(ds_cm_proto   PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
target_include_directories(ds_clnt_proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
target_include_directories(cm_clnt_proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(common_proto  PRIVATE protobuf)
target_link_libraries(ds_cm_proto   PRIVATE protobuf)
target_link_libraries(ds_clnt_proto PRIVATE protobuf)
target_link_libraries(cm_clnt_proto PRIVATE protobuf)
