message(STATUS "UNIT_TEST:${UNIT_TEST}")

file(GLOB TEST_SOURCES CONFIGURE_DEPENDS
  "${CMAKE_CURRENT_SOURCE_DIR}/test_*.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/test_*.cpp"
)

foreach(test_file ${TEST_SOURCES})
  get_filename_component(test_name ${test_file} NAME_WE)
  add_executable(${test_name} ${test_file})
  target_link_libraries(${test_name} PRIVATE 
    simm_common
    data_server_static
    ds_clnt_proto
    gtest
    gtest_main
    folly)
  # tests binary output path is build/${mode}/bin/unit_test
  set_target_properties(${test_name} PROPERTIES
    BUILD_RPATH "\$ORIGIN/../../../../third_party/sict/lib"
    INSTALL_RPATH "\$ORIGIN/../../../../third_party/sict/lib"
  )
  # if(${test_name} MATCHES "^proto_")
  # if(${test_name} MATCHES ".*_proto$")
  # if(${test_name} MATCHES ".*proto.*" OR ${test_name} STREQUAL "special_test")
  # if(${test_name} MATCHES ".*(proto|rpc|meta).*")
  #if(${test_name} MATCHES "test_cm_service.cc")
  #    target_link_libraries(${test_name} PRIVATE 
  #        ds_cm_proto
  #        cm_clnt_proto)
  #endif()
  add_test(NAME ${test_name} COMMAND ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${test_name})
endforeach()
