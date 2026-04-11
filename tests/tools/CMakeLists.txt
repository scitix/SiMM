message(STATUS "UNIT_TEST:${UNIT_TEST}")

file(GLOB TEST_SOURCES CONFIGURE_DEPENDS
  "${CMAKE_CURRENT_SOURCE_DIR}/test_*.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/test_*.cpp"
)

foreach(test_file ${TEST_SOURCES})
  get_filename_component(test_name ${test_file} NAME_WE)
  add_executable(${test_name} ${test_file})
  target_include_directories(${test_name} PRIVATE ${CMAKE_SOURCE_DIR})
  target_link_libraries(${test_name} PRIVATE
    gtest
    gtest_main)
  add_test(NAME ${test_name} COMMAND ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${test_name})
endforeach()
