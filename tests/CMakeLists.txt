enable_testing()

file(GLOB_RECURSE UNIT_TEST_SOURCES CONFIGURE_DEPENDS "*.cc")
message(STATUS "Found unit test files: ${UNIT_TEST_SOURCES}")

file(MAKE_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unit_tests)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unit_tests)

add_subdirectory(common)
add_subdirectory(client)
add_subdirectory(cluster_manager)
add_subdirectory(data_server)
add_subdirectory(tools)
add_subdirectory(correctness)
