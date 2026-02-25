message(STATUS "UNIT_TEST: CORRECTNESS TESTS")

# Detect MPI Support
find_package(MPI REQUIRED)
if (MPI_FOUND)
  message(STATUS "MPI found.")
else ()
  message(WARNING "MPI not found. MPI-related tests might not compile or run correctly.")
endif ()

# Detect OpenMP Support
find_package(OpenMP REQUIRED)
if (OpenMP_FOUND)
  message(STATUS "OpenMP found: ${OpenMP_C_FLAGS} ${OpenMP_CXX_FLAGS}")
else ()
  message(WARNING "OpenMP not found. OpenMP features will be disabled for tests.")
endif ()

file(GLOB TEST_SOURCES CONFIGURE_DEPENDS
  "${CMAKE_CURRENT_SOURCE_DIR}/test_*.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/test_*.cpp"
)

foreach(test_file ${TEST_SOURCES})
  get_filename_component(test_name ${test_file} NAME_WE)
  add_executable(${test_name} ${test_file})
  target_compile_definitions(${test_name} PRIVATE BOOST_TIMER_ENABLE_DEPRECATED)
  target_link_libraries(${test_name} PRIVATE
    simm_common
    client_static
    gtest 
    gtest_main
    folly
  )

  # tests binary output path is build/${mode}/bin/unit_test by default
  set_target_properties(${test_name} PROPERTIES
    BUILD_RPATH "\$ORIGIN/../../../../third_party/sict/lib"
    INSTALL_RPATH "\$ORIGIN/../../../../third_party/sict/lib"
  )

  if (MPI_FOUND)
    target_link_libraries(${test_name} PRIVATE MPI::MPI_CXX)
  endif ()

  if (OpenMP_FOUND)
    target_link_libraries(${test_name} PRIVATE OpenMP::OpenMP_CXX)
  endif ()

  add_test(NAME ${test_name} COMMAND ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${test_name})
endforeach()
