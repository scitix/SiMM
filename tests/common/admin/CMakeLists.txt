file(GLOB TEST_SOURCES CONFIGURE_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/test_*.cc"
)

foreach(test_file ${TEST_SOURCES})
    get_filename_component(test_name ${test_file} NAME_WE)
    add_executable(${test_name} ${test_file})
    target_link_libraries(${test_name} PRIVATE
        simm_common
        common_proto
        gtest
        gtest_main
        folly)
    set_target_properties(${test_name} PROPERTIES
        BUILD_RPATH "\$ORIGIN/../../../../third_party/sict/lib"
        INSTALL_RPATH "\$ORIGIN/../../../../third_party/sict/lib"
    )
    add_test(NAME ${test_name} COMMAND ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${test_name})
endforeach()
