# Collect trace sources to be consumed by simm_common
set(COMMON_TRACE_SRC
  "${CMAKE_CURRENT_SOURCE_DIR}/trace.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/trace_shm.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/trace_flags.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/trace_server.cc"
  PARENT_SCOPE)

# Expose local headers to consumers (simm_common will add this include dir)
set(COMMON_TRACE_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR} PARENT_SCOPE)

# Analyzer executable links directly against simm_common (which now includes trace)
if (ENABLE_TRACE)
  add_executable(simm_trace_analyzer
    "${CMAKE_CURRENT_SOURCE_DIR}/main.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/analyzer.cc")
  set_target_properties(simm_trace_analyzer PROPERTIES CXX_STANDARD 20)
  target_include_directories(simm_trace_analyzer PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
  target_link_libraries(simm_trace_analyzer PRIVATE simm_common)
endif()
