add_library(sict_static_impl STATIC IMPORTED GLOBAL)

set_target_properties(sict_static_impl PROPERTIES
  IMPORTED_LOCATION
    ${CMAKE_SOURCE_DIR}/third_party/scit/lib/libsict_static.a
  INTERFACE_INCLUDE_DIRECTORIES
    ${CMAKE_SOURCE_DIR}/third_party/sict/include
)
