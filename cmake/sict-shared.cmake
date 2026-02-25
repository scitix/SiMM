add_library(sict_shared_impl SHARED IMPORTED GLOBAL)

set_target_properties(sict_shared_impl PROPERTIES
  IMPORTED_LOCATION
    ${CMAKE_SOURCE_DIR}/third_party/sict/lib/libsict.so
  INTERFACE_INCLUDE_DIRECTORIES
    ${CMAKE_SOURCE_DIR}/third_party/sict/include
  IMPORTED_NO_SONAME TRUE
)
