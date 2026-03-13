set(MODULE_NAME client)

file(GLOB MODULE_HEADERS
  "*.h"
  "*.hpp"
)

file(GLOB MODULE_SRC
  "*.cc"
  "*.hpp"
  "*.c"
  "*.cpp"
)

add_library(${MODULE_NAME}_static STATIC ${MODULE_SRC})
# PRIVATE means that the libraries are only used by this target and 
# not propagated to targets that link against this one.
#
# **Caution** about sict, it maybe loaded by shared library at runtime, so
# the user who links client_static should also download sict shared library
# and set LD_LIBRARY_PATH correctly.
target_link_libraries(${MODULE_NAME}_static PRIVATE
  gflags
  ds_clnt_proto
  cm_clnt_proto
  sict
  simm_common
  folly
)
