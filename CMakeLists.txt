#
# Copyright (c) 2026, Scitix Tech PTE. LTD. All rights reserved.
#

cmake_minimum_required(VERSION 3.22)

project(simm 
        VERSION 0.1.0
        DESCRIPTION "Scitix In-Memory Middleware" 
        LANGUAGES CXX C)
message(STATUS "CMAKE_CXX_COMPILER_ID: ${CMAKE_CXX_COMPILER_ID}")

# All 4 build modes - Release/Debug/MinSizeRel/RelWithDebInfo
# Default is RelWithDebInfo
set(CMAKE_CONFIGURATION_TYPES "RelWithDebInfo;Debug;Release;MinSizeRel" CACHE STRING "" FORCE)
if (NOT CMAKE_BUILD_TYPE OR CMAKE_BUILD_TYPE STREQUAL "None")
    set (CMAKE_BUILD_TYPE "RelWithDebInfo")
    message (STATUS "CMAKE_BUILD_TYPE not set, set default = ${CMAKE_BUILD_TYPE}")
endif ()
message (STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")

# check dependent packages are ready
find_package(gflags REQUIRED)
find_package(Protobuf REQUIRED)
find_package(Git REQUIRED)
find_package(prometheus-cpp CONFIG REQUIRED)

# generate build infos, such as git commit hash, build timestamp, etc.
if(NOT GIT_FOUND)
    message(FATAL_ERROR "Git not found, please install git")
endif()
execute_process(
    COMMAND ${GIT_EXECUTABLE} rev-parse --short=8 HEAD
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
    OUTPUT_VARIABLE SIMM_GIT_COMMIT_SHORT_HASH_CMAKE
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
execute_process(
    COMMAND ${GIT_EXECUTABLE} rev-parse HEAD
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
    OUTPUT_VARIABLE SIMM_GIT_COMMIT_FULL_HASH_CMAKE
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
execute_process(
    COMMAND date "+%Y-%m-%d %H:%M:%S"
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
    OUTPUT_VARIABLE SIMM_BUILD_TIMESTAMP_CMAKE
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
set(SIMM_BUILD_TYPE_CMAKE ${CMAKE_BUILD_TYPE})

# Generate header files
configure_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/include/version/version.h.in
    ${CMAKE_CURRENT_BINARY_DIR}/include/version/version.h
)

# set c/c++ standards
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED OFF)
set(CMAKE_C_EXTENSIONS ON)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# user-defined options
option(ENABLE_ASAN "enable -fsanitize=address option" OFF)
option(ENABLE_TESTS "Build tests' binary libraries" OFF)
option(ENABLE_APIPERF "Enable SiMM API perf codes" OFF)
option(ENABLE_METRICS "Enable SiMM request metrics collection" OFF)
option(ENABLE_TRACE "Enable SiMM request tracing" OFF)

if(ENABLE_METRICS)
    message(STATUS "Enable SiMM request metrics collection ...")
    add_compile_definitions(SIMM_ENABLE_METRICS)
endif()

if(ENABLE_TRACE)
    message(STATUS "Enable SiMM request tracing ...")
    add_compile_definitions(SIMM_ENABLE_TRACE)
endif()

option(SICT_USE_SHARED "Use sict netowrk shared library" ON)
if(SICL_STATIC_LIBRARY)
    message(STATUS "Link sict network library in static lib...")
    set(SICT_USE_SHARED OFF CACHE BOOL "" FORCE)
endif()

# current version depends on sict network library
include(cmake/sict-config.cmake)

# FIXME(ytji): disable -Werror temporarily, because it will break build of folly submodule
#set(CMAKE_CXX_FLAGS "${CMAKE_CPP_FLAGS} -W -Wall -Wextra -Werror -Wpedantic -pipe -fPIC -fno-omit-frame-pointer")
set(CMAKE_CXX_FLAGS "${CMAKE_CPP_FLAGS} -W -Wall -Wextra -Wpedantic -pipe -fPIC -fno-omit-frame-pointer")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -fno-inline")
if (ENABLE_ASAN)
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address")
    message(STATUS "Enable -fsanitize=address option")
endif()
#set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O2 -DNDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE "-O2 -DNDEBUG")
message(STATUS "CMAKE_CXX_FLAGS: ${CMAKE_CXX_FLAGS}")
message(STATUS "CMAKE_CXX_FLAGS_DEBUG: ${CMAKE_CXX_FLAGS_DEBUG}")
message(STATUS "CMAKE_CXX_FLAGS_RELEASE: ${CMAKE_CXX_FLAGS_RELEASE}")
message(STATUS "CMAKE_CXX_FLAGS_RELWITHDEBINFO: ${CMAKE_CXX_FLAGS_RELWITHDEBINFO}")

# set build output directories
string(TOLOWER "${CMAKE_BUILD_TYPE}" BUILD_TYPE_SUBDIR)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${PROJECT_SOURCE_DIR}/build/${BUILD_TYPE_SUBDIR}/bin")
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${PROJECT_SOURCE_DIR}/build/${BUILD_TYPE_SUBDIR}/lib")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${PROJECT_SOURCE_DIR}/build/${BUILD_TYPE_SUBDIR}/lib")

# set include directories
include_directories(${PROJECT_SOURCE_DIR}/src
                    ${PROJECT_SOURCE_DIR}/include
                    ${CMAKE_CURRENT_BINARY_DIR}/include
                    ${CMAKE_CURRENT_BINARY_DIR}/third_party/folly/
                    ${CMAKE_CURRENT_BINARY_DIR}/src/
                    ${GFLAGS_INCLUDE_PATH}
                    ${PROJECT_SOURCE_DIR}/third_party/concurrentqueue/
                    ${PROJECT_SOURCE_DIR}/third_party/folly/
                    ${PROJECT_SOURCE_DIR}/third_party/fmt/include/
                    # need to run sict download script firstly to get sict .so and header files
                    ${PROJECT_SOURCE_DIR}/third_party/sict/include/
                    )

set(CMAKE_PREFIX_PATH "${CMAKE_PREFIX_PATH};${PROJECT_SOURCE_DIR}/third_party/fast_float/include/")

# set GNU SOURCE to fix folly dependency
add_compile_definitions(_GNU_SOURCE)

# build third-party libraries
add_subdirectory("third_party/fmt" EXCLUDE_FROM_ALL)
add_subdirectory("third_party/gtest" EXCLUDE_FROM_ALL)
set(FOLLY_NO_EXCEPTION_TRACER ON)
add_subdirectory("third_party/folly" EXCLUDE_FROM_ALL)
message(STATUS "CMAKE_CURRENT_SOURCE_DIR: ${CMAKE_CURRENT_SOURCE_DIR}")
message(STATUS "CMAKE_SOURCE_DIR: ${CMAKE_SOURCE_DIR}")

if(ENABLE_APIPERF)
    message(STATUS "Enable SiMM APIs perf codes...")
    add_compile_definitions(SIMM_APIPERF)
endif()

add_subdirectory(src)
add_subdirectory(tools)
# if build tests' binary
if(ENABLE_TESTS)
    message(STATUS "Building tests ...")
    add_compile_definitions(SIMM_UNIT_TEST)
    add_subdirectory(tests)
endif()

# if build python and cython module
option(BUILD_PYTHON "build python and cython module or not" ON)
if(${BUILD_PYTHON})
    find_package(Python COMPONENTS Interpreter Development REQUIRED)
    add_subdirectory(python)
endif()
