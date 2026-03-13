#!/bin/bash
#
# Copyright (c) 2026, Scitix Tech PTE. LTD. All rights reserved.
#

ROOT_DIR=$(cd $(dirname $0) && pwd)
BUILD_DIR=$ROOT_DIR/build
CLEAN_BUILD="false"
PY_VER="3.10"

usage() {
  echo "  usage: ./build.sh [OPTIONS]"
  echo "  OPTIONS:"
  echo "    --mode xxx                Build mode, pick one: release | debug | relwithdeb | minsizerel"
  echo "    --py_ver xxx              Python version, pick one: 3.10 | 3.12"
  echo "    --clean                   clean build"
  echo "    --test                    Also build unit tests for specified mode"
  echo "    --metric                  Enable SiMM metrics collection, default off"
  echo "    --trace                   Enable SiMM tracing, default off"
  echo "    --apiperf                 enable perf codes in SiMM apis"
  echo "    --sictstatics             use sict static libraries, default OFF"
  echo "    --verbose                 Verbose build"
  echo "    -h, --help                Show this help message"
  echo

  exit 0
}

echo_message() {
  local level=$1
  local message=$2

  case "$level" in
    INFO)
      echo -e "\033[32m[INFO]:\033[0m $message"    # green
      ;;
    ERROR)
      echo -e "\033[31m[ERROR]:\033[0m $message"   # red
      ;;
    WARNING)
      echo -e "\033[33m[WARNING]:\033[0m $message" # yellow
      ;;
    *)
      echo -e "\033[37m[UNKNOWN]:\033[0m $message" # gray
      ;;
  esac
}

ARGS=$(getopt -l \
mode:,\
py_ver:,\
clean,\
test,\
metric,\
trace,\
apiperf,\
sictstatics,\
verbose,\
help -o h -- "$@") || usage

eval set -- "$ARGS"
while [ -n "$1" ]; do
  case "$1" in
  --mode)
    shift
    if [[ "$1" == "release" ]]; then
      BUILD_TYPE="release"
      CMAKEARGS+="-DCMAKE_BUILD_TYPE=Release "
    elif [[ "$1" == "debug" ]]; then
      BUILD_TYPE="debug"
      CMAKEARGS+="-DCMAKE_BUILD_TYPE=Debug "
    elif [[ "$1" == "relwithdeb" ]]; then
      BUILD_TYPE="relwithdeb"
      CMAKEARGS+="-DCMAKE_BUILD_TYPE=RelWithDebInfo "
    elif [[ "$1" == "minsizerel" ]]; then
      BUILD_TYPE="minsizerel"
      CMAKEARGS+="-DCMAKE_BUILD_TYPE=MinSizeRel "
    else
      echo_message ERROR "Invalid build mode: $1"
      usage
    fi
    shift
    ;;
  --py_ver)
    shift
    if [[ "$1" != "3.12" && "$1" != "3.10" ]]; then
      echo_message ERROR "Invalid python version: $1"
    fi
    PY_VER="$1"
    CMAKEARGS+="-DPython_EXECUTABLE=/opt/venv/bin/python$PY_VER -DPython3_EXECUTABLE=/opt/venv/bin/python$PY_VER "
    shift
    ;;
  --clean)
    CLEAN_BUILD="true"
    shift
    ;;
  --test)
    CMAKEARGS+="-DENABLE_TESTS=ON "
    shift
    ;;
  --metric)
    CMAKEARGS+="-DENABLE_METRICS=ON "
    shift
    ;;
  --trace)
    CMAKEARGS+="-DENABLE_TRACE=ON "
    shift
    ;;
  --apiperf)
    CMAKEARGS+="-DENABLE_APIPERF=ON "
    shift
    ;;
  --sictstatics)
    # default is off, namely use sict shared library
    CMAKEARGS+="-DSICL_STATIC_LIBRARY=OFF "
    shift
    ;;
  --verbose)
    CMAKEARGS+="-DCMAKE_VERBOSE_MAKEFILE=ON "
    shift
    ;;
  -h|--help)
    usage
    ;;
  --)
    shift
    break
    ;;
  esac
done

[ x$1 != "x" ] && usage

BUILD_DIR+="/$BUILD_TYPE"
echo_message INFO "Build Type: $BUILD_TYPE"
echo_message INFO "Build Dir : $BUILD_DIR"
echo_message INFO "python version: $PY_VER"
[ -z "$CMAKEARGS" ] || echo_message INFO "CMake arguments: $CMAKEARGS"

if [ "$CLEAN_BUILD" = "true" ]; then
  echo_message INFO "Cleanup Old Build Directory ..."
  rm -rf $BUILD_DIR/*
  rm $ROOT_DIR/python/simm/kv/_kv.cpython*
fi

echo_message INFO "Start cmake & make commands ..."
echo_message INFO "................................................................"
cmake $CMAKEARGS -B$BUILD_DIR -S$ROOT_DIR
cmake --build $BUILD_DIR -j8

if [ $? -ne 0 ]; then
  echo_message ERROR "Oops, build FAILED!"
  exit 1
else
  echo_message INFO "Cheers, build succeed!"
  exit 0
fi
