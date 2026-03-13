#!/bin/bash
#
# Copyright (c) 2026, Scitix Tech PTE. LTD. All rights reserved.
#

usage() {
  echo "  usage: ./build_docker.sh [OPTIONS]"
  echo "  OPTIONS:"
  echo "    --registry xxx            Docker registry URL, default docker.io"
  echo "    --tag xxx                 Simm image tag, contains the image namespace, name, tag, like hisys/simm:latest"
  echo "    --py_ver xxx              Python version, pick one: 3.10 | 3.12"
  echo "    --upload_path xxx         Upload simm c++/python client sdk to the URL, default empty (not upload)"
  echo "    -h, --help                Show this help message"
  echo

  exit 0
}

REGISTRY="docker.io"
TAG=""
PY_VER="3.10"
UPLOAD_PATH=""
UBUNTU_VER="22.04"

ARGS=$(getopt -l \
registry:,\
tag:,\
py_ver:,\
upload_path:,\
help -o h -- "$@") || usage

eval set -- "$ARGS"
while [ -n "$1" ]; do
  case "$1" in
  --registry)
    shift
    REGISTRY="$1"
    shift
    ;;
  --tag)
    shift
    TAG="$1"
    shift
    ;;
  --py_ver)
    shift
    PY_VER="$1"
    shift
    ;;
  --upload_path)
    shift
    UPLOAD_PATH="$1"
    shift
    ;;
  --)
    shift
    break
    ;;
  esac
done

if [[ -z "$TAG" ]]; then
    echo "❌ Error: --tag is required"
    exit 1
fi

if [[ "$PY_VER" != "3.10" && "$PY_VER" != "3.12" ]]; then
    echo "❌ Error: --python only supports 3.10 or 3.12"
    exit 1
elif [[ "$PY_VER" == "3.12" ]]; then
    UBUNTU_VER="24.04"
fi

IMAGE=${REGISTRY}/${TAG}

echo "image: ${IMAGE}"
echo "python_version: ${PY_VER}"

if ! docker build --build-arg PYTHON_VERSION=${PY_VER} --build-arg UBUNTU_VERSION=${UBUNTU_VER} -t ${IMAGE} .; then
    exit 1
fi
docker push ${IMAGE}

if [[ "$UPLOAD_PATH" != "" ]]; then
    echo "Upload sdk to OSS: ${UPLOAD_PATH}"
    CONTAINER_ID=$(docker create ${IMAGE})
    rm -rf ./python/dist
    docker cp ${CONTAINER_ID}:/workspace/python/dist ./python/
    docker rm ${CONTAINER_ID}
    ossctl cp ./python/dist/*.whl ${UPLOAD_PATH}/python_sdk/
else
    echo "skip upload oss"
fi
