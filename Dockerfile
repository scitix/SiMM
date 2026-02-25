ARG UBUNTU_VERSION=22.04

# Base image from base ubuntu
FROM ubuntu:${UBUNTU_VERSION}

ARG PYTHON_VERSION=3.10
ENV PYTHON_VERSION=${PYTHON_VERSION}
ENV DEBIAN_FRONTEND=noninteractive

# Install Python and other build dependencies
RUN apt-get update && apt-get install -y \
    linux-tools-common rsyslog perftest \
    rdma-core ibverbs-providers infiniband-diags ibverbs-utils \
    build-essential cmake git vim python3-pip lz4 curl \
    libgflags-dev libgoogle-glog-dev libacl1-dev \
    libprotobuf-dev protobuf-compiler \
    libcurl4-openssl-dev libssl-dev software-properties-common \
    libboost-all-dev libdouble-conversion-dev \
    && add-apt-repository ppa:deadsnakes/ppa -y \
    && apt-get update && apt-get install -y \
    python${PYTHON_VERSION} python${PYTHON_VERSION}-dev \
    python${PYTHON_VERSION}-venv

# Set default python/pip
RUN update-alternatives --install /usr/bin/python python /usr/bin/python${PYTHON_VERSION} 1 \
    && update-alternatives --install /usr/bin/python3 python3 /usr/bin/python${PYTHON_VERSION} 1 \
    && update-alternatives --install /usr/bin/pip pip /usr/bin/pip3 1

RUN python${PYTHON_VERSION} -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --upgrade build setuptools nanobind pytest pytest-asyncio

WORKDIR /workspace

COPY . /workspace

RUN bash ./build.sh --mode=debug --py_ver=${PYTHON_VERSION} --clean
RUN bash ./build.sh --mode=release --py_ver=${PYTHON_VERSION} --clean

# Set default env
ENV SICL_RPC_CTX_NUM_PER_DEV=2
ENV SICL_RPC_MULTI_RW_SIZE_THRESHOLD=1048576
