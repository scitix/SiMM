#!/bin/bash
# Copyright (c) 2026, Scitix Tech PTE. LTD. All rights reserved.
#

# colored echo
RED="\033[31m"
GREEN="\033[32m"
YELLOW="\033[33m"
BLUE="\033[34m"
MAGENTA="\033[35m"
CYAN="\033[36m"
WHITE="\033[37m"
RESET="\033[0m"

echo ; echo "=== Check User permission ==="
if [ "$EUID" -ne 0 ]; then
  echo -e "${RED}[ERROR]${RESET}: Please run as root . exit..."
  exit 1
fi
echo -e "${GREEN}[INFO]${RESET}: Run as root, continue..."

echo ; echo "=== Check OS and Version ==="
OS_NAME=`cat /etc/os-release | grep "\<NAME\>" | awk -F= '{print $2}'`
OS_VERSION=`cat /etc/os-release | grep "VERSION_ID" | awk -F= '{print $2}'`
OS_NAME_CLEAN=${OS_NAME%\"}
OS_NAME_CLEAN=${OS_NAME_CLEAN#\"}
OS_VERSION_CLEAN=${OS_VERSION%\"}
OS_VERSION_CLEAN=${OS_VERSION_CLEAN#\"}
if [[ ("$OS_NAME_CLEAN" != "Ubuntu" && "$OS_NAME_CLEAN" != "ubuntu") ||\
      ("$OS_VERSION_CLEAN" != "22.04" && "$OS_VERSION_CLEAN" != "24.04") ]]; then
  echo -e "${RED}[ERROR]${RESET}: Only Ubuntu 22.04/24.04 supported now, exit..."
  exit 1
fi
echo -e "${GREEN}[INFO]${RESET}: Host OS : $OS_NAME_CLEAN-$OS_VERSION_CLEAN, continue..."
OS_NAME_CLEAN=$(echo $OS_NAME_CLEAN | tr '[:upper:]' '[:lower:]')
OS_VERSION_CLEAN=$(echo $OS_VERSION_CLEAN | tr '.' '_')

echo ; echo "=== Install sudo package ==="
apt update -y >/dev/null 2>&1
apt install -y sudo >/dev/null 2>&1
if [[ $? -ne 0 ]]; then
  echo -e "${RED}[ERROR]${RESET}: Apt install sudo failed, exit..."
  exit 1
fi
echo -e "${GREEN}[INFO]${RESET}: sudo packages installed or updated, continue..."

echo ; echo "=== Run Git Submodule Update ==="
git submodule update --init --recursive 
echo -e "${GREEN}[INFO]${RESET}: git submodule update done, continue..."

echo ; echo "=== Install Basic Dependency ==="
sudo apt-get install -y libgflags-dev libgoogle-glog-dev libacl1-dev libprotobuf-dev \
                        protobuf-compiler libcurl4-openssl-dev libssl-dev zlib1g-dev \
                        libboost-all-dev libdouble-conversion-dev
if [[ $? -ne 0 ]]; then
  echo -e "${RED}[ERROR]${RESET}: Install dependency packages failed, exit..."
  #exit 1
fi

echo ; echo "=== Install Prometheus-Cpp Dependency ==="
cd ./third_party/
git clone https://github.com/jupp0r/prometheus-cpp.git
cd prometheus-cpp
git submodule init
git submodule update
mkdir build && cd build
cmake .. -DBUILD_SHARED_LIBS=ON -DENABLE_PUSH=OFF -DENABLE_COMPRESSION=OFF
make -j$(nproc)
sudo make install
sudo ldconfig
cd ../../../
rm -rf ./third_party/prometheus-cpp/ # cleanup after build & install

#(BUG): on Ubuntu 24.04, nanobind will fail, need to add risky option(--break-system-packages)
pip install nanobind
if [[ $? -ne 0 ]]; then
  echo -e "${RED}[ERROR]${RESET}: Pip install nanobind failed, exit..."
  exit 1
fi
echo -e "${GREEN}[INFO]${RESET}: install dependency packages done, continue..."

echo ; echo "=== Install SICL Network Library ==="
SICT_PACKAGE_URL="https://oss-ap-southeast.scitix.ai/hisys-simm-depository/sict/${OS_NAME_CLEAN}/${OS_VERSION_CLEAN}/sict.tar.gz"
wget -q --no-check-certificate -O ./third_party/sict.tar.gz ${SICT_PACKAGE_URL}
if [[ $? -ne 0 ]]; then
  echo -e "${RED}[ERROR]${RESET}: wget sict.tar.gz package from public oss bucket failed, exit..."
  exit 1
fi
cd ./third_party/
if [[ ! -f "sict.tar.gz" ]]; then
  echo -e "${RED}[ERROR]${RESET}: sict.tar.gz wget failed, tar.gz package not found, exit..."
  exit 1
fi
tar -xzf sict.tar.gz
if [[ $? -ne 0 ]]; then
  echo -e "${RED}[ERROR]${RESET}: Extract sict.tar.gz failed, exit..."
  exit 1
fi
cd ../
echo -e "${GREEN}[INFO]${RESET}: install sict network packages done, continue..."

#(BUG): on Ubuntu 24.04, pex(pip install) in folly build script may fail in pod env:
# ---
# + pip \
# +      install \
# +      pex
# error: externally-managed-environment

# × This environment is externally managed
# ╰─> To install Python packages system-wide, try apt install
#     python3-xyz, where xyz is the package you are trying to
#     install.
    
#     If you wish to install a non-Debian-packaged Python package,
#     create a virtual environment using python3 -m venv path/to/venv.
#     Then use path/to/venv/bin/python and path/to/venv/bin/pip. Make
#     sure you have python3-full installed.
    
#     If you wish to install a non-Debian packaged Python application,
#     it may be easiest to use pipx install xyz, which will manage a
#     virtual environment for you. Make sure you have pipx installed.
    
#     See /usr/share/doc/python3.12/README.venv for more information.

# note: If you believe this is a mistake, please contact your Python installation or OS distribution provider.
# You can override this, at the risk of breaking your Python installation or OS, by passing --break-system-packages.
# hint: See PEP 668 for the detailed specification.
# Command '['pip', 'install', 'pex']' returned non-zero exit status 1.
#
# As advice, command `sudo apt install -y python3-pex` will succeed
echo ; echo "=== Install Folly Dependency ==="
cd ./third_party/folly/
sudo ./build/fbcode_builder/getdeps.py install-system-deps --recursive >/dev/null 2>&1
if [[ $? -ne 0 ]]; then
  echo -e "${RED}[ERROR]${RESET}: Run folly <getdeps.py install-system-deps --recursive> failed, exit..."
  exit 1
fi
echo -e "${GREEN}[INFO]${RESET}: install folly dependency packages done, continue..."
echo -e "\n\n${GREEN}[INFO]${RESET}: Configuration script completed successfully!"
