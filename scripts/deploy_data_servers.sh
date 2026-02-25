#!/bin/bash
#
# Copyright (c) 2026, Scitix Tech PTE. LTD. All rights reserved.
#
# Usage:
#   deploy target N data_server processes on one host(vm, vscs etc), it will
#     - find all RDMA NICs and round-robin new processes to specified one NIC
#     - kill and clean all old data_server processes
#     - find available 3 port(io/inter/admin ports) for new process
#     - put new process running background

##########################################################
# define some helper functions
##########################################################
# functions to cleanup old running data_server processes
clean_processes() {
    echo "== kill all old data_server processes =="

    #PIDS=$(pgrep -f "data_server" | grep -v "deploy_data_servers.sh")
    PIDS=$(pgrep -f "ds_rpc_admin_port")

    if [ -n "$PIDS" ]; then
        echo "  Find old procs: $PIDS"
        kill -9 $PIDS
        echo "  All old procs killed(9)"
    else
        echo "  Not find any old procs"
    fi

    # check all old data block files
    rm -f /dev/shm/kv_cache_blk_*
}

# functions to find some next available ports from base port(e.g. 40000)
is_port_used() {
    local PORT=$1
    ss -lnt | awk '{print $4}' | grep -q ":$PORT$"
    return $?
}

find_free_port() {
    local P=$1
    while is_port_used $P; do
        echo "[WARN]: port $P is already used, try next..."
        P=$((P + 1))
    done
    echo $P
}

##########################################################
# arguments check
##########################################################
if [[ ($# -eq 1) && "$1" == "clean" ]]; then
    clean_processes
    exit 0
fi

if [[ $# -ne 5 ]]; then
    echo "Usage:"
    echo "  bash $0 <N> <binary_path> <CM_IP> <MEM_LIMIT> <log_dir>"
    echo "    - N           : new processes number to deploy, should >= 1"
    echo "    - binary_path : data_server binary path"
    echo "    - CM_IP       : cluster manager primary node ip"
    echo "    - MEM_LIMIT   : total memory limit data_server can use(In Bytes)"
    echo "    - log_dir     : data_server simm logs output path"
    exit 1
fi

N=$1
BINARY=$2
CM_IP=$3
MEM_LIMIT=$4
LOG_DIR=$5
BASE_PORT=40000

# number check
if ! [[ "$N" =~ ^[0-9]+$ ]]; then
    echo "[Error]: N must be an integer"
    exit 1
fi
if [[ "$N" -lt 1 ]]; then
    echo "[Error]: N must be >= 1"
    exit 1
fi
# binary existence & executable check
if [[ ! -f "$BINARY" ]]; then
    echo "[Error]: Binary '$BINARY' does not exist"
    exit 1
fi
if [[ ! -x "$BINARY" ]]; then
    echo "[Error]: Binary '$BINARY' is not executable"
    exit 1
fi
# log output existence check
if [[ ! -d "$LOG_DIR" ]]; then
    echo "[Error]: Log directory '$LOG_DIR' does not exist"
    exit 1
fi
# TODO : add checks for memory limit and cm ip

##########################################################
# check host RDMA NICs
##########################################################
echo "== RDMA NICs check =="

NET_DEVS=()

# rdma command
if command -v rdma >/dev/null 2>&1; then
    NET_DEVS=($(rdma link show | awk -F': ' '/link/ {print $2}' | awk '{print $1}'))
fi
# ibstat command 
if [ ${#NET_DEVS[@]} -eq 0 ] && command -v ibstat >/dev/null 2>&1; then
    NET_DEVS=($(ibstat -l))
fi
# check ib directory
if [ ${#NET_DEVS[@]} -eq 0 ] && [ -d /sys/class/infiniband ]; then
    NET_DEVS=($(ls /sys/class/infiniband))
fi

if [ ${#NET_DEVS[@]} -eq 0 ]; then
    echo "[Error]: Failed to check RDMA NIC"
    exit 1
fi

NUM_DEVS=${#NET_DEVS[@]}
echo "  Found ${NUM_DEVS} RDMA NICs: ${NET_DEVS[@]}"

###############################################
# kill & clean all old data_server processes
###############################################
clean_processes

##########################################################
# Launch N  data_server processes
##########################################################
echo "== Start to launch $N data_server processes =="

current_port=$BASE_PORT

for ((i=1; i<=N; i++))
do
    echo
    echo "--------------------------------------------------"
    echo "Start to launch $i data_server process ..."

    # RDMA NICs round-robin for current data_server process
    dev_index=$(( (i-1) % NUM_DEVS ))
    DEV=${NET_DEVS[$dev_index]}

    # find next 3 available ports
    port1=$(find_free_port $current_port)
    port2=$(find_free_port $((port1 + 1)))
    port3=$(find_free_port $((port2 + 1)))

    # update port index for next process
    current_port=$((port3 + 1))

    DS_LOG_FILE="${LOG_DIR}/simm_ds-p${i}.log"
    DS_DEFAULT_LOF_FILE="${LOG_DIR}/simm_defualt_ds-p${i}.log"

    echo "  RDMA NIC  : $DEV"
    echo "  Ports     : $port1 / $port2 / $port3"
    echo "  LOG PATH  : $DS_LOG_FILE / $DS_DEFAULT_LOF_FILE"
    echo "  CM IP     : $CM_IP"
    echo "  MEM LIMIT : $MEM_LIMIT"

    SICL_NET_DEVICES=$DEV \
        nohup $BINARY \
        --cm_primary_node_ip=$CM_IP \
        --memory_limit_bytes=$MEM_LIMIT \
        --ds_log_file="$DS_LOG_FILE" \
        --default_logging_file="$DS_DEFAULT_LOF_FILE" \
        --io_service_port=$port1 \
        --mgt_service_port=$port2 \
        --ds_rpc_admin_port=$port3 \
        >/dev/null 2>&1 &

    PID=$!
    sleep 0.5

    if ! kill -0 $PID >/dev/null 2>&1; then
        echo "[ERROR]: data_server process $i launch failed (pid=$PID), exit..."
        exit 1
    fi

    echo "Process $i launch succeed (pid=$PID)!"
done

echo
echo "==============================================="
echo "All $N data_server processes launch succeed!"
echo "==============================================="
