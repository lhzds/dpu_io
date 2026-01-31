#!/bin/bash

# Common
: "${MODE:=release}"
: "${XMAKE_ROOT:=y}"

# DPU backend run
: "${BACKEND_NAME:=backend}"
: "${DEV_PCI:=03:00.1}"
: "${DEV_REP_PCI:=b1:00.1}"
: "${URING_QUEUE_DEPTH:=1024}"
: "${DPU_DMA_TASK_SLOT_NUM:=256}"
: "${DPU_REQ_RB_CAP:=536870912}"   # 512 MB
: "${DPU_RESP_RB_CAP:=1048576}"    # 1 MB
: "${STATS_INTERVAL_MS:=1000}"

# SST pool config (shared by host/dpu, dir differs)
: "${DPU_SST_POOL_DIR:=/home/olh/mnt/sst_pool}"
: "${HOST_SST_POOL_DIR:=/home/olh/mnt/local/sst_pool}"
: "${BASE_POOL_SIZE:=4000}"
: "${BIG_POOL_CNT:=0}"
: "${BIG_POOL_SIZE:=0}"
: "${FILE_SIZE_BASE:=67108864}" # 64 MB
: "${SST_PAD_LEN:=1048576}"     # 1 MB

# IB configuration
: "${HOST_IB_DEV:=ibs21f1}"
: "${HOST_IB_ADDR:=192.168.1.20}"
: "${DPU_IB_DEV:=ib1}"
: "${DPU_IB_ADDR:=192.168.1.21}"
: "${IB_PREFIX_LEN:=24}"

# NVMe-oF config (host setup + dpu connect)
: "${NVMEOF_PORT_NAME:=1}"
: "${NVMEOF_NAMESPACE_NAME:=1}"
: "${NVMEOF_SUBSYSTEM_NAME:=test}"
: "${NVMEOF_PORT_NUM:=4420}"
: "${NVMEOF_IP_ADDR:=${HOST_IB_ADDR}}"

# Host disk partitions + mount points
: "${HOST_DEV:=/dev/nvme3n1}"
: "${HOST_DB_DEV:=${HOST_DEV}p1}"
: "${HOST_DB_MNT:=/home/olh/mnt/local/db}"
: "${HOST_SST_DEV:=${HOST_DEV}p2}"
: "${HOST_SST_MNT:=/home/olh/mnt/local/sst_pool}"

# DPU NVMe-oF connect/disconnect + mount
: "${DPU_NVME_DEV_PATH:=/dev/nvme1n1}"
: "${DPU_SST_DEV_PATH:=/dev/nvme1n1p2}"
: "${DPU_SST_MNT_POINT:=/home/olh/mnt/sst_pool}"
