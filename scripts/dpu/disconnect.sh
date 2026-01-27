#!/bin/bash

SCRIPT_DIR=$(dirname "$(realpath "$0")")
source "$SCRIPT_DIR/../settings.sh"

sudo nvme disconnect $DPU_NVME_DEV_PATH -n $NVMEOF_SUBSYSTEM_NAME
sudo modprobe -r nvme-rdma