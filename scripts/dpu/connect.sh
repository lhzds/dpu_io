#!/bin/bash

SCRIPT_DIR=$(dirname "$(realpath "$0")")
source "$SCRIPT_DIR/../settings.sh"

sudo modprobe -r nvme-rdma
sudo modprobe nvme-rdma pi_disable=1

sudo nvme connect -t rdma -n $NVMEOF_SUBSYSTEM_NAME -a $NVMEOF_IP_ADDR -s $NVMEOF_PORT_NUM
