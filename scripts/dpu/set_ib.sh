#!/bin/bash

SCRIPT_DIR=$(dirname "$(realpath "$0")")
source "$SCRIPT_DIR/../settings.sh"

sudo systemctl enable opensmd
sudo systemctl restart opensmd

sudo ip link set $DPU_IB_DEV up
sudo ip addr add $DPU_IB_ADDR dev $DPU_IB_DEV
ip link show
