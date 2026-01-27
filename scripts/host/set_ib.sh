#!/bin/bash

SCRIPT_DIR=$(dirname "$(realpath "$0")")
source "$SCRIPT_DIR/../settings.sh"

sudo ip link set $HOST_IB_DEV up
sudo ip addr add $HOST_IB_ADDR dev $HOST_IB_DEV
ip link show
