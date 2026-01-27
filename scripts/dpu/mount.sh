#!/bin/bash

SCRIPT_DIR=$(dirname "$(realpath "$0")")
source "$SCRIPT_DIR/../settings.sh"

mkdir -p $DPU_SST_MNT_POINT

sudo mount $DPU_SST_DEV_PATH $DPU_SST_MNT_POINT
sudo chmod 777 $DPU_SST_MNT_POINT