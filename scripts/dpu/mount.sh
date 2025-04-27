#!/bin/bash

DEV_PATH=/dev/nvme1n1p2
MNT_POINT=/home/olh/mnt/sst_pool

mkdir -p $MNT_POINT

sudo mount $DEV_PATH $MNT_POINT
sudo chmod 777 $MNT_POINT