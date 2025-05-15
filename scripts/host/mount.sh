#!/bin/bash

DEV1=/dev/nvme3n1p1
MNT1=/home/olh/mnt/local/db

DEV2=/dev/nvme3n1p2
MNT2=/home/olh/mnt/local/sst_pool

sudo mount "$DEV1" "$MNT1"
sudo mount "$DEV2" "$MNT2"
