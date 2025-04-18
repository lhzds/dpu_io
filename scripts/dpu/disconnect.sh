#!/bin/bash

DEV_PATH=/dev/nvme1n2
SUBSYSTEM_NAME="test"
MNT_POINT=/home/olh/mnt/57-nvme3n1

sudo umount $MNT_POINT
sudo nvme disconnect $DEV_PATH -n $SUBSYSTEM_NAME
