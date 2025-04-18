#!/bin/bash

DEV_PATH=/dev/nvme1n1
MNT_POINT=/home/olh/mnt/57-nvme3n1

sudo mount $DEV_PATH $MNT_POINT
sudo chmod 777 $MNT_POINT