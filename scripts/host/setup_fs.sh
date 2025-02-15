#! /bin/bash

DEV_PATH=/dev/nvme2n1
MNT_POINT=/home/olh/mnt/local/$(basename $DEV_PATH)

sudo umount "$MNT_POINT"
sudo wipefs $DEV_PATH
sudo mkfs.ext4 $DEV_PATH
sudo mount $DEV_PATH "$MNT_POINT"
sudo chmod 777 "$MNT_POINT"
