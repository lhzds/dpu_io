#!/bin/bash

DEV1=/dev/nvme3n1p1
MNT1=/home/olh/mnt/local/db

DEV2=/dev/nvme3n1p2
MNT2=/home/olh/mnt/local/sst_pool

mkdir -p "$MNT1"
mkdir -p "$MNT2"

sudo umount "$MNT1"
sudo umount "$MNT2"

sudo wipefs "$DEV1"
sudo wipefs "$DEV2"

sudo mkfs.ext4 "$DEV1"
sudo mkfs.ext4 "$DEV2"

sudo mount "$DEV1" "$MNT1"
sudo mount "$DEV2" "$MNT2"

sudo chmod 777 "$MNT1"
sudo chmod 777 "$MNT2"
