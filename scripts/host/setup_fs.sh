#!/bin/bash

SCRIPT_DIR=$(dirname "$(realpath "$0")")
source "$SCRIPT_DIR/../settings.sh"

mkdir -p "$HOST_DB_MNT"
mkdir -p "$HOST_SST_MNT"

sudo umount "$HOST_DB_MNT"
sudo umount "$HOST_SST_MNT"

sudo wipefs "$HOST_DB_DEV"
sudo wipefs "$HOST_SST_DEV"

sudo mkfs.ext4 "$HOST_DB_DEV"
sudo mkfs.ext4 "$HOST_SST_DEV"

sudo mount "$HOST_DB_DEV" "$HOST_DB_MNT"
sudo mount "$HOST_SST_DEV" "$HOST_SST_MNT"

sudo chmod 777 "$HOST_DB_MNT"
sudo chmod 777 "$HOST_SST_MNT"
