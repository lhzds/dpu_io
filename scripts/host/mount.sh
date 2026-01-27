#!/bin/bash

SCRIPT_DIR=$(dirname "$(realpath "$0")")
source "$SCRIPT_DIR/../settings.sh"

sudo mount "$HOST_DB_DEV" "$HOST_DB_MNT"
sudo mount "$HOST_SST_DEV" "$HOST_SST_MNT"
