#!/bin/bash

SCRIPT_DIR=$(dirname "$(realpath "$0")")
source "$SCRIPT_DIR/../settings.sh"

umount "$HOST_DB_MNT"
umount "$HOST_SST_MNT"
