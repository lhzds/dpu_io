#!/bin/bash

SCRIPT_DIR=$(dirname "$(realpath "$0")")
source "$SCRIPT_DIR/../settings.sh"

export XMAKE_ROOT

mkdir -p "$HOST_SST_POOL_DIR"

SUBPROJ_DIR=$(dirname "$(dirname "$(realpath "$(dirname "$0")")")")
xmake build -P "$SUBPROJ_DIR" sst_pool_create
xmake run -P "$SUBPROJ_DIR" sst_pool_create \
    -d $HOST_SST_POOL_DIR -s $BASE_POOL_SIZE -C $BIG_POOL_CNT \
    -S $BIG_POOL_SIZE  -b $FILE_SIZE_BASE -p $SST_PAD_LEN
    