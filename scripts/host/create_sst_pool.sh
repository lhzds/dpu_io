#!/bin/bash

SST_POOL_DIR="/home/olh/mnt/local/sst_pool"
BASE_POOL_SIZE=2000
BIG_POOL_CNT=0
BIG_POOL_SIZE=0
FILE_SIZE_BASE=67108864 # 64 MB
SST_PAD_LEN=1048576 # 1 MB

export XMAKE_ROOT=y

mkdir -p "$SST_POOL_DIR"

SUBPROJ_DIR=$(dirname "$(dirname "$(realpath "$(dirname "$0")")")")
xmake build -P "$SUBPROJ_DIR" sst_pool_create
xmake run -P "$SUBPROJ_DIR" sst_pool_create \
    -d $SST_POOL_DIR -s $BASE_POOL_SIZE -C $BIG_POOL_CNT \
    -S $BIG_POOL_SIZE  -b $FILE_SIZE_BASE -p $SST_PAD_LEN
    