#!/bin/bash

MODE=releasedbg

BACKEND_NAME="backend"
DEV_PCI="03:00.1"
DEV_REP_PCI="b1:00.1"

URING_QUEUE_DEPTH=512
DPU_REQ_RB_CAP=268435456 # 256 MB
DPU_RESP_RB_CAP=1048576 # 1 MB

SST_POOL_DIR="/home/olh/mnt/sst_pool"
BASE_POOL_SIZE=2000
BIG_POOL_CNT=0
BIG_POOL_SIZE=0
FILE_SIZE_BASE=67108864 # 64 MB
SST_PAD_LEN=1048576 # 1 MB

export XMAKE_ROOT=y

SUBPROJ_DIR=$(dirname "$(dirname "$(realpath "$(dirname "$0")")")")

xmake f -P "$SUBPROJ_DIR" -m $MODE
xmake build -P "$SUBPROJ_DIR" backend_run
xmake run -d -P "$SUBPROJ_DIR" backend_run \
    -n $BACKEND_NAME -r $DEV_PCI -R $DEV_REP_PCI \
    -u $URING_QUEUE_DEPTH -x $DPU_REQ_RB_CAP -y $DPU_RESP_RB_CAP \
    -d $SST_POOL_DIR -s $BASE_POOL_SIZE -C $BIG_POOL_CNT \
    -S $BIG_POOL_SIZE  -b $FILE_SIZE_BASE -p $SST_PAD_LEN
