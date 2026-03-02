#!/bin/bash

SCRIPT_DIR=$(dirname "$(realpath "$0")")
source "$SCRIPT_DIR/../settings.sh"

export XMAKE_ROOT

SUBPROJ_DIR=$(dirname "$(dirname "$(realpath "$(dirname "$0")")")")

cd $SUBPROJ_DIR

XMAKE_ROOT=y xmake f -m $MODE
XMAKE_ROOT=y xmake run backend_run \
    -n $BACKEND_NAME -r $DEV_PCI -R $DEV_REP_PCI \
    -u $URING_QUEUE_DEPTH --dpu_dma_task_slot_num $DPU_DMA_TASK_SLOT_NUM \
    -x $DPU_REQ_RB_CAP -y $DPU_RESP_RB_CAP \
    -d $DPU_SST_POOL_DIR -s $BASE_POOL_SIZE -C $BIG_POOL_CNT \
    -S $BIG_POOL_SIZE  -b $FILE_SIZE_BASE -p $SST_PAD_LEN \
    -i $STATS_INTERVAL_MS
cd -