#!/bin/bash

DEV_PATH=/dev/nvme1n1
MNT_POINT=/home/ubuntu/mnt/remote/57-nvme3n1

SUBSYSTEM_NAME="test"
PORT_NUM="4420" # 端口号
IP_ADDR="192.168.1.20" # IB 网卡地址

sudo modprobe -r nvme-rdma
sudo modprobe nvme-rdma pi_disable=1

sudo nvme connect -t rdma -n $SUBSYSTEM_NAME -a $IP_ADDR -s $PORT_NUM
sudo mount $DEV_PATH $MNT_POINT
sudo chmod 777 $MNT_POINT
