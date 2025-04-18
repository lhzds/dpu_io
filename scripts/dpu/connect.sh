#!/bin/bash

SUBSYSTEM_NAME="test"
PORT_NUM="4420" # 端口号
IP_ADDR="192.168.1.20" # IB 网卡地址

sudo modprobe -r nvme-rdma
sudo modprobe nvme-rdma pi_disable=1

sudo nvme connect -t rdma -n $SUBSYSTEM_NAME -a $IP_ADDR -s $PORT_NUM
