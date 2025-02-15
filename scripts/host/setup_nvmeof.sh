#!/bin/bash

PORT_NAME=1
NAMESPACE_NAME=1

NVME_DEV="/dev/nvme3n1" # 供远程使用的设备
SUBSYSTEM_NAME="test"
PORT_NUM="4420" # 端口号
IP_ADDR="192.168.1.20" # IB 网卡地址

modprobe mlx5_core
modprobe -r nvme
modprobe nvme num_p2p_queues=1
modprobe nvmet
modprobe nvmet-rdma

# 1. 创建新的子空间
mkdir /sys/kernel/config/nvmet/subsystems/$SUBSYSTEM_NAME
# 2. 允许任何人访问
echo 1 > /sys/kernel/config/nvmet/subsystems/$SUBSYSTEM_NAME/attr_allow_any_host
# 3. 将子空间设置为 offloaded
echo 1 > /sys/kernel/config/nvmet/subsystems/$SUBSYSTEM_NAME/attr_offload
# 4. Create a namespace associated with the subsystem.
# Note：The offloaded subsystem must be associated with namespaces attached to the same physical NVMe device.
mkdir /sys/kernel/config/nvmet/subsystems/$SUBSYSTEM_NAME/namespaces/$NAMESPACE_NAME
# 5. Set the path to the backing store NVMe device 并启用 namespace
# Note：An offloaded subsystem can be associated with only one namespace.
echo -n "$NVME_DEV" > /sys/kernel/config/nvmet/subsystems/$SUBSYSTEM_NAME/namespaces/$NAMESPACE_NAME/device_path
echo 1 > /sys/kernel/config/nvmet/subsystems/$SUBSYSTEM_NAME/namespaces/$NAMESPACE_NAME/enable
# 6. 创建端口
mkdir /sys/kernel/config/nvmet/ports/$PORT_NAME
# 7. 设置属性
echo "$PORT_NUM" > /sys/kernel/config/nvmet/ports/$PORT_NAME/addr_trsvcid
echo "$IP_ADDR" > /sys/kernel/config/nvmet/ports/$PORT_NAME/addr_traddr
echo "rdma" > /sys/kernel/config/nvmet/ports/$PORT_NAME/addr_trtype
echo "ipv4" > /sys/kernel/config/nvmet/ports/$PORT_NAME/addr_adrfam
# 8. 启用 nvme-port
# Note：Only if all subsystems associated with an NVMe-oF port are offloaded, and if the port is capable of performing peer-to-peer transactions, it will become offloaded.
ln -s /sys/kernel/config/nvmet/subsystems/$SUBSYSTEM_NAME/ /sys/kernel/config/nvmet/ports/$PORT_NAME/subsystems/$SUBSYSTEM_NAME
