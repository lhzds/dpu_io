#!/bin/bash

SUBSYSTEM_NAME="test"
PORT_NAME=1
NAMESPACE_NAME=1

rm -rf /sys/kernel/config/nvmet/ports/$PORT_NAME/subsystems/$SUBSYSTEM_NAME
echo 0 > /sys/kernel/config/nvmet/subsystems/$SUBSYSTEM_NAME/namespaces/$NAMESPACE_NAME/enable
echo -n 0 > /sys/kernel/config/nvmet/subsystems/$SUBSYSTEM_NAME/namespaces/$NAMESPACE_NAME/device_path
rmdir --ignore-fail-on-non-empty /sys/kernel/config/nvmet/subsystems/$SUBSYSTEM_NAME/namespaces/$NAMESPACE_NAME
rmdir --ignore-fail-on-non-empty /sys/kernel/config/nvmet/subsystems/$SUBSYSTEM_NAME/
echo -n > /sys/kernel/config/nvmet/ports/$PORT_NAME/addr_trsvcid
echo -n > /sys/kernel/config/nvmet/ports/$PORT_NAME/addr_traddr
echo -n > /sys/kernel/config/nvmet/ports/$PORT_NAME/addr_trtype
echo -n > /sys/kernel/config/nvmet/ports/$PORT_NAME/addr_adrfam
rmdir --ignore-fail-on-non-empty /sys/kernel/config/nvmet/ports/$PORT_NAME

rmmod nvmet_rdma
rmmod nvmet
