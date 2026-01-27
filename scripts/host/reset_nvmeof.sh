#!/bin/bash

SCRIPT_DIR=$(dirname "$(realpath "$0")")
source "$SCRIPT_DIR/../settings.sh"

rm -rf /sys/kernel/config/nvmet/ports/$NVMEOF_PORT_NAME/subsystems/$NVMEOF_SUBSYSTEM_NAME
echo 0 > /sys/kernel/config/nvmet/subsystems/$NVMEOF_SUBSYSTEM_NAME/namespaces/$NVMEOF_NAMESPACE_NAME/enable
echo -n 0 > /sys/kernel/config/nvmet/subsystems/$NVMEOF_SUBSYSTEM_NAME/namespaces/$NVMEOF_NAMESPACE_NAME/device_path
rmdir --ignore-fail-on-non-empty /sys/kernel/config/nvmet/subsystems/$NVMEOF_SUBSYSTEM_NAME/namespaces/$NVMEOF_NAMESPACE_NAME
rmdir --ignore-fail-on-non-empty /sys/kernel/config/nvmet/subsystems/$NVMEOF_SUBSYSTEM_NAME/
echo -n > /sys/kernel/config/nvmet/ports/$NVMEOF_PORT_NAME/addr_trsvcid
echo -n > /sys/kernel/config/nvmet/ports/$NVMEOF_PORT_NAME/addr_traddr
echo -n > /sys/kernel/config/nvmet/ports/$NVMEOF_PORT_NAME/addr_trtype
echo -n > /sys/kernel/config/nvmet/ports/$NVMEOF_PORT_NAME/addr_adrfam
rmdir --ignore-fail-on-non-empty /sys/kernel/config/nvmet/ports/$NVMEOF_PORT_NAME

rmmod nvmet_rdma
rmmod nvmet
