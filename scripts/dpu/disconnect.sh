#!/bin/bash

DEV_PATH=/dev/nvme1n1
SUBSYSTEM_NAME="test"

sudo nvme disconnect $DEV_PATH -n $SUBSYSTEM_NAME
