#!/bin/bash

DEV=/dev/nvme3n1

parted $DEV mklabel gpt
parted $DEV mkpart primary 0% 50% --align optimal
parted $DEV mkpart primary 50% 100% --align optimal
parted $DEV print
