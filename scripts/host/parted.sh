#!/bin/bash

SCRIPT_DIR=$(dirname "$(realpath "$0")")
source "$SCRIPT_DIR/../settings.sh"

parted $HOST_DEV mklabel gpt
parted $HOST_DEV mkpart primary 0% 50% --align optimal
parted $HOST_DEV mkpart primary 50% 100% --align optimal
parted $HOST_DEV print
