#!/bin/bash

SCRIPT_DIR=$(dirname "$(realpath "$0")")
source "$SCRIPT_DIR/../settings.sh"

SUBPROJ_DIR=$(dirname "$(dirname "$(realpath "$(dirname "$0")")")")

cd $SUBPROJ_DIR
XMAKE_ROOT=y xmake f -m $MODE
XMAKE_ROOT=y xmake build backend_run
cd -
