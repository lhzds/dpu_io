#!/bin/bash

SCRIPT_DIR=$(dirname "$(realpath "$0")")
source "$SCRIPT_DIR/../settings.sh"

SUBPROJ_DIR=$(dirname "$(dirname "$(realpath "$(dirname "$0")")")")

cd $SUBPROJ_DIR
xmake f -m $MODE
xmake build backend_run
cd -
