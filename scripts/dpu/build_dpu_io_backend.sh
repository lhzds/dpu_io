#!/bin/bash

MODE=release

SUBPROJ_DIR=$(dirname "$(dirname "$(realpath "$(dirname "$0")")")")

cd $SUBPROJ_DIR
xmake f -m $MODE
xmake build backend_run
cd -
