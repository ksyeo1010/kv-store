#!/bin/sh

# code taken from https://piazza.com/class/kjacvyindn53om?cid=209

set -e
make

cmds=(
    "./tracing-server"
    "./frontend"
    "./storage"
    "./client"
)

for cmd in "${cmds[@]}"; do {
  $cmd & pid=$!
  PID_LIST+=" $pid";
  sleep 2
} done

trap "kill $PID_LIST" SIGINT

wait $PID_LIST