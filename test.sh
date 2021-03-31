#!/bin/sh

set -e
make

# run tracing-server, frontend, storage
# separate each pid, we may automate killing storage
"./tracing-server" & ts_pid=$!
sleep 2
"./frontend" & f_pid=$!
sleep 2
"./storage" & s_pid=$!
sleep 2

# run test and get smoke test output
{
    TEST_CMD=$(go run ./cmd/test/test.go)

    echo ""
    echo $TEST_CMD
    echo ""
} &

# storage failures may go here
sleep 1
kill $s_pid
sleep 0.5
"./storage" & s_pid=$!

PID_LIST+="$ts_pid $f_pid $s_pid"

trap "kill $PID_LIST" SIGINT

wait $PID_LIST