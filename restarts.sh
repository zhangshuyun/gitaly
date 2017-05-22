#!/usr/bin/env bash

set -euo pipefail
IFS=$'\n\t'

process_pid=""
new_process_pid=""

function start {
    GITALY_LISTEN_ADDR=:1828 go run cmd/gitaly/main.go &
    process_pid=$!
}

function restart {
    GITALY_LISTEN_ADDR=:1828 go run cmd/gitaly/main.go &
    new_process_pid=$!

    # Give the new process time to start listening before killing the old one
    sleep 2 

    kill -s TERM ${process_pid} || true 
    process_pid=${new_process_pid}
    new_process_pid=""
}

function finish {
    # Last ditch attempt to cleanup before termination
    kill -s TERM ${process_pid} || true

    # If this is run while both the old and new processes are running
    # we need to make sure we terminate both processes
    if [[ -n "${new_process_pid}" ]]; then  
       kill -s TERM ${new_process_pid} || true
    fi 
}

function forward_signal {    
    kill -s ${1} ${process_pid}
}

function await_process {
    set +e
    wait -n
    local wait_status=$?
    set -e

    if [[ ${wait_status} == 0 ]] || [[ ${wait_status} == 129 ]] ; then
        return
    else 
        exit ${wait_status}
    fi

}

trap restart SIGHUP
trap finish EXIT

for sig in STOP CONT ALRM INT QUIT USR1 USR2 TERM; do
    trap "forward_signal ${sig}" ${sig}
done

start

while true; do
    await_process
    sleep 1

    if [[ -z $(jobs -r) ]]; then
        exit 
    fi
done
