#!/usr/bin/env bash
export SPAN_STORAGE_TYPE=grpc
export GRPC_STORAGE_SERVER='localhost:17271'


trap 'kill $(jobs -p)' SIGINT SIGTERM

./jaegerdoris --config $1 &
sleep 1
pgrep -x "./jaegerdoris" > /dev/null
if [ $? -ne 0 ]; then
    exit 1
fi

./jaegerquery &

wait -n
kill -s SIGINT $(jobs -p)
wait
