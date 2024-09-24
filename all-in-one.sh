#!/usr/bin/env sh
export SPAN_STORAGE_TYPE=grpc
export GRPC_STORAGE_SERVER='localhost:17271'


trap 'kill $(jobs -p)' SIGINT SIGTERM

./jaegerdoris --config $1 &
./jaegerquery &

wait -n
kill -s SIGINT $(jobs -p)
wait
