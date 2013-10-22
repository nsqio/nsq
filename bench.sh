#!/bin/bash
set -e

pushd nsqd >/dev/null
go build
rm -f *.dat
./nsqd --mem-queue-size=1000000 >/dev/null 2>&1 &
nsqd_pid=$!
popd >/dev/null

pushd bench >/dev/null
go build ./...
popd >/dev/null

write_out=$(bench/bench_writer/bench_writer 2>&1)

curl --silent 'http://127.0.0.1:4151/create_channel?topic=sub_bench&channel=ch' >/dev/null 2>&1
sleep 5

read_out=$(bench/bench_reader/bench_reader 2>&1)

cleanup() {
    kill -9 $nsqd_pid
    rm -f nsqd/*.dat
}
trap cleanup INT TERM EXIT

echo "results..."
echo $write_out
echo $read_out
