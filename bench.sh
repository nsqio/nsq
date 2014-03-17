#!/bin/bash
size="$1"
if [ -z $size ]; then
    size="200"
fi
set -e

echo "# compiling/running nsqd"
pushd apps/nsqd >/dev/null
go build
rm -f *.dat
./nsqd --mem-queue-size=1000000 >/dev/null 2>&1 &
nsqd_pid=$!
popd >/dev/null

cleanup() {
    kill -9 $nsqd_pid
    rm -f nsqd/*.dat
}
trap cleanup INT TERM EXIT

sleep 0.3
echo "# creating topic/channel"
curl --silent 'http://127.0.0.1:4151/create_topic?topic=sub_bench' >/dev/null 2>&1
curl --silent 'http://127.0.0.1:4151/create_channel?topic=sub_bench&channel=ch' >/dev/null 2>&1

echo "# compiling bench_reader/bench_writer"
pushd bench >/dev/null
for app in bench_reader bench_writer; do
    pushd $app >/dev/null
    go build
    popd >/dev/null
done
popd >/dev/null

echo -n "PUB: "
bench/bench_writer/bench_writer --size=$size 2>&1
echo -n "SUB: "
bench/bench_reader/bench_reader --size=$size --channel=ch 2>&1 &
wait $!
