#!/bin/bash
set -e
# a helper script to run tests in the appropriate directories

for dir in nsqd; do
    pushd $dir
    go test
    popd
done

pushd nsqd
go build
./nsqd &
PID=$!

popd && pushd nsq
go test
kill -s TERM $PID
popd
