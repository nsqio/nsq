#!/bin/bash
set -e
# a helper script to run tests in the appropriate directories

for dir in nsqd nsq; do
    pushd $dir
    go test
    popd
done

pushd nsqd
go build
./nsqd &
PID=$!

popd && pushd nsqreader
go test
kill -s TERM $PID
popd