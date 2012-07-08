#!/bin/bash
set -e
# a helper script to run tests in the appropriate directories

for dir in nsqd; do
    pushd $dir >/dev/null
    go test -test.v
    popd >/dev/null
done

pushd nsqd >/dev/null
go build
./nsqd >/dev/null 2>&1 &
PID=$!

popd >/dev/null
pushd nsq >/dev/null
go test -test.v
kill -s TERM $PID
popd >/dev/null
