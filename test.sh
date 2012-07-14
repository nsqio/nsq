#!/bin/bash
set -e
# a helper script to run tests in the appropriate directories

for dir in nsqd util/pqueue; do
    pushd $dir >/dev/null
    go test -test.v -timeout 2s
    popd >/dev/null
done

pushd nsqd >/dev/null
go build
rm -f *.dat
./nsqd>/dev/null 2>&1 &
PID=$!

popd >/dev/null
pushd nsq >/dev/null
go test -v -timeout 2s
kill -s TERM $PID
popd >/dev/null
