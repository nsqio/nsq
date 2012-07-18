#!/bin/bash
set -e
# a helper script to run tests in the appropriate directories

for dir in nsqd util/pqueue; do
    echo "testing $dir"
    pushd $dir >/dev/null
    go test -test.v -timeout 2s
    popd >/dev/null
done

pushd nsqd >/dev/null
go build
rm -f *.dat
./nsqd --data-path=/tmp >/dev/null 2>&1 &
PID=$!

popd >/dev/null
pushd nsq >/dev/null
echo "testing nsq"
go test -v -timeout 2s
kill -s TERM $PID
popd >/dev/null

# no tests, but a build is something

for dir in nsqlookupd examples/nsq_to_file examples/nsq_pubsub; do
    pushd $dir >/dev/null
    echo "building $dir"
    go build
    popd >/dev/null
done
