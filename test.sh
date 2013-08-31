#!/bin/bash
set -e
# a helper script to run tests in the appropriate directories

for dir in nsqd nsqlookupd util/pqueue; do
    echo "testing $dir"
    pushd $dir >/dev/null
    go test -test.v -timeout 15s
    popd >/dev/null
done

# build and run nsqlookupd
pushd nsqlookupd >/dev/null
go build
echo "starting nsqlookupd"
./nsqlookupd >/dev/null 2>&1 &
LOOKUPD_PID=$!
popd >/dev/null

# build and run nsqd configured to use our lookupd above
pushd nsqd >/dev/null
go build
rm -f *.dat
cmd="./nsqd --data-path=/tmp --lookupd-tcp-address=127.0.0.1:4160 --tls-cert=./test/cert.pem --tls-key=./test/key.pem"
echo "starting $cmd"
$cmd >/dev/null 2>&1 &
NSQD_PID=$!
popd >/dev/null

sleep 0.3

cleanup() {
    kill -s TERM $NSQD_PID
    kill -s TERM $LOOKUPD_PID
}
trap cleanup INT TERM EXIT

pushd nsqd/test >/dev/null
echo "testing nsq"
go test -v -timeout 15s
popd >/dev/null

# no tests, but a build is something
for dir in nsqadmin nsqlookupd apps/* bench/*; do
    pushd $dir >/dev/null
    echo "building $dir"
    go build
    popd >/dev/null
done
