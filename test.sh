#!/bin/bash
set -e

# build and run nsqlookupd
LOOKUP_LOGFILE=$(mktemp -t nsqlookupd.XXXXXXX)
echo "building and starting nsqlookupd"
echo "  logging to $LOOKUP_LOGFILE"
go build -o apps/nsqlookupd/nsqlookupd ./apps/nsqlookupd/
apps/nsqlookupd/nsqlookupd >$LOOKUP_LOGFILE 2>&1 &
LOOKUPD_PID=$!

# build and run nsqd configured to use our lookupd above
NSQD_LOGFILE=$(mktemp -t nsqd.XXXXXXX)
cmd="apps/nsqd/nsqd --data-path=/tmp --lookupd-tcp-address=127.0.0.1:4160 --tls-cert=nsqd/test/certs/cert.pem --tls-key=nsqd/test/certs/key.pem"
echo "building and starting $cmd"
echo "  logging to $NSQD_LOGFILE"
go build -o apps/nsqd/nsqd ./apps/nsqd
$cmd >$NSQD_LOGFILE 2>&1 &
NSQD_PID=$!

sleep 0.3

cleanup() {
    echo "killing nsqd PID $NSQD_PID"
    kill -s TERM $NSQD_PID || cat $NSQD_LOGFILE
    echo "killing nsqlookupd PID $LOOKUPD_PID"
    kill -s TERM $LOOKUPD_PID || cat $LOOKUP_LOGFILE
}
trap cleanup INT TERM EXIT

go test -timeout 60s ./...
race="-race"
if go version | grep -q go1.0; then
    race=""
fi
GOMAXPROCS=4 go test -timeout 60s $race ./...

# no tests, but a build is something
for dir in nsqadmin apps/* bench/*; do
    if grep -q '^package main$' $dir/*.go ; then
        echo "building $dir"
        go build -o $dir/$(basename $dir) ./$dir
    else
        echo "WARNING: skipping go build in $dir"
    fi
done