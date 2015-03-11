#!/bin/bash
set -e

# build and run nsqlookupd
LOOKUP_LOGFILE=$(mktemp -t nsqlookupd.XXXXXXX)
cmd="apps/nsqlookupd/nsqlookupd --broadcast-address=127.0.0.1"
echo "building and starting $cmd"
echo "  logging to $LOOKUP_LOGFILE"
go build -o apps/nsqlookupd/nsqlookupd ./apps/nsqlookupd/
$cmd >$LOOKUP_LOGFILE 2>&1 &
LOOKUPD_PID=$!

# build and run nsqd configured to use our lookupd above
NSQD_LOGFILE=$(mktemp -t nsqd.XXXXXXX)
cmd="apps/nsqd/nsqd --data-path=/tmp --broadcast-address=127.0.0.1 --lookupd-tcp-address=127.0.0.1:4160 --tls-cert=nsqd/test/certs/cert.pem --tls-key=nsqd/test/certs/key.pem --tls-min-version=tls1.0"
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
GOMAXPROCS=4 go test -timeout 60s -race ./...

# no tests, but a build is something
for dir in $(find apps bench -maxdepth 1 -type d) nsqadmin; do
    if grep -q '^package main$' $dir/*.go ; then
        echo "building $dir"
        go build -o $dir/$(basename $dir) ./$dir
    else
        echo "WARNING: skipping go build in $dir"
    fi
done
