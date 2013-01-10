#!/bin/bash
set -e
# a helper script to run tests in the appropriate directories

if [ "$HOME" == "/home/travis" ]; then
    echo "moving back to build path, and removing checkout in GOPATH"
    echo "this is because nsq uses relative imports, and that precludes building in $GOPATH"
    GITHUB_USER=$(pwd | awk -F/ '{print $(NF-1)}')
    cd ~/builds/$GITHUB_USER/nsq
fi

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
echo "starting nsqd --data-path=/tmp --lookupd-tcp-address=127.0.0.1:4160"
./nsqd --data-path=/tmp --lookupd-tcp-address=127.0.0.1:4160 >/dev/null 2>&1 &
NSQD_PID=$!
popd >/dev/null

cleanup() {
    kill -s TERM $NSQD_PID
    kill -s TERM $LOOKUPD_PID
}
trap cleanup INT TERM EXIT

pushd nsq >/dev/null
echo "testing nsq"
go test -v -timeout 15s
popd >/dev/null

# no tests, but a build is something
for dir in nsqadmin nsqlookupd examples/*; do
    pushd $dir >/dev/null
    echo "building $dir"
    go build
    popd >/dev/null
done
