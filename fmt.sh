#!/bin/bash
for d in nsq nsqd nsqlookupd nsqreader nsqstatsd util util/notify; do
    pushd $d
    go fmt
    popd
done
