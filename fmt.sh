#!/bin/bash
for d in nsq nsqd nsqlookupd util util/pqueue examples/nsq_to_file examples/nsqstatsd; do
    pushd $d
    go fmt
    popd
done
