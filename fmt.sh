#!/bin/bash
for d in nsq nsqd nsqlookupd util util/pqueue examples/nsq_to_file examples/nsqstatsd examples/nsq_pubsub examples/nsq_to_http; do
    pushd $d
    go fmt
    popd
done
