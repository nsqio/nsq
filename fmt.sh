#!/bin/bash
for d in nsq nsqd nsqlookupd nsqadmin util util/pqueue examples/*; do
    pushd $d
    go fmt
    popd
done
