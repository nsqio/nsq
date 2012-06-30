#!/bin/bash
for d in nsq nsqd nsqlookupd nsqstatsd util examples/nsq_to_file; do
    pushd $d
    go fmt
    popd
done
