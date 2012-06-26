#!/bin/bash
for d in nsq nsqd nsqlookupd nsqstatsd util; do
    pushd $d
    go fmt
    popd
done
