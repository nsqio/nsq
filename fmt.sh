#!/bin/bash
for d in nsq nsqd nsqlookupd nsqreader nsqstatsd util; do
    pushd $d
    go fmt
    popd
done
