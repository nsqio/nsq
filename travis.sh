#!/bin/bash

set -e

go_minor_version=$(go version | awk '{print $3}' | awk -F. '{print $2}')
if [[ $go_minor_version -gt 10 ]]; then
    export GO111MODULE=on
else
    wget -O dep https://github.com/golang/dep/releases/download/v0.5.0/dep-linux-amd64
    chmod +x dep
    ./dep ensure
fi

./test.sh
./coverage.sh --coveralls
