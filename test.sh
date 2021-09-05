#!/bin/sh
set -e

GOMAXPROCS=1 go test -timeout 90s ./...

if [ "$GOARCH" = "amd64" ] || [ "$GOARCH" = "arm64" ]; then
    # go test: -race is only supported on linux/amd64, linux/ppc64le,
    # linux/arm64, freebsd/amd64, netbsd/amd64, darwin/amd64 and windows/amd64
    GOMAXPROCS=4 go test -timeout 90s -race ./...
fi

# no tests, but a build is something
for dir in apps/*/ bench/*/; do
    dir=${dir%/}
    if grep -q '^package main$' $dir/*.go 2>/dev/null; then
        echo "building $dir"
        go build -o $dir/$(basename $dir) ./$dir
    else
        echo "(skipped $dir)"
    fi
done

# disable "composite literal uses unkeyed fields"
go vet -composites=false ./...

FMTDIFF="$(find apps internal nsqd nsqlookupd -name '*.go' -exec gofmt -d '{}' ';')"
if [ -n "$FMTDIFF" ]; then
    printf '%s\n' "$FMTDIFF"
    exit 1
fi
