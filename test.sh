#!/bin/sh
set -e

GOMAXPROCS=1 go test -timeout 90s ./...
GOMAXPROCS=4 go test -timeout 90s -race ./...

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

FMTDIFF="$(find apps internal nsqd nsqlookupd -name '*.go' -exec gofmt -d '{}' ';')"
if [ -n "$FMTDIFF" ]; then
    printf '%s\n' "$FMTDIFF"
    exit 1
fi
