#!/bin/bash

# build binary distributions for linux/amd64 and darwin/amd64

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd $DIR

mkdir -p dist
arch=$(go env GOARCH)

version=`cat util/binary_version.go | tail -n1 | awk '{print $NF}' | sed 's/"//g'`
echo "building version $version"
for os in linux darwin; do
    BUILD=$(mktemp -d -t nsq)
    TARGET=nsq-$version.$os-$arch
    mkdir -p $TARGET
    export GOOS=$os GOARCH=$arch CGO_ENABLED=0
    make || exit 1
    DESTDIR=$BUILD/$TARGET PREFIX= make install
    pushd $BUILD
    tar czvf $TARGET.tar.gz $TARGET
    mv $TARGET.tar.gz $DIR/dist
    popd
    make clean
done
