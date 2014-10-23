#!/bin/bash

# Exit on failed commands
set -e

# build binary distributions for linux/amd64 and darwin/amd64

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
mkdir -p $DIR/dist

mkdir -p $DIR/.godeps
export GOPATH=$DIR/.godeps:$GOPATH
gpm install

os=$(go env GOOS)
arch=$(go env GOARCH)
version=$(awk '/const BINARY_VERSION/ {print $NF}' < $DIR/util/binary_version.go | sed 's/"//g')
goversion=$(go version | awk '{print $3}')

echo "... running tests"
./test.sh

for os in linux darwin; do
    echo "... building v$version for $os/$arch"
    BUILD=$(mktemp -d -t nsq)
    TARGET="nsq-$version.$os-$arch.$goversion"
    GOOS=$os GOARCH=$arch CGO_ENABLED=0 make
    make DESTDIR=$BUILD/$TARGET PREFIX= install
    pushd $BUILD
    tar czvf $TARGET.tar.gz $TARGET
    mv $TARGET.tar.gz $DIR/dist
    popd
    make clean
done
