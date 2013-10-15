#!/bin/bash

# build binary distributions for linux/amd64 and darwin/amd64

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
mkdir -p $DIR/dist

os=$(go env GOOS)
arch=$(go env GOARCH)
version=$(cat $DIR/util/binary_version.go | grep "const BINARY_VERSION" | awk '{print $NF}' | sed 's/"//g')
goversion=$(go version | awk '{print $3}')

echo "... running tests"
./test.sh || exit 1

for os in linux darwin; do
    echo "... building v$version for $os/$arch"
    BUILD=$(mktemp -d -t nsq)
    TARGET="nsq-$version.$os-$arch.$goversion"
    GOOS=$os GOARCH=$arch CGO_ENABLED=0 make || exit 1
    make DESTDIR=$BUILD/$TARGET PREFIX= install
    pushd $BUILD
    tar czvf $TARGET.tar.gz $TARGET
    mv $TARGET.tar.gz $DIR/dist
    popd
    make clean
done
