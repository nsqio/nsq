#!/bin/bash

# build binary distributions for linux/amd64 and darwin/amd64

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd $DIR

os=$(go env GOOS)
arch=$(go env GOARCH)
version=$(cat util/binary_version.go | tail -n1 | awk '{print $NF}' | sed 's/"//g')

TMPGOPATH=$(mktemp -d -t nsqgopath)
mkdir -p $TMPGOPATH/src
mkdir -p $TMPGOPATH/pkg/${os}_${arch}
export GOPATH="$TMPGOPATH:$GOROOT"

echo "... getting dependencies"
go get -v github.com/bitly/go-simplejson
go get -v github.com/bmizerany/assert

echo "... running tests"
./test.sh || exit 1

mkdir -p dist
for os in linux darwin; do
    echo "... building v$version for $os/$arch"
    BUILD=$(mktemp -d -t nsq)
    TARGET="nsq-$version.$os-$arch"
    GOOS=$os GOARCH=$arch CGO_ENABLED=0 make || exit 1
    make DESTDIR=$BUILD/$TARGET PREFIX= install
    pushd $BUILD
    tar czvf $TARGET.tar.gz $TARGET
    mv $TARGET.tar.gz $DIR/dist
    popd
    make clean
done
