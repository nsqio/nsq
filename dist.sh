#!/bin/bash

# build binary distributions for linux/amd64 and darwin/amd64

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
mkdir -p $DIR/dist

os=$(go env GOOS)
arch=$(go env GOARCH)
version=$(cat $DIR/util/binary_version.go | grep "const BINARY_VERSION" | awk '{print $NF}' | sed 's/"//g')
goversion=$(go version | awk '{print $3}')

TMPGOPATH=$(mktemp -d -t nsqgopath)
mkdir -p $TMPGOPATH/src
mkdir -p $TMPGOPATH/pkg/${os}_${arch}
mkdir -p $TMPGOPATH/src/github.com/bitly/nsq

git archive HEAD | tar -x -C $TMPGOPATH/src/github.com/bitly/nsq

export GOPATH="$TMPGOPATH:$GOROOT"

echo "... getting dependencies"
go get -v github.com/bitly/go-simplejson
go get -v github.com/bmizerany/assert
go get -v github.com/bitly/go-hostpool

pushd $TMPGOPATH/src/github.com/bitly/nsq

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

popd
