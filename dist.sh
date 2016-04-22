#!/bin/bash

# 1. commit to bump the version and update the changelog/readme
# 2. tag that commit
# 3. use dist.sh to produce tar.gz for linux and darwin
# 4. upload *.tar.gz to our bitly s3 bucket
# 5. docker push nsqio/nsq
# 6. push to nsqio/master
# 7. update the release metadata on github / upload the binaries there too
# 8. update the gh-pages branch with versions / download links
# 9. update homebrew version
# 10. send release announcement emails
# 11. update IRC channel topic
# 12. tweet

set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
rm -rf   $DIR/dist/docker
mkdir -p $DIR/dist/docker
rm -rf   $DIR/.godeps
mkdir -p $DIR/.godeps
export GOPATH=$DIR/.godeps:$GOPATH
GOPATH=$DIR/.godeps gpm install

GOFLAGS='-ldflags="-s -w"'
arch=$(go env GOARCH)
version=$(awk '/const Binary/ {print $NF}' < $DIR/internal/version/binary.go | sed 's/"//g')
goversion=$(go version | awk '{print $3}')

echo "... running tests"
./test.sh

for os in linux darwin freebsd windows; do
    echo "... building v$version for $os/$arch"
    BUILD=$(mktemp -d -t nsq)
    TARGET="nsq-$version.$os-$arch.$goversion"
    GOOS=$os GOARCH=$arch CGO_ENABLED=0 \
        make DESTDIR=$BUILD PREFIX=/$TARGET GOFLAGS="$GOFLAGS" install
    pushd $BUILD
    if [ "$os" == "linux" ]; then
        cp -r $TARGET/bin $DIR/dist/docker/
    fi
    tar czvf $TARGET.tar.gz $TARGET
    mv $TARGET.tar.gz $DIR/dist
    popd
    make clean
    rm -r $BUILD
done

docker build -t nsqio/nsq:v$version .
if [[ ! $version == *"-"* ]]; then
    echo "Tagging nsqio/nsq:v$version as the latest release."
    docker tag -f nsqio/nsq:v$version nsqio/nsq:latest
fi
