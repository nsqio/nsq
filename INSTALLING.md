## Binary Releases

Pre-built binaries (`nsqd`, `nsqlookupd`, `nsqadmin`, and all example apps) for linux and darwin are
available for [download][binary].

## Building From Source

### Pre-requisites

**golang** http://golang.org/doc/install

**simplejson** https://github.com/bitly/go-simplejson

    $ go get github.com/bitly/go-simplejson

**notify** https://github.com/bitly/go-notify

    $ go get github.com/bitly/go-notify

**assert** https://github.com/bmizerany/assert

    $ go get github.com/bmizerany/assert

### Compiling

NOTE: binaries can not be built from within `$GOPATH` because of relative imports. To build,
checkout to a directory outside of your `$GOPATH`.

    $ git clone https://github.com/bitly/nsq.git
    $ cd $REPO
    $ make
    $ make install

Go package (for building Go readers)

    $ go get github.com/bitly/nsq/nsq

Python module (for building Python readers)

    $ pip install pynsq

## Testing

    $ ./test.sh

[binary]: https://github.com/bitly/nsq/downloads
