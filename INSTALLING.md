## <a name="binary"></a>Binary Releases

Pre-built binaries (`nsqd`, `nsqlookupd`, `nsqadmin`, and all example apps) for linux and darwin are
available for download:

 * [nsq-0.2.17.darwin-amd64.tar.gz][0.2.17_darwin]
 * [nsq-0.2.17.linux-amd64.tar.gz][0.2.17_linux]
 * [nsq-0.2.16.darwin-amd64.tar.gz][0.2.16_darwin]
 * [nsq-0.2.16.linux-amd64.tar.gz][0.2.16_linux]
 * [nsq-0.2.15.darwin-amd64.tar.gz][0.2.15_darwin]
 * [nsq-0.2.15.linux-amd64.tar.gz][0.2.15_linux]

## Building From Source

### Pre-requisites

**golang** http://golang.org/doc/install - **version `1.0.3+` is required**

**simplejson** https://github.com/bitly/go-simplejson

    $ go get github.com/bitly/go-simplejson

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

[0.2.17_darwin]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.17.darwin-amd64.tar.gz
[0.2.17_linux]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.17.linux-amd64.tar.gz
[0.2.16_darwin]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.16.darwin-amd64.tar.gz
[0.2.16_linux]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.16.linux-amd64.tar.gz
[0.2.15_darwin]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.15.darwin-amd64.tar.gz
[0.2.15_linux]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.15.linux-amd64.tar.gz
