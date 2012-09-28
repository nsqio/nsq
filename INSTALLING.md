# Pre-requisites

**simplejson** https://github.com/bitly/go-simplejson

    $ go get github.com/bitly/go-simplejson

**notify** https://github.com/bitly/go-notify

    $ go get github.com/bitly/go-notify

**assert** https://github.com/bmizerany/assert

    $ go get github.com/bmizerany/assert

# Installing

Binaries (`nsqd`, `nsqlookupd`, `nsqadmin`, and all example apps)

Note: Binaries can not be built from within $GOPATH because of relative imports. To build, checkout to a directory
outside of $GOPATH

    $ git clone https://github.com/bitly/nsq.git
    $ cd $REPO
    $ ./install.sh

Go package (for building Go readers)

    $ go get github.com/bitly/nsq/nsq

Python module (for building Python readers)

    $ pip install pynsq

# Testing

    $ ./test.sh
