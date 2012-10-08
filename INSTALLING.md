# Pre-requisites

**install-as** https://github.com/mreiferson/go-install-as

    $ git clone git://github.com/mreiferson/go-install-as.git
    $ cd $REPO
    $ make

**simplejson** https://github.com/bitly/go-simplejson

    # installed under a custom import path so you can control versioning
    $ git clone git://github.com/bitly/go-simplejson.git
    $ cd $REPO
    $ go tool install_as --import-as=bitly/simplejson

**notify** https://github.com/bitly/go-notify

    # installed under a custom import path so you can control versioning
    $ git clone git://github.com/bitly/go-notify.git
    $ cd $REPO
    $ go tool install_as --import-as=bitly/notify

**assert** https://github.com/bmizerany/assert

    $ go get github.com/bmizerany/assert

# Installing

Binaries (`nsqd`, `nsqlookupd`, `nsqadmin`, and all example apps)

    $ git clone git://github.com/bitly/nsq.git
    $ cd $REPO
    $ ./install.sh

Go package (for building Go readers)

    $ cd $REPO/nsq
    $ go tool install_as --import-as=bitly/nsq

Python module (for building Python readers)

    $ pip install pynsq

# Testing

    $ ./test.sh
