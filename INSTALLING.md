# Prereqs

**install-as** https://github.com/mreiferson/go-install-as

    $ git clone https://github.com/bitly/go-install-as.git
    $ cd go-install-as
    $ make

**simplejson** https://github.com/bitly/go-simplejson

    # installed under a custom import path so you can control versioning
    $ git clone https://github.com/bitly/go-simplejson.git
    $ cd go-simplejson
    $ go tool install_as --import-as=bitly/simplejson

**notify** https://github.com/bitly/go-notify

    # installed under a custom import path so you can control versioning
    $ git clone https://github.com/bitly/go-notify.git
    $ cd go-notify
    $ go tool install_as --import-as=bitly/notify

# installing nsq

    $ cd nsqd
    $ go build

    $ cd ../nsqlookupd
    $ go build

## Dependencies for tests

    $ go get github.com/bmizerany/assert
    $ ./test.sh
