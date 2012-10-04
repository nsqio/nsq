# Pre-requisites

**install-as** https://github.com/mreiferson/go-install-as

    $ git clone git://github.com/mreiferson/go-install-as.git
    $ cd <repo_root>
    $ make

**simplejson** https://github.com/bitly/go-simplejson

    # installed under a custom import path so you can control versioning
    $ git clone git://github.com/bitly/go-simplejson.git
    $ cd <repo_root>
    $ go tool install_as --import-as=bitly/simplejson

**notify** https://github.com/bitly/go-notify

    # installed under a custom import path so you can control versioning
    $ git clone git://github.com/bitly/go-notify.git
    $ cd <repo_root>
    $ go tool install_as --import-as=bitly/notify

**assert** https://github.com/bmizerany/assert

    $ go get github.com/bmizerany/assert

# Installing

    $ git clone git://github.com/bitly/nsq.git

    # nsq Go package (for building Go readers)
    $ cd <repo_root>/nsq
    $ go tool install_as --import-as=bitly/nsq

    # nsqd binary
    $ cd <repo_root>/nsqd
    $ go build
    $ cp nsqd /usr/local/bin/

    # nsqlookupd binary
    $ cd <repo_root>/nsqlookupd
    $ go build
    $ cp nsqlookupd /usr/local/bin/

    # nsqadmin binary
    $ cd <repo_root>/nsqadmin
    $ go build
    $ cp nsqadmin /usr/local/bin/

    # pynsq Python module (for building Python readers)
    $ cd <repo_root>/pynsq
    $ python setup.py install

# Testing

    $ ./test.sh
