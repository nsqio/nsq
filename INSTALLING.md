
# Prereqs

get `go install_as` https://github.com/mreiferson/go-install-as

simplejson (installed under a custom import path so you can always install some other version as that import path)

    $ git clone https://github.com/bitly/go-simplejson.git
    $ cd go-simplejson
    $ go tool install_as --import-as=bitly/simplejson

# installing nsq

   $ cd nsqd
   $ go build

   $ cd ../nsqlookupd
   $ go build

   $ cd ../nsqreader
   $ go test

