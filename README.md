**NSQ** is a realtime message processing system designed to operate at bitly's scale, handling
billions of messages per day.

It promotes *distributed* and *decentralized* topologies [without single points of failure][spof],
enabling fault tolerance and high availability coupled with a reliable [message delivery
guarantee][message_guarantee].

Operationally, **NSQ** is easy to configure and deploy (all parameters are specified on the command
line and compiled binaries have no runtime dependencies). For maximum flexibility, it is agnostic to
data format (messages can be JSON, [MsgPack][msgpack], [Protocol Buffers][go-protobuf], or anything
else). Official Go and Python libraries are available out of the box and, if you're interested in
building your own client, there's a [protocol spec][protocol] (see [client libraries](#client)).

We publish [binary releases][binary] for linux and darwin.

[![Build Status](https://secure.travis-ci.org/bitly/nsq.png)](http://travis-ci.org/bitly/nsq)

## Why?

**NSQ** was built as a successor to [simplequeue][simplequeue] (part of [simplehttp][simplehttp])
and as such was designed to (in no particular order):

 * provide easy topology solutions that enable high-availability and eliminate SPOFs
 * address the need for stronger message delivery guarantees
 * bound the memory footprint of a single process (by persisting some messages to disk)
 * greatly simplify configuration requirements for producers and consumers
 * provide a straightforward upgrade path
 * improve efficiency

If you're interested in more of the design, history, and evolution please read our [design
doc][design] or [blog post][nsq_post].

## <a name="client"></a>Client Libraries

* [Go (official)][nsq]
* [Python (official)][pynsq]
* [Node.js][node_lib]
* [PHP][php_lib]
* [Ruby][ruby_lib]

## Additional Documentation

**NSQ** is composed of the following individual components:

 * [nsqd][nsqd] is the daemon that receives, buffers, and delivers messages to clients.
 * [nsqlookupd][nsqlookupd] is the daemon that manages topology information
 * [nsqadmin][nsqadmin] is the web UI to view message statistics and perform administrative tasks
 * [nsq][nsq] is a go package for writing `nsqd` clients
 * [pynsq][pynsq] is a python module for writing `nsqd` clients

There is also a [protocol spec][protocol].

### Getting Started

The following steps will run **NSQ** on your local machine and walk through publishing, consuming,
and archiving messages to disk.

 1. follow the instructions in the [INSTALLING][installing] doc (or [download a binary
    release][binary]).
 2. in one shell, start `nsqlookupd`:
        
        $ nsqlookupd

 3. in another shell, start `nsqd`:

        $ nsqd --lookupd-tcp-address=127.0.0.1:4160

 4. in another shell, start `nsqadmin`:

        $ nsqadmin --lookupd-http-address=127.0.0.1:4161

 5. finally, in another shell, start `nsq_to_file`:

        $ nsq_to_file --topic=test --output-dir=/tmp --lookupd-http-address=127.0.0.1:4161

 6. publish some messages to `nsqd`:

        $ curl -d 'hello world 1' 'http://127.0.0.1:4151/put?topic=test'
        $ curl -d 'hello world 2' 'http://127.0.0.1:4151/put?topic=test'
        $ curl -d 'hello world 3' 'http://127.0.0.1:4151/put?topic=test'

 7. to verify things worked as expected, in a web browser open `http://127.0.0.1:4171/` to view 
    the `nsqadmin` UI and see statistics.  Also, check the contents of the log files (`test.*.log`) 
    written to `/tmp`.

The important lesson here is that `nsq_to_file` (the client) is not explicitly told where the `test`
topic is produced, it retrieves this information from `nsqlookupd`.

## Authors

NSQ was designed and developed by Matt Reiferson ([@imsnakes][snakes_twitter]) and Jehiah Czebotar
([@jehiah][jehiah_twitter]) but wouldn't have been possible without the support of
[bitly][bitly]:

 * Dan Frank ([@danielhfrank][dan_twitter])
 * Pierce Lopez ([@ploxiln][pierce_twitter])
 * Will McCutchen ([@mccutchen][mccutch_twitter])
 * Micha Gorelick ([@mynameisfiber][micha_twitter])
 * Jay Ridgeway ([@jayridge][jay_twitter])

### Contributors

 * Phillip Rosen ([@phillro][phil_github]) for the [Node.js Client Library][node_lib]
 * David Gardner ([@davidgardnerisme][david_twitter]) for the [PHP Client Library][php_lib]
 * Clarity Services ([@ClarityServices][clarity_github]) for the [Ruby Client Library][ruby_lib]
 * Harley Laue ([@losinggeneration][harley_github])
 * Justin Azoff ([@JustinAzoff][justin_github])

[simplehttp]: https://github.com/bitly/simplehttp
[msgpack]: http://msgpack.org/
[go-protobuf]: http://code.google.com/p/protobuf/
[simplequeue]: https://github.com/bitly/simplehttp/tree/master/simplequeue
[protocol]: https://github.com/bitly/nsq/blob/master/docs/protocol.md
[installing]: https://github.com/bitly/nsq/blob/master/INSTALLING.md
[nsqd]: https://github.com/bitly/nsq/tree/master/nsqd
[nsqlookupd]: https://github.com/bitly/nsq/tree/master/nsqlookupd
[nsqadmin]: https://github.com/bitly/nsq/tree/master/nsqadmin
[nsq]: https://github.com/bitly/nsq/tree/master/nsq
[pynsq]: https://github.com/bitly/nsq/tree/master/pynsq
[nsq_post]: http://word.bitly.com/post/33232969144/nsq
[binary]: https://github.com/bitly/nsq/downloads
[snakes_twitter]: https://twitter.com/imsnakes
[jehiah_twitter]: https://twitter.com/jehiah
[dan_twitter]: https://twitter.com/danielhfrank
[pierce_twitter]: https://twitter.com/ploxiln
[mccutch_twitter]: https://twitter.com/mccutchen
[micha_twitter]: https://twitter.com/mynameisfiber
[harley_github]: https://github.com/losinggeneration
[david_twitter]: https://twitter.com/davegardnerisme
[justin_github]: https://github.com/JustinAzoff
[phil_github]: https://github.com/phillro
[node_lib]: https://github.com/phillro/nodensq
[php_lib]: https://github.com/davegardnerisme/nsqphp
[bitly]: https://bitly.com
[jay_twitter]: https://twitter.com/jayridge
[ruby_lib]: https://github.com/ClarityServices/ruby_nsq
[clarity_github]: https://github.com/ClarityServices
[spof]: https://github.com/bitly/nsq/blob/master/docs/design.md#spof
[message_guarantee]: https://github.com/bitly/nsq/blob/master/docs/design.md#delivery
[design]: https://github.com/bitly/nsq/blob/master/docs/design.md

