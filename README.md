<p align="center"><img src="docs/images/nsq.png"/></p>

 * **Source**: [https://github.com/bitly/nsq][github]
 * **Issues**: [https://github.com/bitly/nsq/issues][issues]
 * **Mailing List**: [nsq-users@googlegroups.com](https://groups.google.com/d/forum/nsq-users)
 * **Twitter**: [@imsnakes][snakes_twitter] or [@jehiah][jehiah_twitter]

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

The latest stable release is **[0.2.21][latest_tag]** ([ChangeLog][changelog]). We publish [binary
releases][binary] for linux and darwin.

NOTE: master is our *development* branch and may *not* be stable at all times.

[![Build Status](https://secure.travis-ci.org/bitly/nsq.png?branch=master)](http://travis-ci.org/bitly/nsq)

## In Production

<center><table><tr>
<td><a href="http://bitly.com"><img src="docs/images/bitly_logo.png" width="84"/></a></td>
<td><a href="http://life360.com"><img src="docs/images/life360_logo.png" width="100"/></a></td>
<td><a href="http://hailocab.com"><img src="docs/images/hailo_logo.png" width="62"/></a></td>
<td><a href="http://path.com"><img src="docs/images/path_logo.png" width="84"/></a></td>
<td><a href="http://trendrr.com"><img src="docs/images/trendrr_logo.png" width="97"/></a></td>
<td><a href="http://simplereach.com"><img src="docs/images/simplereach_logo.png" width="136"/></a></td>
<td><a href="http://energyhub.com"><img src="docs/images/energyhub_logo.png" width="99"/></a></td>
</table></tr><center>

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

## Features

 * no SPOF, designed for distributed environments
 * messages are guaranteed to be delivered *at least once*
 * low-latency push based message delivery (<a href="#performance">performance</a>)
 * combination load-balanced *and* multicast style message routing
 * configurable high-water mark after which messages are transparently kept on disk
 * few dependencies, easy to deploy, and sane, bounded, default configuration
 * runtime discovery service for consumers to find producers ([nsqlookupd][nsqlookupd])
 * HTTP interface for stats, administrative actions, and producers (no client libraries needed!)
 * memcached-like TCP protocol for producers/consumers
 * integrates with [statsd][statsd] for realtime metrics instrumentation
 * robust cluster administration interface with [graphite][graphite] charts ([nsqadmin][nsqadmin])

## <a name="client"></a>Client Libraries

* [nsq][nsq] Go (official)
* [pynsq][pynsq] Python (official) [pypi][pynsq_pypi]
* [libnsq][libnsq] C
* [nsq-java][nsq-java] Java
* [TrendrrNSQClient][TrendrrNSQClient] Java
* [nsq-client][node2_lib] Node.js [npm][nsq-client_npm]
* [nodensq][node_lib] Node.js [npm][nodensq_npm]
* [nsqphp][php_lib] PHP
* [ruby_nsq][ruby_lib] Ruby [rubygems][ruby_nsq_rubygems]

## Additional Documentation

**NSQ** is composed of the following individual components, each with their own README:

 * [nsqd][nsqd] is the daemon that receives, buffers, and delivers messages to clients.
 * [nsqlookupd][nsqlookupd] is the daemon that manages topology information
 * [nsqadmin][nsqadmin] is the web UI to view message statistics and perform administrative tasks
 * [nsq][nsq] is a go package for writing `nsqd` clients

For more information see the [docs][docs] directory.

### <a name="performance"></a>Performance

DISCLAIMER: Please keep in mind that NSQ is designed to be used in a distributed fashion. Single
node performance is important, but not the end-all-be-all of what we're looking to achieve. Also,
benchmarks are stupid, but here's a few anyway to ignite the flame:

On a 2012 MacBook Air i7 2ghz (`GOMAXPROCS=1`, `go 1.1 beta2 4a712e80e9b1`, NSQ v0.2.19-alpha) 
single publisher, single consumer:

```
$ ./nsqd --mem-queue-size=1000000

$ ./bench_writer
2013/04/09 23:25:54 duration: 2.46904784s - 77.250mb/s - 405014.429ops/s - 2.469us/op

$ ./bench_reader
2013/04/09 23:27:53 duration: 5.996050461s - 31.810mb/s - 166776.448ops/s - 5.996us/op
```

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

 5. publish an initial message (creates the topic in the cluster, too):
 
        $ curl -d 'hello world 1' 'http://127.0.0.1:4151/put?topic=test'

 6. finally, in another shell, start `nsq_to_file`:

        $ nsq_to_file --topic=test --output-dir=/tmp --lookupd-http-address=127.0.0.1:4161

 7. publish more messages to `nsqd`:

        $ curl -d 'hello world 2' 'http://127.0.0.1:4151/put?topic=test'
        $ curl -d 'hello world 3' 'http://127.0.0.1:4151/put?topic=test'

 8. to verify things worked as expected, in a web browser open `http://127.0.0.1:4171/` to view 
    the `nsqadmin` UI and see statistics.  Also, check the contents of the log files (`test.*.log`) 
    written to `/tmp`.

The important lesson here is that `nsq_to_file` (the client) is not explicitly told where the `test`
topic is produced, it retrieves this information from `nsqlookupd` and, despite the timing of the
connection, no messages are lost.

## Authors

NSQ was designed and developed by Matt Reiferson ([@imsnakes][snakes_twitter]) and Jehiah Czebotar
([@jehiah][jehiah_twitter]) but wouldn't have been possible without the support of
[bitly][bitly]:

 * Dan Frank ([@danielhfrank][dan_twitter])
 * Pierce Lopez ([@ploxiln][pierce_twitter])
 * Will McCutchen ([@mccutchen][mccutch_twitter])
 * Micha Gorelick ([@mynameisfiber][micha_twitter])
 * Jay Ridgeway ([@jayridge][jay_twitter])
 * Justin Hines ([@jphines][jphines_twitter])

### Contributors

 * Phillip Rosen ([@phillro][phil_github]) for the [Node.js Client Library][node_lib]
 * David Gardner ([@davidgardnerisme][david_twitter]) for the [PHP Client Library][php_lib]
 * Clarity Services ([@ClarityServices][clarity_github]) for the [Ruby Client Library][ruby_lib]
 * Harley Laue ([@losinggeneration][harley_github])
 * Justin Azoff ([@JustinAzoff][justin_github])
 * Michael Hood ([@michaelhood][michael_github])
 * Xianjie ([@datastream][datastream_github])
 * Dustin Norlander ([@dustismo][dustismo_github])
 * Funky Gao ([@funkygao][funkygao_github])
 * Dan Markham ([@dmarkham][dmarkham_github])
 * Francisco Souza ([@fsouza][fsouza_github])
 * galvinhsiu ([@galvinhsiu][galvinhsiu_github])
 * Eric Lubow ([@elubow][elubow_github])

[github]: https://github.com/bitly/nsq
[issues]: https://github.com/bitly/nsq/issues
[simplehttp]: https://github.com/bitly/simplehttp
[msgpack]: http://msgpack.org/
[go-protobuf]: http://code.google.com/p/protobuf/
[simplequeue]: https://github.com/bitly/simplehttp/tree/master/simplequeue
[changelog]: ChangeLog.md
[protocol]: docs/protocol.md
[installing]: INSTALLING.md
[nsqd]: nsqd/README.md
[nsqlookupd]: nsqlookupd/README.md
[nsqadmin]: nsqadmin/README.md
[nsq]: nsq/README.md
[pynsq]: https://github.com/bitly/pynsq
[nsq_post]: http://word.bitly.com/post/33232969144/nsq
[binary]: INSTALLING.md#binary
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
[spof]: docs/design.md#spof
[message_guarantee]: docs/design.md#delivery
[design]: docs/design.md
[docs]: docs
[patterns]: docs/patterns.md
[latest_tag]: https://github.com/bitly/nsq/tree/v0.2.21
[pynsq_pypi]: http://pypi.python.org/pypi/pynsq
[nodensq_npm]: https://npmjs.org/package/nsq
[ruby_nsq_rubygems]: http://rubygems.org/gems/ruby_nsq
[libnsq]: https://github.com/mreiferson/libnsq
[nsq-java]: https://github.com/bitly/nsq-java
[michael_github]: https://github.com/michaelhood
[datastream_github]: https://github.com/datastream
[dustismo_github]: https://github.com/dustismo
[funkygao_github]: https://github.com/funkygao
[statsd]: https://github.com/etsy/statsd/
[graphite]: http://graphite.wikidot.com/
[TrendrrNSQClient]: https://github.com/dustismo/TrendrrNSQClient
[dmarkham_github]: https://github.com/dmarkham
[fsouza_github]: https://github.com/fsouza
[node2_lib]: https://github.com/jmanero/nsq-client
[nsq-client_npm]: https://npmjs.org/package/nsq-client
[galvinhsiu_github]: https://github.com/galvinhsiu
[elubow_github]: https://github.com/elubow
[jphines_twitter]: https://twitter.com/jphines
