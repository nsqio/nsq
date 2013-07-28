### Introduction

**NSQ** is a realtime distributed messaging platform designed to operate at bitly's scale, handling
billions of messages per day (current peak of 90k+ messages per second).

It promotes *distributed* and *decentralized* topologies without single points of failure,
enabling fault tolerance and high availability coupled with a reliable message delivery
guarantee.  See [features & guarantees][features_guarantees].

Operationally, **NSQ** is easy to configure and deploy (all parameters are specified on the command
line and compiled binaries have no runtime dependencies). For maximum flexibility, it is agnostic to
data format (messages can be JSON, MsgPack, go-protobuf, or anything else). Official Go and Python
libraries are available out of the box and, if you're interested in building your own client,
there's a [protocol spec][protocol] (see [client libraries][client_libraries]).

### Getting Help

If you're having trouble or have questions about **NSQ**, the best place to ask is the [user
group][google_group].

### Issues

All issues should be reported via [github issues][github_issues]. Don't forget to search through the
existing issues to see if that topic has come up before posting.

### Contributing

**NSQ** has growing community and contributions are always welcome (particularly documentation). To
contribute, fork the project on [github][github_nsq] and send a pull request.

### Contributors

 * Dan Frank ([@danielhfrank][dan_twitter])
 * Pierce Lopez ([@ploxiln][pierce_twitter])
 * Will McCutchen ([@mccutchen][mccutch_twitter])
 * Micha Gorelick ([@mynameisfiber][micha_twitter])
 * Jay Ridgeway ([@jayridge][jay_twitter])
 * Todd Levy ([@toddml][todd_twitter])
 * Justin Hines ([@jphines][jphines_twitter])
 * Phillip Rosen ([@phillro][phil_github])
 * David Gardner ([@davidgardnerisme][david_twitter])
 * Clarity Services ([@ClarityServices][clarity_github])
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
 * Will Charczuk ([@wcharczuk][wcharczuk_github])
 * Dominic Wong ([@domwong][domwong_github])

[features_guarantees]: {{ site.baseurl }}/overview/features_and_guarantees.html
[protocol]: {{ site.baseurl }}/clients/tcp_protocol_spec.html
[client_libraries]: {{ site.baseurl }}/clients/client_libraries.html
[github_issues]: https://github.com/bitly/nsq/issues
[github_nsq]: http://github.com/bitly/nsq
[google_group]: http://groups.google.com/group/nsq-users
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
[todd_twitter]: https://twitter.com/toddml
[jay_twitter]: https://twitter.com/jayridge
[clarity_github]: https://github.com/ClarityServices
[michael_github]: https://github.com/michaelhood
[datastream_github]: https://github.com/datastream
[dustismo_github]: https://github.com/dustismo
[funkygao_github]: https://github.com/funkygao
[dmarkham_github]: https://github.com/dmarkham
[fsouza_github]: https://github.com/fsouza
[galvinhsiu_github]: https://github.com/galvinhsiu
[elubow_github]: https://github.com/elubow
[jphines_twitter]: https://twitter.com/jphines
[wcharczuk_github]: https://github.com/wcharczuk
[domwong_github]: https://github.com/domwong
