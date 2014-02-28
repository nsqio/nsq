### Introduction

**NSQ** is a realtime distributed messaging platform designed to operate at scale, handling
billions of messages per day.

It promotes *distributed* and *decentralized* topologies without single points of failure,
enabling fault tolerance and high availability coupled with a reliable message delivery
guarantee.  See [features & guarantees][features_guarantees].

Operationally, **NSQ** is easy to configure and deploy (all parameters are specified on the command
line and compiled binaries have no runtime dependencies). For maximum flexibility, it is agnostic to
data format (messages can be JSON, MsgPack, Protocol Buffers, or anything else). Official Go and
Python libraries are available out of the box and, if you're interested in building your own client,
there's a [protocol spec][protocol] (see [client libraries][client_libraries]).

### In Production

<center><table class="production"><tr>
<td><a href="http://bitly.com"><img src="{{ site.baseurl }}/static/img/bitly_logo.png" width="67"/></a></td>
<td><a href="http://life360.com"><img src="{{ site.baseurl }}/static/img/life360_logo.png" width="80"/></a></td>
<td><a href="http://hailocab.com"><img src="{{ site.baseurl }}/static/img/hailo_logo.png" width="50"/></a></td>
<td><a href="http://simplereach.com"><img src="{{ site.baseurl }}/static/img/simplereach_logo.png" width="108"/></a></td>

<td><a href="http://moz.com"><img src="{{ site.baseurl }}/static/img/moz_logo.png" width="108"/></a></td>
<td><a href="http://path.com"><img src="{{ site.baseurl }}/static/img/path_logo.png" width="67"/></a></td>
<td><a href="http://trendrr.com"><img src="{{ site.baseurl }}/static/img/trendrr_logo.png" width="77"/></a></td>
<td><a href="http://energyhub.com"><img src="{{ site.baseurl }}/static/img/energyhub_logo.png" width="80"/></a></td>
</tr></table></center>

### Getting Help

If you're having trouble or have questions about **NSQ**, the best place to ask is the [user
group][google_group].

If you're on IRC on freenode, join **#nsq**.

### Issues

All issues should be reported via [github issues][github_issues]. Don't forget to search through the
existing issues to see if that topic has come up before posting.

### Contributing

**NSQ** has a growing community and contributions are always welcome (particularly documentation).
To contribute, fork the project on [github][github_nsq] and send a pull request.

### Contributors

NSQ was designed and developed by Matt Reiferson ([@imsnakes][snakes_twitter]) and Jehiah Czebotar
([@jehiah][jehiah_twitter]) but wouldn't have been possible without the support of [bitly][bitly]
and all our [contributors][contributors].

[features_guarantees]: {{ site.baseurl }}/overview/features_and_guarantees.html
[protocol]: {{ site.baseurl }}/clients/tcp_protocol_spec.html
[client_libraries]: {{ site.baseurl }}/clients/client_libraries.html
[github_issues]: https://github.com/bitly/nsq/issues
[github_nsq]: http://github.com/bitly/nsq
[google_group]: http://groups.google.com/group/nsq-users
[snakes_twitter]: https://twitter.com/imsnakes
[jehiah_twitter]: https://twitter.com/jehiah
[bitly]: https://bitly.com
[contributors]: https://github.com/bitly/nsq/graphs/contributors
