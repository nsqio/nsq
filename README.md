<p align="center"><img src="http://bitly.github.io/nsq/static/img/nsq.png"/></p>

 * **Source**: [https://github.com/bitly/nsq][github]
 * **Issues**: [https://github.com/bitly/nsq/issues][issues]
 * **Mailing List**: [nsq-users@googlegroups.com](https://groups.google.com/d/forum/nsq-users)
 * **Docs**: [http://bitly.github.io/nsq][docs]
 * **Twitter**: [@imsnakes][snakes_twitter] or [@jehiah][jehiah_twitter]

**NSQ** is a realtime message processing system designed to operate at bitly's scale, handling
billions of messages per day.

It promotes *distributed* and *decentralized* topologies without single points of failure,
enabling fault tolerance and high availability coupled with a reliable message delivery
guarantee.  See [features & guarantees][features_guarantees].

Operationally, **NSQ** is easy to configure and deploy (all parameters are specified on the command
line and compiled binaries have no runtime dependencies). For maximum flexibility, it is agnostic to
data format (messages can be JSON, MsgPack, Protocol Buffers, or anything else). Official Go and
Python libraries are available out of the box (as well as many other [client
libraries][client_libraries]) and, if you're interested in building your own, there's a [protocol
spec][protocol].

The latest stable release is **[0.2.23][latest_tag]** ([ChangeLog][changelog]). We publish [binary
releases][installing] for linux and darwin.

NOTE: master is our *development* branch and may not be stable at all times.

[![Build Status](https://secure.travis-ci.org/bitly/nsq.png?branch=master)](http://travis-ci.org/bitly/nsq)

## In Production

<center><table><tr>
<td><a href="http://bitly.com"><img src="http://bitly.github.io/nsq/static/img/bitly_logo.png" width="84"/></a></td>
<td><a href="http://life360.com"><img src="http://bitly.github.io/nsq/static/img/life360_logo.png" width="100"/></a></td>
<td><a href="http://hailocab.com"><img src="http://bitly.github.io/nsq/static/img/hailo_logo.png" width="62"/></a></td>
<td><a href="http://path.com"><img src="http://bitly.github.io/nsq/static/img/path_logo.png" width="84"/></a></td>
<td><a href="http://trendrr.com"><img src="http://bitly.github.io/nsq/static/img/trendrr_logo.png" width="97"/></a></td>
<td><a href="http://simplereach.com"><img src="http://bitly.github.io/nsq/static/img/simplereach_logo.png" width="136"/></a></td>
<td><a href="http://energyhub.com"><img src="http://bitly.github.io/nsq/static/img/energyhub_logo.png" width="99"/></a></td>
</tr></table></center>

## Documentation

Online documentation is available at [http://bitly.github.io/nsq][docs]

Offline documentation requires [jekyll][jekyll]:

```bash
$ gem install jekyll
$ git checkout gh-pages
$ jekyll serve --safe --baseurl ''
```

## Authors

NSQ was designed and developed by Matt Reiferson ([@imsnakes][snakes_twitter]) and Jehiah Czebotar
([@jehiah][jehiah_twitter]) but wouldn't have been possible without the support of
[bitly][bitly] and all our [contributors][contributors].

[docs]: http://bitly.github.io/nsq
[github]: https://github.com/bitly/nsq
[issues]: https://github.com/bitly/nsq/issues
[changelog]: ChangeLog.md
[protocol]: http://bitly.github.io/nsq/clients/tcp_protocol_spec.html
[installing]: http://bitly.github.io/nsq/deployment/installing.html
[snakes_twitter]: https://twitter.com/imsnakes
[jehiah_twitter]: https://twitter.com/jehiah
[bitly]: https://bitly.com
[features_guarantees]: http://bitly.github.io/nsq/overview/features_and_guarantees.html
[latest_tag]: https://github.com/bitly/nsq/tree/v0.2.23
[contributors]: https://github.com/bitly/nsq/graphs/contributors
[client_libraries]: http://bitly.github.io/nsq/clients/client_libraries.html
[jekyll]: http://jekyllrb.com/
