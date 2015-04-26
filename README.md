<p align="center"><img src="http://nsq.io/static/img/nsq.png"/></p>

 * **Source**: [https://github.com/bitly/nsq][github]
 * **Issues**: [https://github.com/bitly/nsq/issues][issues]
 * **Mailing List**: [nsq-users@googlegroups.com](https://groups.google.com/d/forum/nsq-users)
 * **IRC**: #nsq on freenode
 * **Docs**: [http://nsq.io][docs]
 * **Twitter**: [@nsqio][nsqio_twitter]

[![Build Status](https://secure.travis-ci.org/bitly/nsq.svg?branch=master)](http://travis-ci.org/bitly/nsq)

**NSQ** is a realtime distributed messaging platform designed to operate at scale, handling
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

The latest stable release is **[0.3.5][latest_tag]** ([ChangeLog][changelog]). We publish [binary
releases][installing] for linux and darwin.

NOTE: master is our *development* branch and may not be stable at all times.

## In Production

<a href="http://bitly.com"><img src="http://nsq.io/static/img/bitly_logo.png" width="84" align="middle"/></a>&nbsp;&nbsp;
<a href="http://life360.com"><img src="http://nsq.io/static/img/life360_logo.png" width="100" align="middle"/></a>&nbsp;&nbsp;
<a href="http://hailocab.com"><img src="http://nsq.io/static/img/hailo_logo.png" width="62" align="middle"/></a>&nbsp;&nbsp;
<a href="http://simplereach.com"><img src="http://nsq.io/static/img/simplereach_logo.png" width="136" align="middle"/></a>&nbsp;&nbsp;
<a href="http://moz.com"><img src="http://nsq.io/static/img/moz_logo.png" width="108" align="middle"/></a>&nbsp;&nbsp;
<a href="http://path.com"><img src="http://nsq.io/static/img/path_logo.png" width="84" align="middle"/></a><br/>

<a href="http://segment.io"><img src="http://nsq.io/static/img/segmentio_logo.png" width="70" align="middle"/></a>&nbsp;&nbsp;
<a href="http://eventful.com"><img src="http://nsq.io/static/img/eventful_logo.png" width="95" align="middle"/></a>&nbsp;&nbsp;
<a href="http://energyhub.com"><img src="http://nsq.io/static/img/energyhub_logo.png" width="99" align="middle"/></a>&nbsp;&nbsp;
<a href="https://project-fifo.net"><img src="http://nsq.io/static/img/project_fifo.png" width="97" align="middle"/></a>&nbsp;&nbsp;
<a href="http://trendrr.com"><img src="http://nsq.io/static/img/trendrr_logo.png" width="97" align="middle"/></a>&nbsp;&nbsp;
<a href="http://reonomy.com"><img src="http://nsq.io/static/img/reonomy_logo.png" width="100" align="middle"/></a><br/>

<a href="http://dramafever.com"><img src="http://nsq.io/static/img/dramafever.png" width="120" align="middle"/></a>&nbsp;&nbsp;
<a href="http://hw-ops.com"><img src="http://nsq.io/static/img/heavy_water.png" width="50" align="middle"/></a>&nbsp;&nbsp;
<a href="http://lytics.io"><img src="http://nsq.io/static/img/lytics.png" width="100" align="middle"/></a>&nbsp;&nbsp;
<a href="http://mediaforge.com"><img src="http://nsq.io/static/img/rakuten.png" width="100" align="middle"/></a>&nbsp;&nbsp;
<a href="http://socialradar.com"><img src="http://nsq.io/static/img/socialradar_logo.png" width="100" align="middle"/></a>&nbsp;&nbsp;
<a href="http://wistia.com"><img src="http://nsq.io/static/img/wistia_logo.png" width="140" align="middle"/></a><br/>

<a href="http://stripe.com"><img src="http://nsq.io/static/img/stripe_logo.png" width="96" align="middle"/></a>&nbsp;&nbsp;
<a href="http://soundest.com"><img src="http://nsq.io/static/img/soundest_logo.png" width="96" align="middle"/></a>&nbsp;&nbsp;
<a href="http://docker.com"><img src="http://nsq.io/static/img/docker_logo.png" width="100" align="middle"/></a>&nbsp;&nbsp;
<a href="http://getweave.com"><img src="http://nsq.io/static/img/weave_logo.png" width="94" align="middle"/></a>&nbsp;&nbsp;
<a href="http://shipwire.com"><img src="http://nsq.io/static/img/shipwire_logo.png" width="97" align="middle"/></a>&nbsp;&nbsp;
<a href="http://digg.com"><img src="http://nsq.io/static/img/digg_logo.png" width="97" align="middle"/></a><br/>

<a href="http://scalabull.com"><img src="http://nsq.io/static/img/scalabull_logo.png" width="97" align="middle"/></a>&nbsp;&nbsp;
<a href="http://augury.com"><img src="http://nsq.io/static/img/augury_logo.png" width="97" align="middle"/></a>&nbsp;&nbsp;
<a href="http://buzzfeed.com"><img src="http://nsq.io/static/img/buzzfeed_logo.png" width="97" align="middle"/></a>&nbsp;&nbsp;
<a href="http://eztable.com"><img src="http://nsq.io/static/img/eztable_logo.png" width="97" align="middle"/></a>&nbsp;&nbsp;
<a href="http://www.dotabuff.com"><img src="http://nsq.io/static/img/dotabuff_logo.png" width="97" align="middle"/></a>

## Authors

NSQ was designed and developed by Matt Reiferson ([@imsnakes][snakes_twitter]) and Jehiah Czebotar
([@jehiah][jehiah_twitter]) but wouldn't have been possible without the support of
[bitly][bitly] and all our [contributors][contributors].

[docs]: http://nsq.io/
[github]: https://github.com/bitly/nsq
[issues]: https://github.com/bitly/nsq/issues
[changelog]: ChangeLog.md
[protocol]: http://nsq.io/clients/tcp_protocol_spec.html
[installing]: http://nsq.io/deployment/installing.html
[snakes_twitter]: https://twitter.com/imsnakes
[jehiah_twitter]: https://twitter.com/jehiah
[bitly]: https://bitly.com
[features_guarantees]: http://nsq.io/overview/features_and_guarantees.html
[latest_tag]: https://github.com/bitly/nsq/releases/tag/v0.3.5
[contributors]: https://github.com/bitly/nsq/graphs/contributors
[client_libraries]: http://nsq.io/clients/client_libraries.html
[nsqio_twitter]: https://twitter.com/nsqio
