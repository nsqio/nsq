<p align="center">
<img align="left" width="175" src="https://nsq.io/static/img/nsq_blue.png">
<ul>
<li><strong>Source</strong>: https://github.com/nsqio/nsq
<li><strong>Issues</strong>: https://github.com/nsqio/nsq/issues
<li><strong>Mailing List</strong>: <a href="https://groups.google.com/d/forum/nsq-users">nsq-users@googlegroups.com</a>
<li><strong>IRC</strong>: #nsq on freenode
<li><strong>Docs</strong>: https://nsq.io
<li><strong>Twitter</strong>: <a href="https://twitter.com/nsqio">@nsqio</a>
</ul>
</p>

[![Build Status](https://github.com/nsqio/nsq/workflows/tests/badge.svg)](https://github.com/nsqio/nsq/actions) [![GitHub release](https://img.shields.io/github/release/nsqio/nsq.svg)](https://github.com/nsqio/nsq/releases/latest) [![Coverage Status](https://coveralls.io/repos/github/nsqio/nsq/badge.svg?branch=master)](https://coveralls.io/github/nsqio/nsq?branch=master)

**NSQ** is a realtime distributed messaging platform designed to operate at scale, handling
billions of messages per day.

It promotes *distributed* and *decentralized* topologies without single points of failure,
enabling fault tolerance and high availability coupled with a reliable message delivery
guarantee.  See [features & guarantees][features_guarantees].

Operationally, **NSQ** is easy to configure and deploy (all parameters are specified on the command
line and compiled binaries have no runtime dependencies). For maximum flexibility, it is agnostic to
data format (messages can be JSON, MsgPack, Protocol Buffers, or anything else). Official Go and
Python libraries are available out of the box (as well as many other [client
libraries][client_libraries]), and if you're interested in building your own, there's a [protocol
spec][protocol].

We publish [binary releases][installing] for Linux, Darwin, FreeBSD and Windows, as well as an official [Docker image][docker_deployment].

NOTE: master is our *development* branch and may not be stable at all times.

## In Production

<a href="https://bitly.com/"><img src="https://nsq.io/static/img/bitly_logo.png" width="84" align="middle"/></a>&nbsp;&nbsp;
<a href="https://www.life360.com/"><img src="https://nsq.io/static/img/life360_logo.png" width="100" align="middle"/></a>&nbsp;&nbsp;
<a href="https://www.simplereach.com/"><img src="https://nsq.io/static/img/simplereach_logo.png" width="136" align="middle"/></a>&nbsp;&nbsp;
<a href="https://moz.com/"><img src="https://nsq.io/static/img/moz_logo.png" width="108" align="middle"/></a>&nbsp;&nbsp;
<a href="https://segment.com/"><img src="https://nsq.io/static/img/segment_logo.png" width="70" align="middle"/></a>&nbsp;&nbsp;
<a href="https://eventful.com/events"><img src="https://nsq.io/static/img/eventful_logo.png" width="95" align="middle"/></a><br/>

<a href="https://www.energyhub.com/"><img src="https://nsq.io/static/img/energyhub_logo.png" width="99" align="middle"/></a>&nbsp;&nbsp;
<a href="https://project-fifo.net/"><img src="https://nsq.io/static/img/project_fifo.png" width="97" align="middle"/></a>&nbsp;&nbsp;
<a href="https://trendrr.com/"><img src="https://nsq.io/static/img/trendrr_logo.png" width="97" align="middle"/></a>&nbsp;&nbsp;
<a href="https://reonomy.com/"><img src="https://nsq.io/static/img/reonomy_logo.png" width="100" align="middle"/></a>&nbsp;&nbsp;
<a href="https://hw-ops.com/"><img src="https://nsq.io/static/img/heavy_water.png" width="50" align="middle"/></a>&nbsp;&nbsp;
<a href="https://www.getlytics.com/"><img src="https://nsq.io/static/img/lytics.png" width="100" align="middle"/></a><br/>

<a href="https://mediaforge.com/"><img src="https://nsq.io/static/img/rakuten.png" width="100" align="middle"/></a>&nbsp;&nbsp;
<a href="https://wistia.com/"><img src="https://nsq.io/static/img/wistia_logo.png" width="140" align="middle"/></a>&nbsp;&nbsp;
<a href="https://stripe.com/"><img src="https://nsq.io/static/img/stripe_logo.png" width="96" align="middle"/></a>&nbsp;&nbsp;
<a href="https://www.shipwire.com/"><img src="https://nsq.io/static/img/shipwire_logo.png" width="97" align="middle"/></a>&nbsp;&nbsp;
<a href="https://digg.com/"><img src="https://nsq.io/static/img/digg_logo.png" width="97" align="middle"/></a>&nbsp;&nbsp;
<a href="https://www.scalabull.com/"><img src="https://nsq.io/static/img/scalabull_logo.png" width="97" align="middle"/></a><br/>

<a href="https://www.soundest.com/"><img src="https://nsq.io/static/img/soundest_logo.png" width="96" align="middle"/></a>&nbsp;&nbsp;
<a href="https://www.docker.com/"><img src="https://nsq.io/static/img/docker_logo.png" width="100" align="middle"/></a>&nbsp;&nbsp;
<a href="https://www.getweave.com/"><img src="https://nsq.io/static/img/weave_logo.png" width="94" align="middle"/></a>&nbsp;&nbsp;
<a href="https://www.augury.com/"><img src="https://nsq.io/static/img/augury_logo.png" width="97" align="middle"/></a>&nbsp;&nbsp;
<a href="https://www.buzzfeed.com/"><img src="https://nsq.io/static/img/buzzfeed_logo.png" width="97" align="middle"/></a>&nbsp;&nbsp;
<a href="https://eztable.com/"><img src="https://nsq.io/static/img/eztable_logo.png" width="97" align="middle"/></a><br/>

<a href="https://www.dotabuff.com/"><img src="https://nsq.io/static/img/dotabuff_logo.png" width="97" align="middle"/></a>&nbsp;&nbsp;
<a href="https://www.fastly.com/"><img src="https://nsq.io/static/img/fastly_logo.png" width="97" align="middle"/></a>&nbsp;&nbsp;
<a href="https://talky.io/"><img src="https://nsq.io/static/img/talky_logo.png" width="97" align="middle"/></a>&nbsp;&nbsp;
<a href="https://groupme.com/"><img src="https://nsq.io/static/img/groupme_logo.png" width="97" align="middle"/></a>&nbsp;&nbsp;
<a href="https://wiredcraft.com/"><img src="https://nsq.io/static/img/wiredcraft_logo.jpg" width="97" align="middle"/></a>&nbsp;&nbsp;
<a href="https://sproutsocial.com/"><img src="https://nsq.io/static/img/sproutsocial_logo.png" width="90" align="middle"/></a><br/>

<a href="https://fandom.wikia.com/"><img src="https://nsq.io/static/img/fandom_logo.svg" width="100" align="middle"/></a>&nbsp;&nbsp;
<a href="https://gitee.com/"><img src="https://nsq.io/static/img/gitee_logo.svg" width="140" align="middle"/></a>&nbsp;&nbsp;
<a href="https://bytedance.com/"><img src="https://nsq.io/static/img/bytedance_logo.png" width="140" align="middle"/></a>&nbsp;&nbsp;
<a href="https://www.tokopedia.com/"><img src="https://nsq.io/static/img/tokopedia_logo.svg" width="145" align="middle"/></a><br/>
<a href="https://www.kuafood.com/"><img src="https://nsq.io/static/img/kuafood_logo.png" width="145" align="middle"/></a><br/>

## Code of Conduct

Help us keep NSQ open and inclusive. Please read and follow our [Code of Conduct](CODE_OF_CONDUCT.md).

## Authors

NSQ was designed and developed by Matt Reiferson ([@imsnakes][snakes_twitter]) and Jehiah Czebotar
([@jehiah][jehiah_twitter]) but wouldn't have been possible without the support of [Bitly][bitly],
maintainers ([Pierce Lopez][pierce_github]), and all our [contributors][contributors].

Logo created by Wolasi Konu ([@kisalow][wolasi_twitter]).

[protocol]: https://nsq.io/clients/tcp_protocol_spec.html
[installing]: https://nsq.io/deployment/installing.html
[docker_deployment]: https://nsq.io/deployment/docker.html
[snakes_twitter]: https://twitter.com/imsnakes
[jehiah_twitter]: https://twitter.com/jehiah
[bitly]: https://bitly.com
[features_guarantees]: https://nsq.io/overview/features_and_guarantees.html
[contributors]: https://github.com/nsqio/nsq/graphs/contributors
[client_libraries]: https://nsq.io/clients/client_libraries.html
[wolasi_twitter]: https://twitter.com/kisalow
[pierce_github]: https://github.com/ploxiln
