<p align="center">
<img align="left" width="175" src="http://nsq.io/static/img/nsq_blue.png">
<ul>
<li><strong>Source</strong>: https://github.com/nsqio/nsq
<li><strong>Issues</strong>: https://github.com/nsqio/nsq/issues
<li><strong>Mailing List</strong>: <a href="https://groups.google.com/d/forum/nsq-users">nsq-users@googlegroups.com</a>
<li><strong>IRC</strong>: #nsq on freenode
<li><strong>Docs</strong>: http://nsq.io
<li><strong>Twitter</strong>: <a href="https://twitter.com/nsqio">@nsqio</a>
</ul>
</p>

[![Build Status](https://secure.travis-ci.org/nsqio/nsq.svg?branch=master)](http://travis-ci.org/nsqio/nsq) [![GitHub release](https://img.shields.io/github/release/nsqio/nsq.svg)](https://github.com/nsqio/nsq/releases/latest) [![Coverage Status](https://coveralls.io/repos/github/nsqio/nsq/badge.svg?branch=master)](https://coveralls.io/github/nsqio/nsq?branch=master)

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

We publish [binary releases][installing] for linux and darwin.

NOTE: master is our *development* branch and may not be stable at all times.

## In Production

<a href="http://bitly.com"><img src="http://nsq.io/static/img/bitly_logo.png" width="84" align="middle"/></a>&nbsp;&nbsp;
<a href="https://www.life360.com/"><img src="http://nsq.io/static/img/life360_logo.png" width="100" align="middle"/></a>&nbsp;&nbsp;
<a href="https://www.hailoapp.com/"><img src="http://nsq.io/static/img/hailo_logo.png" width="62" align="middle"/></a>&nbsp;&nbsp;
<a href="http://www.simplereach.com/"><img src="http://nsq.io/static/img/simplereach_logo.png" width="136" align="middle"/></a>&nbsp;&nbsp;
<a href="https://moz.com/"><img src="http://nsq.io/static/img/moz_logo.png" width="108" align="middle"/></a>&nbsp;&nbsp;
<a href="https://path.com/"><img src="http://nsq.io/static/img/path_logo.png" width="84" align="middle"/></a><br/>

<a href="https://segment.com/"><img src="http://nsq.io/static/img/segmentio_logo.png" width="70" align="middle"/></a>&nbsp;&nbsp;
<a href="http://eventful.com/events"><img src="http://nsq.io/static/img/eventful_logo.png" width="95" align="middle"/></a>&nbsp;&nbsp;
<a href="http://www.energyhub.com"><img src="http://nsq.io/static/img/energyhub_logo.png" width="99" align="middle"/></a>&nbsp;&nbsp;
<a href="https://project-fifo.net"><img src="http://nsq.io/static/img/project_fifo.png" width="97" align="middle"/></a>&nbsp;&nbsp;
<a href="http://trendrr.com"><img src="http://nsq.io/static/img/trendrr_logo.png" width="97" align="middle"/></a>&nbsp;&nbsp;
<a href="https://reonomy.com/"><img src="http://nsq.io/static/img/reonomy_logo.png" width="100" align="middle"/></a><br/>

<a href="http://hw-ops.com"><img src="http://nsq.io/static/img/heavy_water.png" width="50" align="middle"/></a>&nbsp;&nbsp;
<a href="http://www.getlytics.com/"><img src="http://nsq.io/static/img/lytics.png" width="100" align="middle"/></a>&nbsp;&nbsp;
<a href="http://mediaforge.com"><img src="http://nsq.io/static/img/rakuten.png" width="100" align="middle"/></a>&nbsp;&nbsp;
<a href="http://socialradar.com"><img src="http://nsq.io/static/img/socialradar_logo.png" width="100" align="middle"/></a>&nbsp;&nbsp;
<a href="http://wistia.com"><img src="http://nsq.io/static/img/wistia_logo.png" width="140" align="middle"/></a>&nbsp;&nbsp;
<a href="https://stripe.com/"><img src="http://nsq.io/static/img/stripe_logo.png" width="96" align="middle"/></a><br/>

<a href="https://www.soundest.com/"><img src="http://nsq.io/static/img/soundest_logo.png" width="96" align="middle"/></a>&nbsp;&nbsp;
<a href="https://www.docker.com/"><img src="http://nsq.io/static/img/docker_logo.png" width="100" align="middle"/></a>&nbsp;&nbsp;
<a href="http://www.getweave.com/"><img src="http://nsq.io/static/img/weave_logo.png" width="94" align="middle"/></a>&nbsp;&nbsp;
<a href="http://www.shipwire.com"><img src="http://nsq.io/static/img/shipwire_logo.png" width="97" align="middle"/></a>&nbsp;&nbsp;
<a href="http://digg.com"><img src="http://nsq.io/static/img/digg_logo.png" width="97" align="middle"/></a>&nbsp;&nbsp;
<a href="http://www.scalabull.com/"><img src="http://nsq.io/static/img/scalabull_logo.png" width="97" align="middle"/></a><br/>

<a href="http://www.augury.com/"><img src="http://nsq.io/static/img/augury_logo.png" width="97" align="middle"/></a>&nbsp;&nbsp;
<a href="http://www.buzzfeed.com/"><img src="http://nsq.io/static/img/buzzfeed_logo.png" width="97" align="middle"/></a>&nbsp;&nbsp;
<a href="http://eztable.com"><img src="http://nsq.io/static/img/eztable_logo.png" width="97" align="middle"/></a>&nbsp;&nbsp;
<a href="http://www.dotabuff.com/"><img src="http://nsq.io/static/img/dotabuff_logo.png" width="97" align="middle"/></a>&nbsp;&nbsp;
<a href="https://www.fastly.com/"><img src="http://nsq.io/static/img/fastly_logo.png" width="97" align="middle"/></a>&nbsp;&nbsp;
<a href="https://talky.io"><img src="http://nsq.io/static/img/talky_logo.png" width="97" align="middle"/></a><br/>

<a href="https://groupme.com"><img src="http://nsq.io/static/img/groupme_logo.png" width="97" align="middle"/></a>&nbsp;&nbsp;
<a href="https://deis.com"><img src="http://nsq.io/static/img/deis_logo.svg" width="75" align="middle"/></a>&nbsp;&nbsp;
<a href="https://wiredcraft.com"><img src="http://nsq.io/static/img/wiredcraft_logo.jpg" width="97" align="middle"/></a>&nbsp;&nbsp;
<a href="https://sproutsocial.com"><img src="http://nsq.io/static/img/sproutsocial_logo.png" width="90" align="middle"/></a>&nbsp;&nbsp;

## Code of Conduct

Help us keep NSQ open and inclusive. Please read and follow our [Code of Conduct](CODE_OF_CONDUCT.md).

## Authors

NSQ was designed and developed by Matt Reiferson ([@imsnakes][snakes_twitter]) and Jehiah Czebotar
([@jehiah][jehiah_twitter]) but wouldn't have been possible without the support of
[bitly][bitly] and all our [contributors][contributors].

Logo created by Wolasi Konu ([@kisalow][wolasi_twitter]).

[protocol]: http://nsq.io/clients/tcp_protocol_spec.html
[installing]: http://nsq.io/deployment/installing.html
[snakes_twitter]: https://twitter.com/imsnakes
[jehiah_twitter]: https://twitter.com/jehiah
[bitly]: https://bitly.com
[features_guarantees]: http://nsq.io/overview/features_and_guarantees.html
[contributors]: https://github.com/nsqio/nsq/graphs/contributors
[client_libraries]: http://nsq.io/clients/client_libraries.html
[wolasi_twitter]: https://twitter.com/kisalow
