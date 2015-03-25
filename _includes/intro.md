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
<td align="center"><a href="http://bitly.com"><img src="{{ site.baseurl }}/static/img/bitly_logo.png" width="84"/></a></td>
<td align="center"><a href="http://life360.com"><img src="{{ site.baseurl }}/static/img/life360_logo.png" width="100"/></a></td>
<td align="center"><a href="http://hailocab.com"><img src="{{ site.baseurl }}/static/img/hailo_logo.png" width="62"/></a></td>
<td align="center"><a href="http://simplereach.com"><img src="{{ site.baseurl }}/static/img/simplereach_logo.png" width="136"/></a></td>
<td align="center"><a href="http://moz.com"><img src="{{ site.baseurl }}/static/img/moz_logo.png" width="108"/></a></td>
<td align="center"><a href="http://path.com"><img src="{{ site.baseurl }}/static/img/path_logo.png" width="84"/></a></td>
</tr><tr>
<td align="center"><a href="http://segment.io"><img src="{{ site.baseurl }}/static/img/segmentio_logo.png" width="70"/></a></td>
<td align="center"><a href="http://eventful.com"><img src="{{ site.baseurl }}/static/img/eventful_logo.png" width="95"/></a></td>
<td align="center"><a href="http://energyhub.com"><img src="{{ site.baseurl }}/static/img/energyhub_logo.png" width="99"/></a></td>
<td align="center"><a href="https://project-fifo.net"><img src="{{ site.baseurl }}/static/img/project_fifo.png" width="97"/></a></td>
<td align="center"><a href="http://trendrr.com"><img src="{{ site.baseurl }}/static/img/trendrr_logo.png" width="97"/></a></td>
<td align="center"><a href="http://reonomy.com"><img src="{{ site.baseurl }}/static/img/reonomy_logo.png" width="100"/></a></td>
</tr><tr>
<td align="center"><a href="http://dramafever.com"><img src="{{ site.baseurl }}/static/img/dramafever.png" width="120"/></a></td>
<td align="center"><a href="http://hw-ops.com"><img src="{{ site.baseurl }}/static/img/heavy_water.png" width="50"/></a></td>
<td align="center"><a href="http://lytics.io"><img src="{{ site.baseurl }}/static/img/lytics.png" width="100"/></a></td>
<td align="center"><a href="http://mediaforge.com"><img src="{{ site.baseurl }}/static/img/rakuten.png" width="100"/></a></td>
<td align="center"><a href="http://socialradar.com"><img src="{{ site.baseurl }}/static/img/socialradar_logo.png" width="100"/></a></td>
<td align="center"><a href="http://wistia.com"><img src="{{ site.baseurl }}/static/img/wistia_logo.png" width="140"/></a></td>
</tr><tr>
<td align="center"><a href="http://stripe.com"><img src="{{ site.baseurl }}/static/img/stripe_logo.png" width="140"/></a></td>
<td align="center"><a href="http://soundest.com"><img src="{{ site.baseurl }}/static/img/soundest_logo.png" width="140"/></a></td>
<td align="center"><a href="http://docker.com"><img src="{{ site.baseurl }}/static/img/docker_logo.png" width="145"/></a></td>
<td align="center"><a href="http://getweave.com"><img src="{{ site.baseurl }}/static/img/weave_logo.png" width="110"/></a></td>
<td align="center"><a href="http://shipwire.com"><img src="{{ site.baseurl }}/static/img/shipwire_logo.png" width="140"/></a></td>
<td align="center"><a href="http://digg.com"><img src="{{ site.baseurl }}/static/img/digg_logo.png" width="140"/></a></td>
</tr><tr>
<td align="center"><a href="http://scalabull.com"><img src="{{ site.baseurl }}/static/img/scalabull_logo.png" width="110"/></a></td>
<td align="center"><a href="http://augury.com"><img src="{{ site.baseurl }}/static/img/augury_logo.png" width="110"/></a></td>
<td align="center:"><a href="http://buzzfeed.com"><img src="{{ site.baseurl }}/static/img/buzzfeed_logo.png" width="97"/></a></td>
<td align="center"><a href="http://eztable.com"><img src="{{ site.baseurl }}/static/img/eztable_logo.png" width="105"/></a></td>
<td align="center"><a href="http://www.dotabuff.com"><img src="{{ site.baseurl }}/static/img/dotabuff_logo.png" width="105"/></a></td>
</tr></table></center>

### On The Twitter

<center>
<a class="twitter-timeline" width="520" height="600" data-dnt="true" data-chrome="noborders noheader" href="https://twitter.com/nsqio/favorites" data-widget-id="535600226652782593">Favorite Tweets by @nsqio</a>
<script>!function(d,s,id){var js,fjs=d.getElementsByTagName(s)[0],p=/^http:/.test(d.location)?'http':'https';if(!d.getElementById(id)){js=d.createElement(s);js.id=id;js.src=p+"://platform.twitter.com/widgets.js";fjs.parentNode.insertBefore(js,fjs);}}(document,"script","twitter-wjs");</script>
</center>

### Getting Help

* **Source**: [https://github.com/bitly/nsq][github_nsq]
* **Issues**: [https://github.com/bitly/nsq/issues][github_issues]
* **Mailing List**: [nsq-users@googlegroups.com][google_group]
* **IRC**: #nsq on freenode
* **Twitter**: [@nsqio][nsqio_twitter]

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
[nsqio_twitter]: https://twitter.com/nsqio
