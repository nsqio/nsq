--- 
title: Performance
layout: post
category: overview
permalink: /overview/performance.html
---

DISCLAIMER: Please keep in mind that **NSQ** is designed to be used in a distributed fashion. Single
node performance is important, but not the end-all-be-all of what we're looking to achieve. Also,
benchmarks are stupid, but here's a few anyway to ignite the flame:

On a 2012 MacBook Air i7 2ghz, go1.2, NSQ v0.2.24...

#### GOMAXPROCS=1 (single publisher, single consumer)

{% highlight bash %}
$ ./bench.sh 
results...
PUB: 2014/01/12 22:09:08 duration: 2.311925588s - 82.500mb/s - 432539.873ops/s - 2.312us/op
SUB: 2014/01/12 22:09:19 duration: 6.009749983s - 31.738mb/s - 166396.273ops/s - 6.010us/op
{% endhighlight %}

#### GOMAXPROCS=4 (4 publishers, 4 consumers)

{% highlight bash %}
$ ./bench.sh 
results...
PUB: 2014/01/13 16:58:05 duration: 1.411492441s - 135.130mb/s - 708469.965ops/s - 1.411us/op
SUB: 2014/01/13 16:58:16 duration: 5.251380583s - 36.321mb/s - 190426.114ops/s - 5.251us/op
{% endhighlight %}
