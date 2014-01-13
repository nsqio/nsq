--- 
title: Performance
layout: post
category: overview
permalink: /overview/performance.html
---

DISCLAIMER: Please keep in mind that **NSQ** is designed to be used in a distributed fashion. Single
node performance is important, but not the end-all-be-all of what we're looking to achieve. Also,
benchmarks are stupid, but here's a few anyway to ignite the flame:

On a 2012 MacBook Air i7 2ghz:

    GOMAXPROCS=1
    go 1.2
    NSQ v0.2.24

Single publisher, single consumer:

{% highlight bash %}
$ ./bench.sh 
results...
2014/01/12 22:09:08 duration: 2.311925588s - 82.500mb/s - 432539.873ops/s - 2.312us/op
2014/01/12 22:09:19 duration: 6.009749983s - 31.738mb/s - 166396.273ops/s - 6.010us/op
{% endhighlight %}
