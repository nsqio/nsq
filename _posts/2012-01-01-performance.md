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
    go 1.1.1
    NSQ v0.2.22-alpha

Single publisher, single consumer:

{% highlight bash %}
$ ./nsqd --mem-queue-size=1000000

$ ./bench_writer
2013/08/03 10:05:13 duration: 2.533087137s - 75.297mb/s - 394775.207ops/s - 2.533us/op

$ ./bench_reader
2013/08/03 10:06:42 duration: 6.208810367s - 30.720mb/s - 161061.450ops/s - 6.209us/op
{% endhighlight %}
