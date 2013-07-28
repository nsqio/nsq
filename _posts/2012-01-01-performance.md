--- 
title: Performance
layout: post
category: overview
permalink: /overview/performance.html
---

DISCLAIMER: Please keep in mind that NSQ is designed to be used in a distributed fashion. Single
node performance is important, but not the end-all-be-all of what we're looking to achieve. Also,
benchmarks are stupid, but here's a few anyway to ignite the flame:

On a 2012 MacBook Air i7 2ghz (`GOMAXPROCS=1`, `go 1.1 beta2 4a712e80e9b1`, `NSQ v0.2.19-alpha`)
single publisher, single consumer:

{% highlight bash %}
$ ./nsqd --mem-queue-size=1000000

$ ./bench_writer
2013/04/09 23:25:54 duration: 2.46904784s - 77.250mb/s - 405014.429ops/s - 2.469us/op

$ ./bench_reader
2013/04/09 23:27:53 duration: 5.996050461s - 31.810mb/s - 166776.448ops/s - 5.996us/op
{% endhighlight %}
