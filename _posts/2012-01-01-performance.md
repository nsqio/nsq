---
title: Performance
layout: post
category: overview
permalink: /overview/performance.html
---

### Distributed Performance

The main repo contains a script (`bench/bench.py`) that automates a distributed benchmark on EC2.
It bootstraps `N` nodes, some running `nsqd` and some running load-generating utilities (`PUB` and
`SUB`), and then parses their output to provide an aggregate total.

#### Setup

The following runs reflect the default parameters of 6 `c3.2xlarge`, the cheapest instance type
that supports 1gbit links. 3 nodes run an `nsqd` instance and the rest run instances of
`bench_reader` (`SUB`) and `bench_writer` (`PUB`), to generate load depending on the benchmark mode.

    $ ./bench/bench.py --access-key=... --secret-key=... --ssh-key-name=...
    [I 140917 10:58:10 bench:102] launching 6 instances
    [I 140917 10:58:12 bench:111] waiting for instances to launch...
    ...
    [I 140917 10:58:37 bench:130] (1) bootstrapping ec2-54-160-145-64.compute-1.amazonaws.com (i-0a018ce1)
    [I 140917 10:59:37 bench:130] (2) bootstrapping ec2-54-90-195-149.compute-1.amazonaws.com (i-0f018ce4)
    [I 140917 11:00:00 bench:130] (3) bootstrapping ec2-23-22-236-55.compute-1.amazonaws.com (i-0e018ce5)
    [I 140917 11:00:41 bench:130] (4) bootstrapping ec2-23-23-40-113.compute-1.amazonaws.com (i-0d018ce6)
    [I 140917 11:01:10 bench:130] (5) bootstrapping ec2-54-226-180-44.compute-1.amazonaws.com (i-0c018ce7)
    [I 140917 11:01:43 bench:130] (6) bootstrapping ec2-54-90-83-223.compute-1.amazonaws.com (i-10018cfb)

#### Producer Throughput

This benchmark measures *just* producer throughput, with no additional load.  The message
size is 100 bytes and messages are distributed over 3 topics.

    $ ./bench/bench.py --access-key=... --secret-key=... --ssh-key-name=... --mode=pub --msg-size=100 run
    [I 140917 12:39:37 bench:140] launching nsqd on 3 host(s)
    [I 140917 12:39:41 bench:163] launching 9 producer(s) on 3 host(s)
    ...
    [I 140917 12:40:20 bench:248] [bench_writer] 10.002s - 197.463mb/s - 2070549.631ops/s - 4.830us/op

An ingress of **`~2.07mm`** msgs/sec, consuming an aggregate **`197mb/s`** of bandwidth.

#### Producer and Consumer Throughput

This benchmark more accurately reflects real-world conditions by servicing both producers *and*
consumers. Again, the message size is 100 bytes and messages are distributed over 3 topics, each
with a *single* channel (24 clients per channel).

    $ ./bench/bench.py --access-key=... --secret-key=... --ssh-key-name=... --msg-size=100 run
    [I 140917 12:41:11 bench:140] launching nsqd on 3 host(s)
    [I 140917 12:41:15 bench:163] launching 9 producer(s) on 3 host(s)
    [I 140917 12:41:22 bench:186] launching 9 consumer(s) on 3 host(s)
    ...
    [I 140917 12:41:55 bench:248] [bench_reader] 10.252s - 76.946mb/s - 806838.610ops/s - 12.706us/op
    [I 140917 12:41:55 bench:248] [bench_writer] 10.030s - 80.315mb/s - 842149.615ops/s - 11.910us/op

At an ingress of **`~842k`** and egress of **`~806k`** msgs/s, consuming an aggregate **`156mb/s`**
of bandwidth, we're now maxing out the CPU capacity on the `nsqd` nodes. By introducing consumers,
`nsqd` needs to maintain per-channel in-flight accounting so the load is naturally higher.

The consumer numbers are slightly lower than producers because consumers send twice the number of
commands as producers (a `FIN` command must be sent for each message), impacting throughput.

Adding another 2 nodes (one `nsqd` and one load-generating) attains over **`1mm`** msgs/s:

    $ ./bench/bench.py --access-key=... --secret-key=... --ssh-key-name=... --msg-size=100 run
    [I 140917 13:38:28 bench:140] launching nsqd on 4 host(s)
    [I 140917 13:38:32 bench:163] launching 16 producer(s) on 4 host(s)
    [I 140917 13:38:43 bench:186] launching 16 consumer(s) on 4 host(s)
    ...
    [I 140917 13:39:12 bench:248] [bench_reader] 10.561s - 100.956mb/s - 1058624.012ops/s - 9.976us/op
    [I 140917 13:39:12 bench:248] [bench_writer] 10.023s - 105.898mb/s - 1110408.953ops/s - 9.026us/op

### Single Node Performance

DISCLAIMER: Please keep in mind that **NSQ** is designed to be used in a distributed fashion. Single
node performance is important, but not the end-all-be-all of what we're looking to achieve. Also,
benchmarks are stupid, but here's a few anyway to ignite the flame:

 * 2012 MacBook Air i7 2ghz
 * go1.2
 * NSQ v0.2.24
 * 200 byte messages

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
