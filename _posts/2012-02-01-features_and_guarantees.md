---
title: Features & Guarantees
layout: post
category: overview
permalink: /overview/features_and_guarantees.html
---

**NSQ** is a realtime distributed messaging platform.

### Features

 * support distributed topologies with no SPOF
 * horizontally scalable (no brokers, seamlessly add more nodes to the cluster)
 * low-latency push based message delivery ([performance][performance])
 * combination load-balanced *and* multicast style message routing
 * excel at both streaming (high-throughput) and job oriented (low-throughput) workloads
 * primarily in-memory (beyond a high-water mark messages are transparently kept on disk)
 * runtime discovery service for consumers to find producers ([nsqlookupd][nsqlookupd])
 * transport layer security (TLS)
 * data format agnostic
 * few dependencies (easy to deploy) and a sane, bounded, default configuration
 * simple TCP protocol supporting client libraries in any language
 * HTTP interface for stats, admin actions, and producers (*no client library needed to publish*)
 * integrates with [statsd][statsd] for realtime instrumentation
 * robust cluster administration interface ([nsqadmin][nsqadmin])

### Guarantees

As with any distributed system, achieving your goal is a matter of making intelligent tradeoffs.
By being transparent about the reality of these tradeoffs we hope to set expectations about how
**NSQ** will behave when deployed in production.

#### messages are *not* durable (by default)

Although the system supports a "release valve" (`--mem-queue-size`) after which messages will
be transparently kept on disk, it is primarily an *in-memory* messaging platform.

`--mem-queue-size` can be set to 0 to to ensure that all incoming messages are persisted to disk.
In this case, if a node failed, you are susceptible to a reduced failure surface (i.e. did the
OS or underlying IO subsystem fail).

There is no built in replication.  However, there are a variety of ways this tradeoff is managed
such as deployment topology and techniques which actively slave and persist topics to disk in a
fault-tolerant fashion.

#### messages are delivered *at least once*

Closely related to above, this assumes that the given `nsqd` node does not fail.

This means, for a variety of reasons, messages can be delivered *multiple* times (client
timeouts, disconnections, requeues, etc.).  It is the client's responsibility to perform
idempotent operations or de-dupe.

#### messages received are *un-ordered*

You **cannot** rely on the order of messages being delivered to consumers.

Similar to message delivery semantics, this is the result of requeues, the combination of
in-memory and on disk storage, and the fact that each `nsqd` node shares nothing.

It is relatively straightforward to achieve *loose ordering* (i.e. for a given consumer its
messages are ordered but not across the cluster as a whole) by introducing a window of latency in
your consumer to accept messages and order them before processing (although, in order to preserve
this invariant one must drop messages falling *outside* that window).

#### consumers *eventually* find all topic producers

The discovery service ([nsqlookupd][nsqlookupd]) is designed to be *eventually consistent*.
`nsqlookupd` nodes do not coordinate to maintain state or answer queries.

Network partitions do not affect *availability* in the sense that both sides of the partition can
still answer queries.  Deployment topology has the most significant effect of mitigating these
types of issues.

[performance]: {{ site.baseurl }}/overview/performance.html
[nsqlookupd]: https://github.com/bitly/nsq/tree/master/nsqlookupd/README.md
[nsqadmin]: https://github.com/bitly/nsq/tree/master/nsqadmin/README.md
[statsd]: https://github.com/etsy/statsd/
[graphite]: http://graphite.wikidot.com/
