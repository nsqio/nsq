# Redesigned NSQ Overview

## Original NSQ

NSQ is a realtime distributed MQ platform written in Golang. NSQ is composed of 3 parts: nsqd as messages storage service, nsqlookupd as topology collect and query service, nsqadmin as a web UI to introspect the cluster in realtime.

In nsqd, data flow is modeled as a tree of streams and consumers. A topic is a distinct stream of data. A channel is a logical grouping of consumers subscribed to a given topic. In original nsqd all messages is in Golang channel except overflow, every consume channel receives a copy of all the messages for the topic.
![arch](https://f.cloud.github.com/assets/187441/1700696/f1434dc8-6029-11e3-8a66-18ca4ea10aca.gif "arch")

The original NSQ support distributed topologies with no SPOF and can scale horizontally, and has a simple TCP protocol.

## Why we need redesign it

Due to the simpleness and the features of NSQ, we used it as our main MQ system for a while until we found several issues we could not solve without redesigning it.

### Deployment

The original NSQ promotes a consumer-side discovery model that alleviates the upfront configuration burden of having to tell consumers where to find the topic(s) they need.

However, it does not provide any means to solve the problem of where a service should publish to. By co-locating nsqd, we can  only solve the problem partially. When the services become more and more, it is not possible to deploy a co-locating nsqd for every service. So we need a coordinator to balance all the topics while creating them and tell all the services which nsqd they should publish to.

### Lost on exception

The original NSQ store the messages using the Golang channel which is in memory and only persist messages while overflow. This is good in some situations but very bad if we get exceptions that cause the process crashed or even worse we encounter unexpected machine shutdown.

### Trace the message

Since in original NSQ the messages is in Golang channel in normal, and will be cleaned after consumed, we can not find out whether the message has ever reached in the NSQ. Also we can not retain some messages for a while in case we need it later after consumed.

### Consume history messages

There are some cases we need re-process some history messages so we can fix errors after update the message handler. In original NSQ it is not possible to do this since it just handle messages in memory and do not persist all the messages to disk.

### Receive in Order

In some situations, we need make sure all the messages are transferred in order from one system to another to make two system eventually consistence even there are some exceptions (network or crash). To achieve this, persistence of the messages is a prerequisite, and beyond that we need make sure both producers and consumers are under our control.

### Dynamic Debug Log

This is a very useful feature to inspect the online problems without affecting the system. In fact, in the original NSQ the logs are too less to debug.

## The redesign road

Because of the above reasons, we decided to redesign the NSQ. What is important is we have to try our best to make it compatible with original NSQ. Only in this way we can migrate to the new system more smoothly. A notable modification is the internal ID of the message. In original NSQ it is 16-byte hex string, we changed it to 16-byte binary string. We changed this because we noticed that client SDK just read/use the id as 16-byte data and it is not a problem while migrating.

Another change we need to make first is to persist all the messages to the disk. Most of the missing features due to the golang channel as the memory storage for messages. This change is not easy since it will impact the performance, and we did many effort to make sure the least impaction on performance. It turns out that we got a good result since we can achieve a better performance than the original NSQ after we redesigned. We achieve this by optimizing many performance bottlenecks in both the original NSQ and the redesigned one.

Since all the messages go to disk files, we can implement the trace and consume history messages much easier. We use only a cursor to stand for the channel and in this way we also avoid the copy overhead from topic to all channels.

To implement the replication and HA, we introduced the cluster coordinator on both nsqd and nsqlookupd side and most of the cluster related codes are outside of the topic so we can easily merge our work into the mainline in future.

We did so many changes to the original NSQ, so we need make sure it will work. Thanks to the unit tests in the NSQ, we can reuse most of them and we add more tests to make sure everything is ok. For distributed system, we introduced Jepsen test to make sure no any unexpected lost while in some bad  network environment.

## Put them all together

Put all the work together we got the architecture overview below:
![New Arch](https://raw.githubusercontent.com/absolute8511/nsq/master/doc/NSQ%20redesigned%20arch.png "New arch")
The technical details about all these new features will be disclosed in future articles and the source code is already open sourced at Github.

[Redesigned NSQ Server](https://github.com/absolute8511/nsq)

[Golang SDK](https://github.com/absolute8511/go-nsq)

[Java SDK](https://github.com/youzan/nsqJavaSDK)

[PHP SDK](https://github.com/youzan/php-nsq-client)

[Spark Connector](https://github.com/youzan/spark-nsq-consumer)

[Flume Connector](https://github.com/DoraALin/flume-nsq-sink)

