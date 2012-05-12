### NSQ

An infrastructure component designed to support highly available, distributed, fault tolerant, "guaranteed" message
delivery.

#### Background

`simplequeue` was developed as a a dead-simple in-memory message queue. It spoke HTTP and had no knowledge (or care) for
the data you put in or took out. Life was good.

We used `simplequeue` as the foundation for a distributed message queue. In production, we silo'd a `simplequeue` right
where messages were produced (ie. frontends) and effectively reduced the potential for data loss in a system which did
not persist messages (by guaranteeing that the loss of any single `simplequeue` would not prevent the rest of the
message producers or workers, to function).

We added `pubsub`, an HTTP server to aggregate streams and provide a long-lived `/sub` endpoint. We leveraged `pubsub`
to transmit streams across data-centers in order to daisy chain a given feed to various downstream services. A nice
property of this setup is that producers are de-coupled from downstream consumers (a downstream consumer only needs to
know of the `pubsub` to receive data).

There are a few issues with this combination of tools...

One is simply the operational complexity of having to setup the data pipe to begin with. Often this involves services
setup as follows:

    `api` > `simplequeue` > `queuereader` > `pubsub` > `ps_to_http` > `simplequeue` > `queuereader`

Of particular note are the `pubsub` > `ps_to_http` links. We repeatedly encounter the issue of consuming a single stream
with the desire to avoid a SPOF. You have 2 options, none ideal. Often we just put the `ps_to_http` process on a single
box and pray. Alternatively we've chosen to consume the full stream multiple times but only process a % of the stream on
a given host (essentially sharding).  To make things even more complicated we need to repeat this chain for each stream of data we're interested in.

Messages traveling through the system have no guarantee that they will be delivered to a client and the responsibility of requeueing is placed on the client.  This churn of messages being passed back and forth increases the potential for errors resulting in message loss.

#### Why build on top of 0mq / why not AMQP?

Obviously there are "off-the-shelf" solutions which attempt to solve similar problems. On the more popular side you find
solutions that fall into the AMQP family (`rabbitmq`, `activemq`, etc.).

There are a few reasons why we felt an AMQP implementation was not the type of tool we wanted to fulfill our needs, the
most important being it's core design when it comes to [high availability](http://www.rabbitmq.com/ha.html).
Summarizing, AMQP follows the broker messaging pattern. All messages of a given type (eventually) reach a single
instance (broker), resulting in a SPOF for all messages of that type. There are ways to mitigate this risk, akin to the
types of solutions that one would employ when using MySQL (slaves, etc.), but they seem brittle and undesirable (as well
as fundamentally different from the spirit of `simplequeue`).

`0mq` on the other hand is designed as a messaging *library*. The topology and component implementation is entirely
up to you, it simply provides the primitives to be able to abstract away lower level details of *how* you're
transporting data. `0mq` is your protocol, allowing us to skip the step of designing our own and instead leverage
it's robust client library support, which includes all of the major programming languages used at bitly.

#### Enter NSQ

`NSQ` is designed to address the fragile nature of the combination of components listed above as well as provide
high-availability as a byproduct of a messaging pattern that includes no SPOF. It also addresses the need for stronger
guarantees around the delivery of a message.

A single `NSQ` process handles multiple "topics" (by convention, this would previously have been referred to as a
"stream"). Second, a topic can have multiple "channels". In practice, a channel maps to a downstream service. Each
channel receives all the messages from a topic. The channels buffer data independently of each other, preventing a slow
consumer from causing a backlog for other channels. A channel can have multiple clients, a message (assuming successful
delivery) will only be delivered to one of the connected clients, at random.

For example, the "decodes" topic could have a channel for "clickatron", "spam", and "fishnet", etc. The benefit should
be easy to see, there are no additional services needed to be setup for new queues or to daisy chain a new downstream
service.

`NSQ` is fully distributed with no single broker or point of failure. `NSQ` clients (queuereaders) are connected via
`0mq` sockets to **all** `NSQ` instances providing the specified topic. There are no middle-men, no brokers, and no
SPOF. The *topology* solves the problems described above:

    NSQ     NSQ    NSQ
      \     /\     /
       \   /  \   /
        \ /    \ /
         X      X
        / \    / \
       /   \  /   \
    ...  consumers  ...

You don't have to deal with figuring out how to robustly distribute an aggregated feed, instead you consume directly
from *all* producers. It also doesn't *technically* matter which client connects to which `NSQ`, as long as there are
enough clients connected to all producers to satisfy the volume of messages, you're guaranteed that all will be
delivered to the system downstream.

It's also worth pointing out the bandwidth efficiency when you have >1 consumers of a topic. You're no longer daisy
chaining multiple copies of the stream over the network, it's all happening right at the source. Moving away from HTTP
also significantly reduces the per-message overhead.

#### Message Delivery Guarantees

`NSQ` guarantees that a message will be delivered **at least once**.  Duplicate messages are possible and downstream systems should be designed to perform idempotent operations.

It accomplishes this by performing a handshake with the client, as follows:

  1. client GET message
  2. `NSQ` sends message and stores in temporary internal location
  3. client ACK receipt of message
     * if client does not ACK `NSQ` will requeue the message
  4. client replies SUCCESS or FAIL
     * if client does not reply `NSQ` will requeue the message
  5. `NSQ` requeues on FAIL and purges on SUCCESS

#### Lookup Service

`NSQ` includes a helper application, `nsq_lookup`, which provides a directory service where queuereaders can lookup the
addresses of `NSQ` instances that contain the topics they are interested in subscribing to. This decouples the consumers
from the producers (they both individually only need to have intimate knowledge of `nsq_lookup`, never each other).

At a lower level each `NSQ` has a long-lived connection to `nsq_lookup` over which it periodically pushes it's state.
This data is used to inform which addresses `nsq_lookup` will give to queuereaders. The heuristic could be based on
depth, number of connected queuereaders or naive strategies like round-robin, etc. The goal is to ensure that all
producers are being read from.  On the client side an HTTP interface is exposed for queuereaders to poll.

High availability of `nsq_lookup` is achieved by running multiple instances. They don't communicate directly to each
other and don't require strong data consistency between themselves. The data is considered *eventually* consistent, the
queuereaders randomly choose a `nsq_lookup` to poll. Stale (or otherwise inaccessible) nodes don't grind the system to a
halt.
