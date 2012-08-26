# Not Simple Queue

An infrastructure component designed to support highly available, distributed, fault tolerant,
loosely guaranteed message processing.

## Background

[simplequeue][1] was developed, you guessed it, as a *simple* in-memory message queue with an HTTP
interface. It is agnostic to the type and format of the data you put in and take out.

We use `simplequeue` as the foundation for a distributed message queue by siloing an instance on
each host that produces messages. This effectively reduces the potential for data loss in a system
which otherwise does not persist messages (by guaranteeing that the loss of any single host would
not prevent the rest of the message producers or consumers from functioning).

We also use [pubsub][1], an HTTP server to aggregate streams and provide an endpoint for N number of
clients to subscribe. We use it to transmit streams across hosts (or datacenters) and be queued
again for writing to various downstream services.

We use this foundation to process 100s of millions of messages a day. It is the core upon which
*everything* is built.

This setup has several nice properties:

 * producers are de-coupled from downstream consumers
 * no producer-side single point of failures
 * easy to interact with (all HTTP)

But, it also has its issues...

One is simply the operational overhead/complexity of having to setup and configure the various tools
in the chain. This often looks like:

    api > simplequeue > queuereader > pubsub > ps_to_http > simplequeue > queuereader

Of particular note are the `pubsub > ps_to_http` links. Given this setup, consuming a stream in a
way that avoids SPOFs is a challenge. There are two options, neither of which is ideal:

 1. just put the `ps_to_http` process on a single box and pray
 2. shard by *consuming* the full stream but *processing* only a percentage of it on each host 
    (though this does not resolve the issue of seamless failover)

To make things even more complicated, we need to repeat this for *each* stream of data we're
interested in.

Also, messages traveling through the system have no delivery guarantee and the responsibility of
requeueing is placed on the client (for instance, if processing fails). This churn increases the
potential for situations that result in message loss.

## Enter NSQ

`NSQ` is designed to:

 1. greatly simplify configuration complexity
 2. provide a straightforward upgrade path
 3. provide easy topology solutions that enable high-availability and eliminate SPOFs
 4. address the need for stronger message delivery guarantees
 5. bound the memory footprint of a single process (by persisting some messages to disk)
 6. improve efficiency
 
### Simplifying Configuration Complexity

A single `nsqd` instance is designed to handle multiple streams of data at once. Streams are called
"topics" and a topic has 1 or more "channels". Each channel receives a *copy* of all the
messages for a topic. In practice, a channel maps to a downstream service consuming a topic.

Topics and channels all buffer data independently of each other, preventing a slow consumer from
causing a backlog for other channels (the same applies at the topic level).

A channel can, and generally does, have multiple clients connected. Assuming all connected clients
are in a state where they are ready to receive messages, a message will be delivered to a random
client.

For example:
    
    "clicks" (topic)
       |                             |- client 
       |>---- "metrics" (channel) --<|- ...
       |                             |- client
       |
       |                                   |- client
       |>---- "spam_analysis" (channel) --<|- ...
       |                                   |- client
       |
       |                             |- client
       |>---- "archive" (channel) --<|- ...
                                     |- client

Configuration is greatly simplified because there is no additional setup required to introduce a new
distinct consumer for a given topic nor is there any need to setup new services to introduce a new
topic.

`NSQ` also includes a helper application, `nsqlookupd`, which provides a directory service where
consumers can lookup the addresses of `nsqd` instances that provide the topics they are interested
in subscribing to. In terms of configuration, this decouples the consumers from the producers (they
both individually only need to know where to contact common instances of `nsqlookupd`, never each
other) reducing complexity and maintenance.

At a lower level each `nsqd` has a long-lived TCP connection to `nsqlookupd` over which it
periodically pushes its state. This data is used to inform which `nsqd` addresses `nsqlookupd` will
give to consumers. For consumers, a HTTP `/lookup` endpoint is exposed for polling.

NOTE: in future versions, the heuristic `nsqlookupd` uses to return addresses could be based on
depth, number of connected clients, or other "intelligent" strategies. The current implementation is
simply *all*. Ultimately, the goal is to ensure that all producers are being read from such that
depth stays near zero.

### Straightforward Upgrade Path

This was one of our **highest** priorities. Our production systems handle a large volume of traffic,
all built upon our existing messaging tools, so we needed a way to slowly and methodically upgrade
specific parts of our infrastructure with little to no impact.

First, on the message *producer* side we built `nsqd` to match `simplequeue`, ie. a HTTP `/put`
endpoint to POST binary data (with the one caveat that the endpoint takes an additional query
parameter specifying the "topic"). Services that wanted to start writing to `nsqd` now just had to
point to `nsqd`.

Second, we built libraries in both Python and Go that matched the functionality and idioms we had
been accustomed to in our existing libraries. This eased the transition on the message *consumer*
side by limiting the code changes to bootstrapping.  All business logic remained the same.

Finally, we built utilities to glue old and new components together. These are all available in the
`examples` directory in the repository:

 * `nsq_pubsub` - expose a `pubsub` like HTTP interface to topics in an `NSQ` cluster
 * `nsq_to_file` - durably write all messages for a given topic to a file
 * `nsq_to_http` - perform HTTP requests for all messages in a topic to (multiple) endpoints

### Eliminating SPOFs

`NSQ` is designed to be used in a distributed fashion. `nsqd` clients are connected (over TCP) to
**all** instances providing the specified topic. There are no middle-men, no brokers, and no SPOFs:

    NSQ     NSQ    NSQ
     |\     /\     /|
     | \   /  \   / |
     |  \ /    \ /  |
     |   X      X   |
     |  / \    / \  |
     | /   \  /   \ |
     C      C       C     (consumers)

This topology eliminates the need to chain single, aggregated, feeds. Instead you consume directly
from **all** producers. *Technically*, it doesn't matter which client connects to which `NSQ`, as
long as there are enough clients connected to all producers to satisfy the volume of messages,
you're guaranteed that all will be eventually processed.

For `nsqlookupd`, high availability is achieved by running multiple instances. They don't
communicate directly to each other and data is considered to be eventually consistent. Consumers
poll *all* of the `nsqlookupd` instances they are configured with resulting in a union of all the
responses. Stale, inaccessible, or otherwise faulty nodes don't grind the system to a halt.

### Message Delivery Guarantees

`NSQ` guarantees that a message will be delivered **at least once**. Duplicate messages are a
possibility and downstream systems should be designed to perform idempotent operations.

This is accomplished by performing a handshake with the client, as follows (assume the client has
successfully connected and subscribed to a topic):

  1. client sends RDY command indicating the # of messages they are willing to accept
  2. `NSQ` sends message and stores in temporary internal location (decrementing RDY count for that 
     client)
  3. client replies FIN or REQ indicating success or failure (requeue) respectively (if client does
     not reply`NSQ` will automatically timeout after a configurable amount of time and automatically 
     REQ the message)

This ensures that the only edge case that would result in message loss is an unclean shutdown of a
`nsqd` process (and only for the messages that were in memory).

### Bounded Memory Footprint

`nsqd` provides a configuration option (`--mem-queue-size`) that will determine the number of
messages that are kept in memory for a given queue (for both topics *and* channels). If the depth of
a queue exceeds this threshold messages will be written to disk. This bounds the footprint of a
given `nsqd` process to:

    mem-queue-size * #_of_channels_and_topics

Also, an astute observer might have identified that this is a convenient way to gain an even higher
guarantee of delivery by setting this value to something low (like 1 or even 0). The disk-backed
queue is designed to survive unclean restarts (although messages might be delivered twice).

Also, related to message delivery guarantees, *clean* shutdowns (by sending a `nsqd` process the
TERM signal) safely persist messages in memory, in-flight, deferred, and in various internal
buffers.

### Efficiency

`NSQ` was designed to communicate over a `memcached` like command protocol with simple size-prefixed
responses. Compared to the previous toolchain using HTTP, this is significantly more efficient
per-message.

Also, by eliminating the daisy chained stream copies, configuration, setup, and development time is
greatly reduced (especially in cases where there are >1 consumers of a topic). All the duplication
happens at the source.

Finally, because `simplequeue` has no high-level functionality built in, the client is responsible
for maintaining message state (it does so by embedding this information and sending the message back
and forth for retries). In `NSQ` all of this information is kept in the core.

## EOL

We've been using `NSQ` in production for several months. It's already made an improvement in terms
of robustness, development time, and simplicity to systems that have moved over to it.

[1]: https://github.com/bitly/simplehttp
