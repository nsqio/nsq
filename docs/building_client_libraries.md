## Building NSQ Client Libraries

NSQ's design pushes a lot of responsibility onto client libraries in order to maintain overall
cluster robustness and performance.

This guide attempts to outline the various responsibilities well-behaved client libraries need to
fulfill. Because publishing to `nsqd` is trivial (just an HTTP POST to the `/put` endpoint), this
document focuses on consumers.

By setting these expectations we hope to provide a foundation for achieving consistency across
languages for NSQ users.

### Overview

 1. [Configuration](#configuration)
 2. [Discovery](#discovery) (optional)
 3. [Connection Handling](#connection_handling)
 4. [Feature Negotiation](#feature_negotiation)
 5. [Data Flow / Heartbeats](#data_flow)
 6. [Message Handling](#message_handling)
 7. [RDY State](#rdy_state)
 8. [Backoff](#backoff)

## <a name="configuration"></a>Configuration

At a high level, our philosophy with respect to configuration is to design the system to have the
flexibility to support different workloads, use sane defaults that run well "out of the box", and
minimize the number of dials.

A client subscribes to a `topic` on a `channel` over a TCP connection to `nsqd` instance(s). You can
only subscribe to one topic per connection so multiple topic consumption needs to be structured
accordingly.

Using `nsqlookupd` for discovery is optional so client libraries should support a configuration
where a client connects *directly* to one or more `nsqd` instances or where it is configured to poll
one or more `nsqlookupd` instances. When a client is configured to poll `nsqlookupd` the polling
interval should be configurable. Additionally, because typical deployments of NSQ are in distributed
environments with many producers and consumers, the client library should automatically add jitter
based on a random % of the configured value. This will help avoid a thundering herd of connections.
For more detail see [Discovery](#discovery).

An important performance knob for clients is the number of messages it can receive before `nsqd`
expects a response. This pipelining facilitates buffered, batched, and asynchronous message
handling. By convention this value is called `max_in_flight` and it effects how `RDY` state is
managed. For more detail see [RDY State](#rdy_state).

Being a system that is designed to gracefully handle failure, client libraries are expected to 
implement retry handling for failed messages and provide options for bounding that behavior in terms
of number of attempts per message.  For more detail see [Message Handling](#message_handling).

Relatedly, when message processing fails, the client library is expected to automatically handle
re-queueing the message. NSQ supports sending a delay along with the `REQ` command. Client libraries
are expected to provide options for what this delay should be set to initially (for the first
failure) and how it should change for subsequent failures. For more detail see [Backoff](#backoff).

Most importantly, the client library should support some method of configuring callback handlers for
message processing. The signature of these callbacks should be simple, typically accepting a single
parameter (an instance of a "message object").

## <a name="discovery"></a>Discovery

An important component of NSQ is `nsqlookupd`, which provides a discovery service for consumers to
locate producers of a given topic at runtime.

Although optional, using `nsqlookupd` greatly reduces the amount of configuration required to
maintain and scale a large distributed NSQ cluster.

When a client uses `nsqlookupd` for discovery, the client library should manage the process of
polling all `nsqlookupd` instances for an up-to-date set of `nsqd` producers and should manage the
connections to those producers.

Querying an `nsqlookupd` instance is straightforward. Perform an HTTP request to the lookup endpoint
with a query parameter of the topic the client is attempting to discover (i.e.
`/lookup?topic=clicks`). The response format is JSON:

```json
{
    "status_code": 200,
    "status_txt": "OK",
    "data": {
        "channels": ["archive", "science", "metrics"],
        "producers": [
            {
                "broadcast_address": "clicksapi01.routable.domain.net", 
                "hostname": "clicksapi01.domain.net",
                "tcp_port": 4150,
                "http_port": 4151,
                "version": "0.2.18"
            },
            {
                "broadcast_address": "clicksapi02.routable.domain.net", 
                "hostname": "clicksapi02.domain.net",
                "tcp_port": 4150,
                "http_port": 4151,
                "version": "0.2.18"
            }
        ]
    }
}
```

The `broadcast_address` and `tcp_port` should be used to connect to an `nsqd` producer. Because, by
design, `nsqlookupd` instances don't coordinate their lists of producers, the client library should
union the lists it received from all `nsqlookupd` queries to build the final list of `nsqd`
producers to connect to. The `broadcast_address:tcp_port` combination should be used as the unique
key for this union.

A periodic timer should be used to repeatedly poll the configured `nsqlookupd` so that clients will
automatically discover new producers. The client library should automatically initiate connections
to all newly found `nsqd` producers.

When client library execution begins it should bootstrap this polling process by kicking off an
initial set of requests to the configured `nsqlookupd` instances.

## <a name="connection_handling"></a>Connection Handling

Once a client has an `nsqd` producer to connect to (via discovery or manual configuration), it
should open a TCP connection to `broadcast_address:port`. A separate TCP connection should be made
to each `nsqd` for each topic the client wants to consume.

When connecting to an `nsqd` instance, the client library should send the following data, in order:

 1. the magic identifier
 2. an `IDENTIFY` command (and payload) and read/verify response
 3. a `SUB` command (specifying desired topic) and read/verify response
 4. an initial `RDY` count of 1 (see [RDY State](#rdy_state)).
 
(low-level details on the protocol are available in the [spec](protocol.md))

### Reconnection

Client libraries should automatically handle reconnection as follows:

 * If the client is configured with a specific list of `nsqd` instances, reconnection should be
   handled by delaying the retry attempt in an exponential backoff manner (i.e. try to reconnect in
   8s, 16s, 32s, etc., up to a max).

 * If the client is configured to discover instances via `nsqlookupd`, reconnection should be
   handled automatically based on the polling interval (i.e. if a client disconnects from an `nsqd`,
   the client library should *only* attempt to reconnect if that instance is discovered by a 
   subsequent `nsqlookupd` polling round). This ensures that clients can learn about producers that 
   are introduced to the topology *and* ones that are removed (or failed).

## <a name="feature_negotiation"></a>Feature Negotiation

The `IDENTIFY` command can be used to set `nsqd` side metadata, modify client settings,
and negotiate features.  It satisfies two needs:

 1. In certain cases a client would like to modify how `nsqd` interacts with it (currently this 
    is limited to modifying a client's heartbeat interval but you can imagine this could evolve to 
    include enabling compression, TLS, output buffering, etc.)
 2. `nsqd` responds to the `IDENTIFY` command with a JSON payload that includes important server 
    side configuration values that the client should respect while interacting with the instance.

After connecting, based on the user's configuration, a client library should send an `IDENTIFY`
command (the body of an `IDENTIFY` command is a JSON payload), e.g:

```json
{
    "short_id": "app01",
    "long_id": "app01.bitly.net",
    "heartbeat_interval": 30000,
    "feature_negotiation": true
}
```

The `feature_negotiation` field indicates that the client can *accept* a JSON payload in return. The
`short_id` and `long_id` are arbitrary text fields that are used by `nsqd` (and `nsqadmin`) to
identify clients. `heartbeat_interval` configures the interval between heartbeats on a per-client
basis.

The `nsqd` will respond `OK` if it does not support feature negotiation (introduced in `nsqd`
v0.2.20+), otherwise:

```json
{
    "max_rdy_count": 2500,
    "version": "0.2.20-alpha"
}
```

More detail on the use of the `max_rdy_count` field is in the [RDY State](#rdy_state) section.

## <a name="data_flow"></a>Data Flow and Heartbeats

Once a client is in a subscribed state, data flow in the NSQ protocol is asynchronous. For
consumers, this means that in order to build truly robust and performant client libraries they
should be structured using asynchronous network IO loops and/or "threads" (the scare quotes are used
to represent both OS-level threads and userland threads, like coroutines).

Additionally clients are expected to respond to periodic heartbeats from the `nsqd` instances
they're connected to. By default this happens at 30 second intervals. The client can respond with
*any* command but, by convention, it's easiest to simply respond with a `NOP` whenever a heartbeat
is received. See the [protocol spec](protocol.md) for specifics on how to identify heartbeats.

A "thread" should be dedicated to reading data off the TCP socket, unpacking the data from the
frame, and performing the multiplexing logic to route the data as appropriate. This is also
conveniently the best spot to handle heartbeats.  At the lowest level, reading the protocol 
involves the following sequential steps:

 1. read 4 byte big endian uint32 size
 2. read size bytes data
 3. unpack data
 4. ...
 5. profit
 6. goto 1

### A Brief Interlude on Errors

Due to their asynchronous nature, it would take a bit of *extra* state tracking in order to
correlate protocol errors with the commands that generated them. Instead, we took the "fail fast"
approach so the overwhelming majority of protocol-level error handling is fatal. This means that if
the client sends an invalid command (or gets itself into an invalid state) the `nsqd` instance it's
connected to will protect itself (and the system) by forcibly closing the connection (and, if
possible, sending an error to the client). This, coupled with the connection handling mentioned
above, makes for a more robust and stable system.

The only errors that are not fatal are:

 * `E_FIN_FAILED` - The client tried to send a `FIN` command for an invalid message ID.
 * `E_REQ_FAILED` - The client tried to send a `REQ` command for an invalid message ID.
 * `E_TOUCH_FAILED` - The client tried to send a `TOUCH` command for an invalid message ID.

Because these errors are most often timing issues, they are not considered fatal. These situations
typically occur when a message times out on the `nsqd` side and is re-queued and delivered to
another client. The original recipient is no longer allowed to respond on behalf of that message.

## <a name="message_handling"></a>Message Handling

When the IO loop unpacks a data frame containing a message, it should route that message to the
configured handler for processing.

The `nsqd` producer expects to receive a reply within its configured message timeout (default: 60
seconds). There are a few possible scenarios:

 1. The handler indicates that the message was processed successfully.
 2. The handler indicates that the message processing was unsuccessful.
 3. The handler decides that it needs more time to process the message.
 4. The in-flight timeout expires and `nsqd` automatically re-queues the message.

In the first 3 cases, the client library should send the appropriate command on the client's behalf
(`FIN`, `REQ`, and `TOUCH` respectively).

The `FIN` command is the simplest of the bunch. It tells `nsqd` that it can safely discard the
message. `FIN` can also be used to discard a message that you do not want to process or retry.

The `REQ` command tells `nsqd` that the message should be re-queued (with an optional parameter
specifying the amount of time to *defer* additional attempts). If the optional parameter is not
specified by the client, the client library should automatically calculate the duration in relation
to the number of attempts to process the message (a multiple is typically sufficient). The client
library should discard messages that exceed the configured max attempts. When this occurs, a
user-supplied callback should be executed to notify and enable special handling.

If the message handler requires more time than the configured message timeout, the `TOUCH` command
can be used to reset the timer on the `nsqd` side. This can be done repeatedly until the message is
either `FIN` or `REQ`, up to the `nsqd` producer's configured max timeout.  Client libraries should
never *automatically* `TOUCH` on behalf of the client.

If the `nsqd` instance receives *no* response, the message will time out and be automatically
re-queued for delivery to an available client.

Finally, a property of each message is the number of attempts. Client libraries should compare this
value against the configured max and discard messages that have exceeded it. When a message is
discarded there should be a callback fired. Typical default implementations of this callback might
include writing to a directory on disk, logging, etc. The user should be able to override the
default handling.

## <a name="rdy_state"></a>RDY State

Because messages are *pushed* from `nsqd` to clients we needed a way to manage the flow of data in
userland rather than relying on low-level TCP semantics. A client's `RDY` state is NSQ's flow
control mechanism.

As outlined in the [configuration section](#configuration), a consumer is configured with a
`max_in_flight`. This is a concurrency and performance knob, e.g. some downstream systems are able
to more-easily batch process messages and benefit greatly from a higher `max-in-flight`.

When a client connects to `nsqd` (and subscribes) it is placed in an initial `RDY` state of `0`. No
messages will be sent to the client.

Client libraries have a few responsibilities:

 1. bootstrap and evenly distribute the configured `max_in_flight` to all connections.
 2. never allow the aggregate sum of `RDY` counts for all connections (`total_rdy_count`) 
    to exceed the configured `max_in_flight`.
 3. never exceed the per connection `nsqd` configured `max_rdy_count`.
 4. expose an API method to reliably indicate message flow starvation

### <a name="bootstrap_and_distribution"></a>1. Bootstrap and Distribution

There are a few considerations when choosing an appropriate `RDY` count for a connection (in order
to evenly distribute `max_in_flight`):

 * the # of connections is dynamic, often times not even known in advance (ie. when
  *discovering* producers via `nsqlookupd`).
 * `max_in_flight` may be *lower* than your number of connections

To kickstart message flow a client library needs to send an *initial* `RDY` count. Because the
eventual number of connections is often not known ahead of time it should start with a value of `1`
so that the client library does not unfairly favor the first connection(s).

Additionally, after each message is processed, the client library should evaluate whether or not
it's time to update `RDY` state. An update should be triggered if the current value is `0` or if it
is below ~25% of the last value sent.

The client library should always attempt to evenly distribute `RDY` count across all connections.
Typically, this is implemented as `max_in_flight / num_conns`.

However, when `max_in_flight < num_conns` this simple formula isn't sufficient. In this state,
client libraries should perform a dynamic runtime evaluation of producer "liveness" by measuring the
duration of time since it last received a message for a connection. After a configurable expiration,
it should *re-distribute* whatever `RDY` count is available to a new (random) set of producers. By
doing this, you guarantee that you'll (eventually) find producers with messages. Clearly this has a
latency impact.

### 2. Maintaining `max_in_flight`

The client library should maintain a ceiling for the maximum number of messages in flight for a
given consumer. Specifically, the aggregate sum of each connection's `RDY` count should never exceed
the configured `max_in_flight`.

Below is example code in Python to determine whether or not the proposed RDY count is valid for a
given connection:

```python
def send_ready(reader, conn, count):
    if (reader.total_ready_count + count) > reader.max_in_flight:
        return
    
    conn.send_ready(count)
    conn.rdy_count = count
    reader.total_ready_count += count
```

### 3. Producer Max RDY Count

Each `nsqd` is configurable with a `--max-rdy-count` (see [feature
negotiation](#feature_negotiation) for more information on the handshake a client can perform to
ascertain this value). If the client sends a `RDY` count that is outside of the acceptable range its
connection will be forcefully closed. For backwards compatibility, this value should be assumed to
be `2500` if the `nsqd` instance does not support [feature negotiation](#feature_negotiation).

### 4. Message Flow Starvation

Finally, the client library should provide an API method to indicate message flow starvation. It is
insufficient for clients (in their message handlers) to simply compare the number of messages they
have in-flight vs. their configured `max_in_flight` in order to decide to "process a batch". There
are two cases when this is problematic:

 1. When clients configure `max_in_flight > 1`, due to variable `num_conns`, there are
    cases where `max_in_flight` is not evenly divisible by `num_conns`. Because the contract states 
    that you should never *exceed* `max_in_flight`, you must round down, and you end up with cases 
    where the sum of all `RDY` counts is less than `max_in_flight`.
 2. Consider the case where only a subset of producers have messages. Because of the expected [even 
    distribution](#bootstrap_and_distribution) of `RDY` count, those active producers only have a 
    fraction of the configured `max_in_flight`.

In both cases, a client will never actually receive `max_in_flight` # of messages. Therefore, the
client library should expose a method `is_starved` that will evaluate whether *any* of the
connections are starved, as follows:

```python
def is_starved(conns):
    for c in conns:
        # the constant 0.85 is designed to *anticipate* starvation rather than wait for it
        if c.in_flight > 0 and c.in_flight >= (c.last_ready * 0.85):
            return True
    return False
```

The `is_starved` method should be used by message handlers to reliably identify when to process a
batch of messages.

## <a name="backoff"></a>Backoff

The question of what to do when message processing fails is a complicated one to answer. The
[message handling](#message_handling) section detailed client library behavior that would *defer*
the processing of failed messages for some (increasing) duration of time. The other piece of the
puzzle is whether or not to reduce throughput. The interplay between these two pieces of
functionality is crucial for overall system stability.

By slowing down the rate of processing, or "backing off", the consumer allows the downstream system
to recover from transient failure. However, this behavior should be configurable as it isn't always
desirable, such as situations where *latency* is prioritized.

Backoff should be implemented by sending `RDY 0` to the appropriate producers, stopping message
flow. The duration of time to remain in this state should be calculated based on the number of
repeated failures (exponential). Similarly, successful processing should reduce this duration until
the reader is no longer in a backoff state.

While a reader is in a backoff state, after the timeout expires, the client library should only ever
send `RDY 1` regardless of `max_in_flight`. This effectively "tests the waters" before returning to
full throttle. Additionally, during a backoff timeout, the client library should ignore any success
or failure results with respect to calculating backoff duration (i.e. it should only take into
account *one* result per backoff timeout).

![nsq_client_flow](images/nsq_client_flow.png)

## Bringing It All Together

Distributed systems are fun.

The interactions between the various components of an NSQ cluster work in concert to provide a
platform on which to build robust, performant, and stable infrastructure. We hope this guide shed
some light as to how important the client's role is.

In terms of actually implementing all of this, we treat [pynsq][pynsq] and [go-nsq][go-nsq] as our
reference codebases. The structure of [pynsq][pynsq] can be broken down into three core components:

 * `Message` - a high-level message object, which exposes stateful methods for responding to the 
   `nsqd` producer (`FIN`, `REQ`, `TOUCH`, etc.) as well as metadata such as attempts and timestamp.
 * `Connection` - a high-level wrapper around a TCP connection to a specific `nsqd` producer, which 
   has knowledge of in flight messages, its `RDY` state, negotiated features, and various timings.
 * `Reader` - the front-facing API a user interacts with, which handles discovery, creates 
   connections (and subscribes), bootstraps and manages `RDY` state, parses raw incoming data, 
   creates `Message` objects, and dispatches messages to handlers.

We're happy to help support anyone interested in building client libraries for NSQ. We're looking
for contributors to continue to expand our language support as well as flesh out functionality in
existing libraries. The community has already open sourced client libraries for 7 languages:

 * [go-nsq][go-nsq] Go (official)
 * [pynsq][pynsq] Python (official)
 * [libnsq][libnsq] C
 * [nsq-java][nsq-java] Java
 * [TrendrrNSQClient][TrendrrNSQClient] Java
 * [nsqjava][nsqjava] Java
 * [nsq-client][nsq-client] Node.js
 * [nodensq][nodensq] Node.js
 * [nsqphp][nsqphp] PHP
 * [ruby_nsq][ruby_nsq] Ruby

[go-nsq]: https://github.com/bitly/nsq/tree/master/nsq
[pynsq]: https://github.com/bitly/pynsq
[libnsq]: https://github.com/mreiferson/libnsq
[nsq-java]: https://github.com/bitly/nsq-java
[TrendrrNSQClient]: https://github.com/dustismo/TrendrrNSQClient
[nsqjava]: https://github.com/domwong/nsqjava
[nsq-client]: https://github.com/jmanero/nsq-client
[nodensq]: https://github.com/phillro/nodensq
[nsqphp]: https://github.com/davegardnerisme/nsqphp
[ruby_nsq]: https://github.com/ClarityServices/ruby_nsq
