---
title: Internals
layout: post
category: overview
permalink: /overview/internals.html
---

NSQ is composed of 3 daemons:

 * **[nsqd][nsqd]** is the daemon that receives, queues, and delivers messages to clients.

 * **[nsqlookupd][nsqlookupd]** is the daemon that manages topology information
   and provides an eventually consistent discovery service.

 * **[nsqadmin][nsqadmin]** is a web UI to introspect the cluster in realtime
   (and perform various administrative tasks).

Data flow in NSQ is modeled as a tree of *streams* and *consumers*. A **topic** is a distinct
stream of data. A **channel** is a logical grouping of consumers subscribed to a given **topic**.

![topics/channels](https://f.cloud.github.com/assets/187441/1700696/f1434dc8-6029-11e3-8a66-18ca4ea10aca.gif)

A single **nsqd** can have many topics and each topic can have many channels. A channel
receives a *copy* of all the messages for the topic, enabling *multicast* style delivery while each
message on a channel is *distributed* amongst its subscribers, enabling load-balancing.

These primitives form a powerful framework for expressing a variety of [simple and complex
topologies][topology_patterns].

For more information about the design of NSQ see the [design doc][design_doc].

## Topics and Channels

Topics and channels, the core primitives of NSQ, best exemplify how the design of the system
translates seamlessly to the features of Go.

Go's channels (henceforth referred to as "go-chan" for disambiguation) are a natural way to express
queues, thus an NSQ topic/channel, at its core, is just a *buffered* go-chan of `Message` pointers.
The size of the buffer is equal to the `--mem-queue-size` configuration parameter.

After reading data off the wire, the act of publishing a message to a topic involves:

 1. instantiation of a `Message` struct (and allocation of the message body `[]byte`)
 2. read-lock to get the `Topic`
 3. read-lock to check for the ability to publish
 4. send on a buffered go-chan

To get messages from a topic to its channels the topic cannot rely on typical go-chan receive
semantics, because multiple goroutines receiving on a go-chan would *distribute* the messages
while the desired end result is to *copy* each message to every channel (goroutine).

Instead, each topic maintains 3 primary goroutines. The first one, called `router`, is responsible
for reading newly published messages off the incoming go-chan and storing them in a queue (memory
or disk).

The second one, called `messagePump`, is responsible for copying and pushing messages to channels as
described above.

The third is responsible for `DiskQueue` IO and will be discussed later.

Channels are a *little* more complicated but share the underlying goal of exposing a *single* input
and *single* output go-chan (to abstract away the fact that, internally, messages might be in
memory or on disk):

![queue goroutine](https://f.cloud.github.com/assets/187441/1698990/682fc358-5f76-11e3-9b05-3d5baba67f13.png)

Additionally, each channel maintains 2 time-ordered priority queues responsible for deferred and
in-flight message timeouts (and 2 accompanying goroutines for monitoring them).

Parallelization is improved by managing a *per-channel* data structure, rather than relying on the
Go runtime's *global* timer scheduler.

**Note:** Internally, the Go runtime uses a single priority queue and goroutine to manage timers.
This supports (but is not limited to) the entirety of the `time` package. It normally obviates the
need for a *user-land* time-ordered priority queue but it's important to keep in mind that it's a
*single* data structure with a *single* lock, potentially impacting `GOMAXPROCS > 1` performance.
See [runtime/time.goc][runtime_time].

## Backend / DiskQueue

One of NSQ's design goals is to bound the number of messages kept in memory. It does this by
transparently writing message overflow to disk via `DiskQueue` (which owns the *third* primary
goroutine for a topic or channel).

Since the memory queue is just a go-chan, it's trivial to route messages to memory first, if
possible, then fallback to disk:

{% highlight go %}
for msg := range c.incomingMsgChan {
	select {
	case c.memoryMsgChan <- msg:
	default:
		err := WriteMessageToBackend(&msgBuf, msg, c.backend)
		if err != nil {
			// ... handle errors ...
		}
	}
}
{% endhighlight %}

Taking advantage of Go's `select` statement allows this functionality to be expressed in just a few
lines of code: the `default` case above only executes if `memoryMsgChan` is full.

NSQ also has the concept of **ephemeral** channels. Ephemeral channels *discard* message overflow
(rather than write to disk) and disappear when they no longer have clients subscribed.  This is
a perfect use case for Go's interfaces. Topics and channels have a struct member declared
as a `Backend` *interface* rather than a concrete type. Normal topics and channels use a
`DiskQueue` while ephemeral channels stub in a `DummyBackendQueue`, which implements a no-op
`Backend`.

## Reducing GC Pressure

In any garbage collected environment you're subject to the tension between throughput (doing useful
work), latency (responsiveness), and resident set size (footprint).

As of Go 1.2, the GC is mark-and-sweep (parallel), non-generational, non-compacting, stop-the-world
and mostly precise . It's *mostly* precise because the remainder of the work wasn't completed in
time (it's slated for Go 1.3).

The Go GC will certainly continue to improve, but the universal truth is: ***the less garbage you
create the less time you'll collect***.

First, it's important to understand how the GC is behaving *under real workloads*. To this end,
**nsqd** publishes GC stats in [statsd][statsd] format (alongside other internal metrics).
**nsqadmin** displays graphs of these metrics, giving you insight into the GC's impact in both
frequency and duration:

![single node view](https://f.cloud.github.com/assets/187441/1699828/8df666c6-5fc8-11e3-95e6-360b07d3609d.png)

In order to actually *reduce* garbage you need to know where it's being generated. Once again the
Go toolchain provides the answers:

 1. Use the [`testing`][testing] package and `go test -benchmem` to benchmark hot code paths. It
    profiles the number of allocations per iteration (and benchmark runs can be compared with
    [`benchcmp`][benchcmp]).
 2. Build using `go build -gcflags -m`, which outputs the result of [escape analysis][escape_an].

With that in mind, the following optimizations proved useful for **nsqd**:

 1. Avoid `[]byte` to `string` conversions.
 2. Re-use buffers or objects (and someday possibly [`sync.Pool`][sync_pool]
    aka [issue 4720][issue_4720]).
 3. Pre-allocate slices (specify capacity in `make`) and always know the number
    and size of items over the wire.
 4. Apply sane limits to various configurable dials (such as message size).
 5. Avoid boxing (use of `interface{}`) or unnecessary wrapper types (like a `struct` for
    a "multiple value" go-chan).
 6. Avoid the use of `defer` in hot code paths (it allocates).

### TCP Protocol

The [NSQ TCP protocol][protocol_spec] is a shining example of a section where these GC optimization
concepts are utilized to great effect.

The protocol is structured with length prefixed frames, making it straightforward and performant to
encode and decode:

    [x][x][x][x][x][x][x][x][x][x][x][x]...
    |  (int32) ||  (int32) || (binary)
    |  4-byte  ||  4-byte  || N-byte
    ------------------------------------...
        size      frame ID     data

Since the exact type and size of a frame's components are known ahead of time, we can avoid the
[`encoding/binary`][encoding_binary] package's convenience [`Read()`][binary_read] and
[`Write()`][binary_write] wrappers (and their extraneous interface lookups and conversions) and
instead call the appropriate [`binary.BigEndian`][byte_order] methods directly.

To reduce socket IO syscalls, client `net.Conn` are wrapped with [`bufio.Reader`][bufio_reader] and
[`bufio.Writer`][bufio_writer]. The `Reader` exposes [`ReadSlice()`][readslice], which reuses its
internal buffer. This nearly eliminates allocations while reading off the socket, greatly reducing
GC pressure. This is possible because the data associated with most commands does not escape (in
the edge cases where this is not true, the data is *explicitly* copied).

At an even lower level, a `MessageID` is declared as `[16]byte` to be able to use it as a `map`
key (slices cannot be used as map keys). However, since data read from the socket is stored as
`[]byte`, rather than produce garbage by allocating `string` keys, and to avoid a copy from the
slice to the backing array of the `MessageID`, the `unsafe` package is used to cast the slice
directly to a `MessageID`:

    id := *(*nsq.MessageID)(unsafe.Pointer(&msgID))

**Note:** *This is a hack*. It wouldn't be necessary if this was optimized by the compiler and
[Issue 3512][issue_3512] is open to potentially resolve this. It's also worth reading through
[issue 5376][issue_5376], which talks about the possibility of a "const like" `byte` type that
could be used interchangeably where `string` is accepted, *without* allocating and copying.

Similarly, the Go standard library only provides numeric conversion methods on a `string`. In order
to avoid `string` allocations, **nsqd** uses a [custom base 10 conversion method][base10_convert]
that operates directly on a `[]byte`.

These may seem like micro-optimizations but the TCP protocol contains some of the *hottest* code
paths. In aggregate, at the rate of tens of thousands of messages per second, they have a
significant impact on the number of allocations and overhead:

    benchmark                    old ns/op    new ns/op    delta
    BenchmarkProtocolV2Data           3575         1963  -45.09%

    benchmark                    old ns/op    new ns/op    delta
    BenchmarkProtocolV2Sub256        57964        14568  -74.87%
    BenchmarkProtocolV2Sub512        58212        16193  -72.18%
    BenchmarkProtocolV2Sub1k         58549        19490  -66.71%
    BenchmarkProtocolV2Sub2k         63430        27840  -56.11%

    benchmark                   old allocs   new allocs    delta
    BenchmarkProtocolV2Sub256           56           39  -30.36%
    BenchmarkProtocolV2Sub512           56           39  -30.36%
    BenchmarkProtocolV2Sub1k            56           39  -30.36%
    BenchmarkProtocolV2Sub2k            58           42  -27.59%

## HTTP

NSQ's HTTP API is built on top of Go's [`net/http`][net_http] package. Because it's *just* HTTP, it
can be leveraged in almost any modern programming environment without special client libraries.

Its simplicity belies its power, as one of the most interesting aspects of Go's HTTP tool-chest
is the wide range of debugging capabilities it supports. The [`net/http/pprof`][net_http_pprof]
package integrates directly with the native HTTP server, exposing endpoints to retrieve CPU, heap,
goroutine, and OS thread profiles. These can be targeted directly from the `go` tool:

    $ go tool pprof http://127.0.0.1:4151/debug/pprof/profile

This is a tremendously valuable for debugging and profiling a *running* process!

In addition, a `/stats` endpoint returns a slew of metrics in either JSON or pretty-printed text,
making it easy for an administrator to introspect from the command line in realtime:

    $ watch -n 0.5 'curl -s http://127.0.0.1:4151/stats | grep -v connected'

This produces continuous output like:

    [page_views     ] depth: 0     be-depth: 0     msgs: 105525994 e2e%: 6.6s, 6.2s, 6.2s
        [page_view_counter        ] depth: 0     be-depth: 0     inflt: 432  def: 0    re-q: 34684 timeout: 34038 msgs: 105525994 e2e%: 5.1s, 5.1s, 4.6s
        [realtime_score           ] depth: 1828  be-depth: 0     inflt: 1368 def: 0    re-q: 25188 timeout: 11336 msgs: 105525994 e2e%: 9.0s, 9.0s, 7.8s
        [variants_writer          ] depth: 0     be-depth: 0     inflt: 592  def: 0    re-q: 37068 timeout: 37068 msgs: 105525994 e2e%: 8.2s, 8.2s, 8.2s

    [poll_requests  ] depth: 0     be-depth: 0     msgs: 11485060 e2e%: 167.5ms, 167.5ms, 138.1ms
        [social_data_collector    ] depth: 0     be-depth: 0     inflt: 2    def: 3    re-q: 7568  timeout: 402   msgs: 11485060 e2e%: 186.6ms, 186.6ms, 138.1ms

    [social_data    ] depth: 0     be-depth: 0     msgs: 60145188 e2e%: 199.0s, 199.0s, 199.0s
        [events_writer            ] depth: 0     be-depth: 0     inflt: 226  def: 0    re-q: 32584 timeout: 30542 msgs: 60145188 e2e%: 6.7s, 6.7s, 6.7s
        [social_delta_counter     ] depth: 17328 be-depth: 7327  inflt: 179  def: 1    re-q: 155843 timeout: 11514 msgs: 60145188 e2e%: 234.1s, 234.1s, 231.8s

    [time_on_site_ticks] depth: 0     be-depth: 0     msgs: 35717814 e2e%: 0.0ns, 0.0ns, 0.0ns
        [tail821042#ephemeral     ] depth: 0     be-depth: 0     inflt: 0    def: 0    re-q: 0     timeout: 0     msgs: 33909699 e2e%: 0.0ns, 0.0ns, 0.0ns

Finally, each new Go release typically brings [measurable performance gains][autobench]. It's
always nice when recompiling against the latest version of Go provides a free boost!

## Dependencies

Coming from other ecosystems, Go's philosophy (or lack thereof) on managing dependencies takes a
little time to get used to.

NSQ evolved from being a single giant repo, with *relative imports* and little to no separation
between internal packages, to fully embracing the recommended best practices with respect to
structure and dependency management.

There are two main schools of thought:

 1. **Vendoring**: copy dependencies at the correct revision into your application's repo
    and modify your import paths to reference the local copy.
 2. **Virtual Env**: list the revisions of dependencies you require and at build time, produce a
    pristine `GOPATH` environment containing those pinned dependencies.

**Note:** This really only applies to *binary* packages as it doesn't make sense for an importable
package to make intermediate decisions as to which version of a dependency to use.

NSQ uses **[gpm][gpm]** to provide support for (2) above.

It works by recording your dependencies in a [`Godeps`][godeps] file, which we later use to
construct a `GOPATH` environment.

## Testing

Go provides solid built-in support for writing tests and benchmarks and, because Go makes
it so easy to model concurrent operations, it's trivial to stand up a full-fledged instance of
**nsqd** inside your test environment.

However, there was one aspect of the initial implementation that became problematic for testing:
global state. The most obvious offender was the use of a global variable that held the reference to
the instance of **nsqd** at runtime, i.e. `var nsqd *NSQd`.

Certain tests would inadvertently mask this global variable in their local scope by using
short-form variable assignment, i.e. `nsqd := NewNSQd(...)`. This meant that the global reference
did not point to the instance that was currently running, breaking tests.

To resolve this, a `Context` struct is passed around that contains configuration metadata and a
reference to the parent **nsqd**. All references to global state were replaced with this local
`Context`, allowing children (topics, channels, protocol handlers, etc.) to safely access this data
and making it more reliable to test.

## Robustness

A system that isn't robust in the face of changing network conditions or unexpected events is a
system that will not perform well in a distributed production environment.

NSQ is designed and implemented in a way that allows the system to tolerate failure and behave in a
consistent, predictable, and unsurprising way.

The overarching philosophy is to fail fast, treat errors as fatal, and provide a means to debug any
issues that do occur.

But, in order to *react* you need to be able to *detect* exceptional conditions...

### Heartbeats and Timeouts

The NSQ TCP protocol is push oriented. After connection, handshake, and subscription the consumer
is placed in a `RDY` state of `0`. When the consumer is ready to receive messages it updates that
`RDY` state to the number of messages it is willing to accept. NSQ client libraries continually
manage this behind the scenes, resulting in a flow-controlled stream of messages.

Periodically, **nsqd** will send a heartbeat over the connection. The client can configure the
interval between heartbeats but **nsqd** expects a response before it sends the next one.

The combination of application level heartbeats and `RDY` state avoids [head-of-line
blocking][hol_blocking], which can otherwise render heartbeats useless (i.e. if a consumer is
behind in processing message flow the OS's receive buffer will fill up, blocking heartbeats).

To guarantee progress, all network IO is bound with deadlines relative to the configured heartbeat
interval. This means that you can literally unplug the network connection between **nsqd** and a
consumer and it will detect and properly handle the error.

When a fatal error is detected the client connection is forcibly closed. In-flight messages are
timed out and re-queued for delivery to another consumer. Finally, the error is logged and various
internal metrics are incremented.

### Managing Goroutines

It's surprisingly easy to *start* goroutines. Unfortunately, it isn't quite as easy to orchestrate
their cleanup. Avoiding deadlocks is also challenging. Most often this boils down to an ordering
problem, where a goroutine receiving on a go-chan exits *before* the upstream goroutines sending on
it.

Why care at all though? It's simple, an orphaned goroutine is a *memory leak*. Memory leaks in long
running daemons are bad, especially when the expectation is that your process will be stable when
all else fails.

To further complicate things, a typical **nsqd** process has *many* goroutines involved in message
delivery. Internally, message "ownership" changes often. To be able to shutdown cleanly, it's
incredibly important to account for all *intraprocess* messages.

Although there aren't any magic bullets, the following techniques make it a little easier to
manage...

#### WaitGroups

The [`sync`][sync] package provides [`sync.WaitGroup`][sync_waitgroup], which can be used to
perform accounting of how many goroutines are live (and provide a means to wait on their exit).

To reduce the typical boilerplate, **nsqd** uses this wrapper:

{% highlight go %}
type WaitGroupWrapper struct {
	sync.WaitGroup
}

func (w *WaitGroupWrapper) Wrap(cb func()) {
	w.Add(1)
	go func() {
		cb()
		w.Done()
	}()
}

// can be used as follows:
wg := WaitGroupWrapper{}
wg.Wrap(func() { n.idPump() })
...
wg.Wait()
{% endhighlight %}

#### Exit Signaling

The easiest way to trigger an event in multiple child goroutines is to provide a single go-chan
that you close when ready. All pending receives on that go-chan will activate, rather than having
to send a separate signal to each goroutine.

{% highlight go %}
func work() {
    exitChan := make(chan int)
    go task1(exitChan)
    go task2(exitChan)
    time.Sleep(5 * time.Second)
    close(exitChan)
}
func task1(exitChan chan int) {
    <-exitChan
    log.Printf("task1 exiting")
}

func task2(exitChan chan int) {
    <-exitChan
    log.Printf("task2 exiting")
}
{% endhighlight %}

#### Synchronizing Exit

It was quite difficult to implement a reliable, deadlock free, exit path that accounted for all
in-flight messages. A few tips:

 1. Ideally the goroutine responsible for sending on a go-chan should also be responsible for
    closing it.

 2. If messages cannot be lost, ensure that pertinent go-chans are emptied (especially
    unbuffered ones!) to guarantee senders can make progress.

 3. Alternatively, if a message is no longer relevant, sends on a single go-chan should be
    converted to a `select` with the addition of an exit signal (as discussed above) to guarantee
    progress.

 4. The general order should be:

     1. Stop accepting new connections (close listeners)
     2. Signal exit to child goroutines (see above)
     3. Wait on `WaitGroup` for goroutine exit (see above)
     4. Recover buffered data
     5. Flush anything left to disk

#### Logging

Finally, the most important tool at your disposal is to ***log the entrance and exit of your
goroutines!***. It makes it *infinitely* easier to identify the culprit in the case of deadlocks or
leaks.

**nsqd** log lines include information to correlate goroutines with their siblings (and parent),
such as the client's remote address or the topic/channel name.

The logs are verbose, but not verbose to the point where the log is overwhelming. There's a fine
line, but **nsqd** leans towards the side of having *more* information in the logs when a fault
occurs rather than trying to reduce chattiness at the expense of usefulness.

[nsqd]: http://nsq.io/components/nsqd.html
[nsqlookupd]: http://nsq.io/components/nsqlookupd.html
[nsqadmin]: http://nsq.io/components/nsqadmin.html
[design_doc]: http://nsq.io/overview/design.html
[protocol_spec]: http://nsq.io/clients/tcp_protocol_spec.html
[gpm]: https://github.com/pote/gpm
[hol_blocking]: http://en.wikipedia.org/wiki/Head-of-line_blocking
[encoding_binary]: http://golang.org/pkg/encoding/binary/
[byte_order]: http://golang.org/pkg/encoding/binary/#ByteOrder
[bufio_reader]: http://golang.org/pkg/bufio/#Reader
[bufio_writer]: http://golang.org/pkg/bufio/#Writer
[readslice]: http://golang.org/pkg/bufio/#Reader.ReadSlice
[sync]: http://golang.org/pkg/sync/
[sync_waitgroup]: http://golang.org/pkg/sync/#WaitGroup
[sync_rwmutex]: http://golang.org/pkg/sync/#RWMutex
[binary_read]: http://golang.org/pkg/encoding/binary/#Read
[binary_write]: http://golang.org/pkg/encoding/binary/#Write
[net_http]: http://golang.org/pkg/net/http/
[net_http_pprof]: http://golang.org/pkg/net/http/pprof/
[statsd]: https://github.com/etsy/statsd/
[sync_pool]: https://groups.google.com/forum/#!topic/golang-dev/kJ_R6vYVYHU
[testing]: http://golang.org/pkg/testing/
[benchcmp]: http://golang.org/misc/benchcmp
[issue_3512]: https://code.google.com/p/go/issues/detail?id=3512
[issue_4720]: https://code.google.com/p/go/issues/detail?id=4720
[issue_5376]: https://code.google.com/p/go/issues/detail?id=5376
[godeps]: https://github.com/bitly/nsq/blob/master/Godeps
[runtime_time]: http://golang.org/src/pkg/runtime/time.goc?s=1684:1787#L83
[autobench]: https://github.com/davecheney/autobench
[escape_an]: http://en.wikipedia.org/wiki/Escape_analysis
[base10_convert]: https://github.com/bitly/nsq/blob/master/util/byte_base10.go#L7-L27
[topology_patterns]: http://nsq.io/deployment/topology_patterns.html
