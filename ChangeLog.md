# NSQ Changelog

## Binaries

### 0.2.22-alpha

**Upgrading from 0.2.21**: there are no backward incompatible changes in this release.

New Features / Enhancements:

 * #228 - nsqadmin displays tombstoned topics in the /nodes list

Bug Fixes:

 * #228 - nsqlookupd/nsqadmin would display inactive nodes in /nodes list
 * #216 - fix edge cases in nsq_to_file that caused empty files

### 0.2.21 - 2013-06-07

**Upgrading from 0.2.20**: there are no backward incompatible changes in this release.

This release introduces a significant new client feature as well as a slew of consistency and
recovery improvements to diskqueue.

First, we expanded the feature negotiation options for clients. There are many cases where you want
different output buffering semantics from `nsqd` to your client. You can now control both
output buffer size and the output buffer timeout via new fields in the `IDENTIFY` command. You can
even disable output buffering if low latency is a priority.

You can now specify a duration between fsyncs via `--sync-timeout`. This is a far better way to
manage when the process fsyncs messages to disk (vs the existing `--sync-every` which is based on #
of messages). `--sync-every` is now considered a deprecated option and will be removed in a future
release.

Finally, `0.2.20` introduced a significant regression in #176 where a topic would not write messages
to its channels. It is recommended that all users running `0.2.20` upgrade to this release. For
additional information see #217.

New Features / Enhancements:

 * #214 - add --sync-timeout for time based fsync, improve when diskqueue syncs
 * #196 - client configurable output buffering
 * #190 - nsq_tail generates a random #ephemeral channel

Bug Fixes:

 * #218/#220 - expose --statsd-interval for nsqadmin to handle non 60s statsd intervals
 * #217 - fix new topic channel creation regression from #176 (thanks @elubow)
 * #212 - dont use port in nsqadmin cookies
 * #214 - dont open diskqueue writeFile with O_APPEND
 * #203/#211 - diskqueue depth accounting consistency
 * #207 - failure to write a heartbeat is fatal / reduce error log noise
 * #206 - use broadcast address for statsd prefix
 * #205 - cleanup example utils exit

### 0.2.20 - 2013-05-13

**Upgrading from 0.2.19**: there are no backward incompatible changes in this release.

This release adds a couple of convenient features (such as adding the ability to empty a *topic*)
and continues our work to reduce garbage produced at runtime to relieve GC pressure in the Go
runtime.

`nsqd` now has two new flags to control the max value clients can use to set their heartbeat
interval as well as adjust a clients maximum RDY count. This is all set/communicated via `IDENTIFY`.

`nsqadmin` now displays `nsqd` -> `nsqlookupd` connections in the "nodes" view. This is useful for
visualizing how the topology is connected as well as situations where `--broadcast-address` is being
used incorrectly.

`nsq_to_http` now has a "host pool" mode where upstream state will be adjusted based on
successful/failed requests and for failures, upstreams will be exponentially backed off. This is an
incredibly useful routing mode.

As for bugs, we fixed an issue where "fatal" client errors were not actually being treated as fatal.
Under certain conditions deleting a topic would not clean up all of its files on disk. There was a
reported issue where the `--data-path` was not writable by the process and this was only discovered
after message flow began. We added a writability check at startup to improve feedback. Finally.
`deferred_count` was being sent as a counter value to statsd, it should be a gauge.

New Features / Enhancements:

 * #197 - nsqadmin nodes list improvements (show nsqd -> lookupd conns)
 * #192 - add golang runtime version to daemon version output
 * #183 - ability to empty a topic
 * #176 - optimizations to reduce garbage, copying, locking
 * #184 - add table headers to nsqadmin channel view (thanks @elubow)
 * #174/#186 - nsq_to_http hostpool mode and backoff control
 * #173/#187 - nsq_stat utility for command line introspection
 * #175 - add nsqd --max-rdy-count configuration option
 * #178 - add nsqd --max-heartbeat-interval configuration option

Bug Fixes:

 * #198 - fix fatal errors not actually being fatal
 * #195 - fix delete topic does not delete all diskqueue files
 * #193 - fix data race in channel requeue
 * #185 - ensure that --data-path is writable on startup
 * #182 - fix topic deletion ordering to prevent race conditions with lookupd/diskqueue
 * #179 - deferred_count as gauge for statsd
 * #173/#188/#191 - fix nsqadmin counter template error; fix nsqadmin displaying negative rates

### 0.2.19 - 2013-04-11

**Upgrading from 0.2.18**: there are no backward incompatible changes in this release.

This release is a small release that introduces one major client side feature and resolves one
critical bug.

`nsqd` clients can now configure their own heartbeat interval. This is important because as of
`0.2.18` *all* clients (including producers) received heartbeats by default. In certain cases
receiving a heartbeat complicated "simple" clients that just wanted to produce messages and not
handle asynchronous responses. This gives flexibility for the client to decide how it would like
behave.

A critical bug was discovered where emptying a channel would leave client in-flight state
inconsistent (it would not zero) which limited deliverability of messages to those clients.

New Features / Enhancements:

 * #167 - 'go get' compatibility
 * #158 - allow nsqd clients to configure (or disable) heartbeats

Bug Fixes:

 * #171 - fix race conditions identified testing against go 1.1 (scheduler improvements)
 * #160 - empty channel left in-flight count inconsistent (thanks @dmarkham)

### 0.2.18 - 2013-02-28

**Upgrading from 0.2.17**: all V2 clients of nsqd now receive heartbeats (previously only clients
that subscribed would receive heartbeats, excluding TCP *producers*).

**Upgrading from 0.2.16**: follow the notes in the 0.2.17 changelog for upgrading from 0.2.16.

Beyond the important note above regarding heartbeats this release includes `nsq_tail`, an extremely
useful utility application that can be used to introspect a topic on the command line. If statsd is
enabled (and graphite in `nsqadmin`) we added the ability to retrieve rates for display in
`nsqadmin`.

We resolved a few critical issues with data consistency in `nsqlookupd` when channels and topics are
deleted. First, deleting a topic would cause that producer to disappear from `nsqlookupd` for all
topics. Second, deleting a channel would cause that producer to disappear from the topic list in
`nsqlookupd`.

New Features / Enhancements:

 * #131 - all V2 nsqd clients get heartbeats
 * #154 - nsq_tail example reader
 * #143 - display message rates in nsqadmin

Bug Fixes:

 * #148 - store tombstone data per registration in nsqlookupd
 * #153 - fix large graph formulas in nsqadmin
 * #150/#151 - fix topics disappearing from nsqlookupd when channels are deleted

### 0.2.17 - 2013-02-07

**Upgrading from 0.2.16**: IDENTIFY and SUB now return success responses (they previously only
responded to errors). The official Go and Python libraries are forwards/backwards compatible with
this change however 3rd party client libraries may not be.

**Upgrading from 0.2.15**: in #132 deprecations in SUB were removed as well as support for the old,
line oriented, `nsqd` metadata file format. For these reasons you should upgrade to `0.2.16` first.

New Features / Enhancements:

 * #119 - add TOUCH command to nsqd
 * #142 - add --broadcast-address flag to nsqd/nsqadmin (thanks @dustismo)
 * #135 - atomic MPUB
 * #133 - improved protocol fatal error handling and responses; IDENTIFY/SUB success responses
 * #118 - switch nsqadmin actions to POST and require confirmation
 * #117/#147 - nsqadmin action POST notifications
 * #122 - configurable msg size limits
 * #132 - deprecate identify in SUB and old nsqd metadata file format

Bug Fixes:

 * #144 - empty channel should clear inflight/deferred messages
 * #140 - fix MPUB protocol documentation
 * #139 - fix nsqadmin handling of legacy statsd prefixes for graphs
 * #138/#145 - fix nsqadmin action redirect handling
 * #134 - nsqd to nsqlookupd registration fixes
 * #129 - nsq_to_file gzip file versioning
 * #106 - nsqlookupd topic producer tombstones
 * #100 - sane handling of diskqueue read errors
 * #123/#125 - fix notify related exit deadlock

### 0.2.16 - 2013-01-07

**Upgrading from 0.2.15**: there are no backward incompatible changes in this release.

However, this release introduces the `IDENTIFY` command (which supersedes sending 
metadata along with `SUB`) for clients of `nsqd`.  The old functionality will be 
removed in a future release.

 * #114 persist paused channels through restart
 * #121 fix typo preventing compile of bench_reader (thanks @datastream)
 * #120 fix nsqd crash when empty command is sent (thanks @michaelhood)
 * #115 nsq_to_file --filename-format --datetime-format parameter and fix
 * #101 fix topic/channel delete operations ordering
 * #98 nsqadmin fixes when not using lookupd
 * #90/#108 performance optimizations / IDENTIFY protocol support in nsqd. For 
   a single consumer of small messages (< 4k) increases throughput ~400% and 
   reduces # of allocations ~30%.
 * #105 strftime compatible datetime format
 * #103 nsq_to_http handler logging
 * #102 compatibility with Go tip
 * #99 nsq_to_file --gzip flag
 * #95 proxy graphite requests through nsqadmin
 * #93 fix nqd API response for no topics
 * #92 graph rendering options
 * #86 nsq_to_http Content-Length headers
 * #89 gopkg doc updates
 * #88 move pynsq to it's own repo
 * #81/#87 reader improvements / introduced MPUB. Fix bug for mem-queue-size < 10
 * #76 statsd/graphite support
 * #75 administrative ability to create topics and channels

### 0.2.15 - 2012-10-25

 * #84 fix lookupd hanging on to ephemeral channels w/ no producers
 * #82 add /counter page to nsqadmin
 * #80 message size benchmark
 * #78 send Content-Length for nsq_to_http requests
 * #57/#83 documentation updates

### 0.2.14 - 2012-10-19

 * #77 ability to pause a channel (includes bugfix for message pump/diskqueue)
 * #74 propagate all topic changes to lookupd
 * #65 create binary releases

### 0.2.13 - 2012-10-15

 * #70 deadlined nsq_to_http outbound requests
 * #69/#72 improved nsq_to_file sync strategy
 * #58 diskqueue metadata bug and refactoring

### 0.2.12 - 2012-10-10

 * #63 consolidated protocol V1 into V2 and fixed PUB bug
 * #61 added a makefile for simpler building
 * #55 allow topic/channel names with `.`
 * combined versions for all binaries

### 0.2.7 - 0.2.11

 * Initial public release.

## Go Client Library

### 0.3.2-alpha

 * #204 - fix early termination blocking
 * #186 - max backoff duration of 0 disables backoff
 * #164/#202 - add Writer
 * #175 - support server side configurable max RDY count
 * #177 - support broadcast_address
 * #161 - connection pool goroutine safety

### 0.3.1 - 2013-02-07

**Upgrading from 0.3.0**: This release requires NSQ binary version `0.2.17+` for `TOUCH` support.

 * #119 - add TOUCH command
 * #133 - improved handling of errors/magic
 * #127 - send IDENTIFY (missed in #90)
 * #16 - add backoff to Reader

### 0.3.0 - 2013-01-07

**Upgrading from 0.2.4**: There are no backward incompatible changes to applications
written against the public `nsq.Reader` API.

However, there *are* a few backward incompatible changes to the API for applications that 
directly use other public methods, or properties of a few NSQ data types:

`nsq.Message` IDs are now a type `nsq.MessageID` (a `[16]byte` array).  The signatures of
`nsq.Finish()` and `nsq.Requeue()` reflect this change.

`nsq.SendCommand()` and `nsq.Frame()` were removed in favor of `nsq.SendFramedResponse()`.

`nsq.Subscribe()` no longer accepts `shortId` and `longId`.  If upgrading your consumers
before upgrading your `nsqd` binaries to `0.2.16-rc.1` they will not be able to send the 
optional custom identifiers.
    
 * #90 performance optimizations
 * #81 reader performance improvements / MPUB support

### 0.2.4 - 2012-10-15

 * #69 added IsStarved() to reader API

### 0.2.3 - 2012-10-11

 * #64 timeouts on reader queries to lookupd
 * #54 fix crash issue with reader cleaning up from unexpectedly closed nsqd connections

### 0.2.2 - 2012-10-09

 * Initial public release

## pynsq Python Client Library

 * #88 moved **pynsq** to its own [repository](https://github.com/bitly/pynsq).
