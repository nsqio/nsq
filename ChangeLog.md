# NSQ Changelog

## Binaries

### 0.2.17-alpha

 * #118 - nsqadmin POST actions/confirmations
 * #106 - nsqlookupd topic producer tombstones
 * #100 - sane handling of diskqueue read errors
 * #117 - nsqadmin admin action POST notifications
 * #122 - configurable msg size limits
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

### 0.3.1-alpha

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
