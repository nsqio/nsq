# NSQ Changelog

## NSQ Binaries

* 0.2.16 - Alpha
    * #95 proxy graphite requests through nsqadmin
    * #93 fix nqd API response for no topics
    * #92 graph rendering options
    * #87 fix MPUB
    * #89 gopkg doc updates
    * #88 move pynsq to it's own repo
    * #81 reader improvements / introduced MPUB. Fix bug for mem-queue-size < 10
    * #76 statsd/graphite support
    * #75 administrative ability to create topics and channels
* 0.2.15 - 2012-10-25
    * #84 fix lookupd hanging on to ephemeral channels w/ no producers
    * #82 add /counter page to nsqadmin
    * #80 message size benchmark
    * #78 send Content-Length for nsq_to_http requests
    * #57/#83 documentation updates
* 0.2.14 - 2012-10-19
    * #77 ability to pause a channel (includes bugfix for message pump/diskqueue)
    * #74 propagate all topic changes to lookupd
    * #65 create binary releases
* 0.2.13 - 2012-10-15
    * #70 deadlined nsq_to_http outbound requests
    * #69/#72 improved nsq_to_file sync strategy
    * #58 diskqueue metadata bug and refactoring
* 0.2.12 - 2012-10-10
    * #63 consolidated protocol V1 into V2 and fixed PUB bug
    * #61 added a makefile for simpler building
    * #55 allow topic/channel names with `.`
    * combined versions for all binaries
* 0.2.7 - 0.2.11
    * Initial public release.

## NSQ Go Client Library

* 0.2.5 - Alpha
    * #81 reader performance improvements / MPUB support
* 0.2.4 - 2012-10-15
    * #69 added IsStarved() to reader API
* 0.2.3 - 2012-10-11
    * #64 timeouts on reader queries to lookupd
    * #54 fix crash issue with reader cleaning up from unexpectedly closed nsqd connections
* 0.2.2 - 2012-10-09
    * Initial public release

## pynsq Python Client Library

* 0.3 - 2012-10-25
    * #79 heartbeat checks, lookupd jitter, removal of automatic JSON, refactoring for subclassing
* 0.2 - 2012-10-09
    * Initial public release
