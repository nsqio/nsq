# NSQ Changelog

## Binaries

### 0.3.8 - 2016-05-26

**Upgrading from 0.3.7**: Binaries contain no backwards incompatible changes.

This release fixes a critical regression in `0.3.7` that could result in message loss when
attempting to cleanly shutdown `nsqd` by sending it a `SIGTERM`. The expected behavior was for it
to flush messages in internal buffers to disk before exiting. See #757 and #759 for more details.

A few performance improvements landed including #743, which improves channel throughput by ~17%,
and #740, which reduces garbage when reading messages from disk.

We're now stripping debug info, reducing binary size, in the official binary downloads and Windows
binaries are now bundled with the appropriate `.exe` extension (#726 and #751).

Features:

 * #743 - `nsqd`: remove channel `messagePump`
 * #751 - strip debug info from binaries (thanks @ploxiln)
 * #740 - `nsqd`: reduce garbage when reading from diskqueue (thanks @dieterbe)

Bugs:

 * #757/#759 - `nsqd`: properly handle `SIGTERM` (thanks @judwhite)
 * #738 - updates for latest `go-options`
 * #730 - `nsqd`: diskqueue sync count on both read/write
 * #734 - `nsqadmin`: make `rate` column work without `--proxy-graphite` (thanks @ploxiln)
 * #726 - add `.exe` extension to Windows binaries (thanks @ploxiln)
 * #722 - `nsqadmin`: fix connected duration > `1hr`

### 0.3.7 - 2016-02-23

**Upgrading from 0.3.6**: Binaries contain no backwards incompatible changes.

This release has been built with Go 1.6.

Highlights include the various work done to reduce `nsqd` lock contention, significantly improving
the impact of high load on the `/stats` endpoint, addressing issues with timeouts and failures
in `nsqadmin` (#700, #701, #703, #709).

Thanks to @judwhite, `nsqd` and `nsqlookupd` now natively support being run as a Windows service
(#718). We're also now publishing official Windows releases.

`nsqd` will now `flock` its data directory on linux, preventing two `nsqd` from running
simultaneously pointed at the same path (#583).

On the bugfix side, the most noteworthy change is that `nsqd` will now correctly reset health state
on a successful backend write (#671).

Features:

 * #700/#701/#703/#709 - `nsqd`: reduce lock contention (thanks @zachbadgett @absolute8511)
 * #718 - `nsqd`/`nsqlookupd`: support running as a windows service (thanks @judwhite)
 * #706 - `nsqd`: support enabling/disabling block profile via HTTP (thanks @absolute8511)
 * #710 - `nsqd`: support `POST` `/debug/pprof/symbol` (thanks @absolute8511)
 * #662 - `nsqadmin`: add flags for formatting statsd keys (thanks @kesutton)
 * #583 - `nsqd`: `flock` `--data-path` on linux
 * #663 - `nsqd`: optimize GUID generation (thanks @ploxiln)

Bugs:

 * #672 - `nsqd`: fix max size accounting in `diskqueue` (thanks @judwhite)
 * #671 - `nsqd`: reset health on successful backend write (thanks @judwhite)
 * #615 - `nsqd`: prevent OOM when reading from `nsqlookupd` peer
 * #664/#666 - dist.sh/Makefile cleanup (thanks @ploxiln)

### 0.3.6 - 2015-09-24

**Upgrading from 0.3.5**: Binaries contain no backwards incompatible changes.

We've adopted the [Contributor Covenant 1.2 Code of Conduct](CODE_OF_CONDUCT.md) (#593). Help us
keep NSQ open and inclusive by reading and following this document.

We closed a few longstanding issues related to `nsqadmin`, namely (#323, et al.) converting it to
an API and single-page app (so that it is _much_ easier to develop), displaying fine-grained errors
(#421, #657), and enabling support for `--tls-required` configurations (#396).

For `nsqd`, we added support for deferred publishing aka `DPUB` (#293), which allows a producer to
specify a duration of time to delay initial delivery of the message. We also addressed performance
issues relating to large numbers of topics/channels (#577) by removing some per-channel goroutines
in favor of a centralized, periodic, garbage collection approach.

In order to provide more flexibility when deploying NSQ in dynamically orchestrated topologies,
`nsqd` now supports the ability to configure `nsqlookupd` peers at runtime via HTTP (#601),
eliminating the need to restart the daemon.

As part of the large `nsqadmin` refactoring, we took the opportunity to cleanup the internals for
_all_ of the daemon's HTTP code paths (#601, #610, #612, #641) as well as improving the test suite
so that it doesn't leave around temporary files (#553).

Features:

 * #593 - add code of conduct
 * #323/#631/#632/#642/#421/#649/#650/#651/#652/#654 - `nsqadmin`: convert to API / single-page app
 * #653 - `nsqadmin`: expand notification context
 * #293 - `nsqd`: add deferred pub (`DPUB`)
 * #577 - `nsqd`: drop per-channel queue workers in favor of centralized queue GC
 * #584 - `nsqlookupd`: improve registration DB performance (thanks @xiaost)
 * #601 - `nsqd`: HTTP endpoints to dynamically configure `nsqlookupd` peers
 * #608 - `nsqd`: support for filtering `/stats` to topic/channel (thanks @chrusty)
 * #601/#610/#612/#641 - improved HTTP internal routing / log HTTP requests
 * #628 - `nsqd`: clarify help text for `--e2e-processing-latency-percentile`
 * #640 - switch `--{consumer,producer}-opt` to `nsq.ConfigFlag`

Bugs:

 * #656 - `nsqadmin`: update `statsd` prefix to `stats.counters`
 * #421/#657 - `nsqadmin`: display upstream/partial errors
 * #396 - `nsqdamin`/`nsqd`: support for `--tls-required`
 * #558 - don't overwrite docker root FS
 * #582 - `nsqd`: ignore benign EOF errors
 * #587 - `nsqd`: GUID error handling / catch errors if GUID goes backwards (thanks @mpe)
 * #586 - `nsqd`: fix valid range for `--worker-id`
 * #550/#602/#617/#618/#619/#620/#622 - `nsqd`: fix benchmarks (thanks @Dieterbe)
 * #553 - cleanup test dirs
 * #600 - `nsqd`: enforce diskqueue min/max message size (thanks @twmb)

### 0.3.5 - 2015-04-26

**Upgrading from 0.3.3**: Binaries contain no backwards incompatible changes.

This is another quick bug fix release to address the broken `nsqadmin` binary in the distribution
(see #578).

### 0.3.4 - 2015-04-26

**WARNING**: please upgrade to `v0.3.5` to address the broken `nsqadmin` binary.

**Upgrading from 0.3.3**: Binaries contain no backwards incompatible changes.

This is a quick bug fix release to fix the outdated `go-nsq` dependency in `v0.3.3`
for the bundled utilities (see 6e8504e).

### 0.3.3 - 2015-04-26

**WARNING**: please upgrade to `v0.3.5` to address the outdated `go-nsq` dependency for the
bundled utilities and the broken `nsqadmin` binary.

**Upgrading from 0.3.2**: Binaries contain no backwards incompatible changes.

This release is primarily a bug fix release after cleaning up and reorganizing the codebase.
`nsqadmin` is now importable, which paves the way for completing #323. The bundled utilities
received a few feature additions and bug fixes (mostly from bug fixes on the `go-nsq` side).

Features:

 * #569 - `nsqadmin`: re-org into importable package
 * #562 - `nsq_to_{nsq,http}`: add `epsilon-greedy` mode (thanks @twmb)
 * #547 - `nsqd`: adds `start_time` to `/stats` (thanks @ShawnSpooner)
 * #544 - `nsq_to_http`: accept any `200` response as success (thanks @mikedewar)
 * #548 - `nsq_to_http`: read entire request body (thanks @imgix)
 * #552/#554/#555/#556/#561 - code cleanup and `/internal` package re-org (thanks @cespare)

Bugs:

 * #573 - `nsqd`: don't persist metadata upon startup (thanks @xiaost)
 * #560 - `nsqd`: do not print `EOF` error when client closes cleanly (thanks @twmb)
 * #557 - `nsqd`: fix `--tls-required=tcp-https` with `--tls-client-auth-policy` (thanks @twmb)
 * #545 - enable shell expansion in official Docker image (thanks @paddyforan)

NOTE: the bundled utilities are built against [`go-nsq` `v1.0.4`][go-nsq_104] and include all of
those features/fixes.

[go-nsq_104]: https://github.com/nsqio/go-nsq/releases/tag/v1.0.4

### 0.3.2 - 2015-02-08

**Upgrading from 0.3.1**: Binaries contain no backwards incompatible changes however as of this
release we've updated our official Docker images.

We now provide a single Docker image [`nsqio/nsq`](https://registry.hub.docker.com/u/nsqio/nsq/)
that includes *all* of the NSQ binaries. We did this for several reasons, primarily because the
tagged versions in the previous incarnation were broken (and did not actually pin to a version!).
The new image is an order of magnitude smaller, weighing in around 70mb.

In addition, the impetus for this quick release is to address a slew of reconnect related bug fixes
in the utility apps (`nsq_to_nsq`, `nsq_to_file`, etc.), for details see the [`go-nsq` `v1.0.3`
release notes](https://github.com/nsqio/go-nsq/releases/tag/v1.0.3).

Features:

 * #534/#539/#540 - single Dockerfile approach (thanks @paddyforan)

Bugs:

 * #529 - nsqadmin: fix more `#ephemeral` topic deletion issues
 * #530 - nsqd: fix the provided sample config file (thanks @jnewmano)
 * #538 - nsqd: fix orphaned ephemeral channels (thanks @adamsathailo)

### 0.3.1 - 2015-01-21

**Upgrading from 0.3.0**: No backwards incompatible changes.

This release contains minor bug fixes and feature additions.

There are a number of functionality improvements to the `nsq_stat` and `nsq_to_file` helper
applications (and general support for `#ephemeral` topics, broken in `0.2.30`).

Additionally, the TLS options continue to improve with support for setting `--tls-min-version` and
a work-around for a bug relating to `TLS_FALLBACK_SCSV` ([to be fixed in Go
1.5](https://go-review.googlesource.com/#/c/1776/)).

Features:

 * #527 - nsq_stat: deprecate `--status-every` in favor of `--interval`
 * #524 - nsq_stat: add `--count` option (thanks @nordicdyno)
 * #518 - nsqd: set defaults for `--tls-min-version` and set TLS max version to 1.2
 * #475/#513/#518 - nsqd: `--tls-required` can be disabled for HTTP / add `--tls-min-version`
                    (thanks @twmb)
 * #496 - nsq_to_file: add `<PID>` to filename and rotation by size/interval (thanks @xiaost)
 * #507 - nsq_stat: add rates (thanks @xiaost)
 * #505 - nsqd: speed up failure path of `BytesToBase10` (thanks @iand)

Bugs:

 * #522 - nsqadmin: fix `#ephemeral` topic deletion issues
 * #509 - nsqd: fix `diskqueue` atomic rename on Windows (thanks @allgeek)
 * #479 - nsqd: return `output_buffer_*` resolved settings in `IDENTIFY` response (thanks @tj)

### 0.3.0 - 2014-11-18

**Upgrading from 0.2.31**: No backwards incompatible changes.

This release includes a slew of bug fixes and few key feature additions.

The biggest functional change is that `nsqd` no longer decrements its `RDY` count for clients. This
means that client libraries no longer have to periodically re-send `RDY`. For some context, `nsqd`
already provided back-pressure due to the fact that a client must respond to messages before
receiving new ones. The decremented `RDY` count only made the implementation of the server and
client more complex without additional benefit. Now the `RDY` command can be treated as an "on/off"
switch. For details see #404 and the associated changes in nsqio/go-nsq#83 and nsqio/pynsq#98.

The second biggest change (and oft-requested feature!) is `#ephemeral` topics. Their behavior
mirrors that of channels. This feature is incredibly useful for situations where you're using
topics to "route" messages to consumers (like RPC) or when a backlog of messages is undesirable.

There are now scripts in the `bench` directory that automate the process of running a distributed
benchmark.  This is a work-in-progress, but it already provides a closer-to-production setup and
therefore more accurate results.  There's much work to do here!

A whole bunch of bugs were fixed - notably all were 3rd-party contributions! Thanks!

 * #305 - `#ephemeral` topics
 * #404/#459 - don't decr `RDY` / send `RDY` before `FIN`/`REQ`
 * #472 - improve `nsqd` `diskqueue` sync strategies
 * #488 - ability to filter topics by regex in `nsq_to_file` (thanks @lxfontes)
 * #438 - distributed pub-sub benchmark scripts
 * #448 - better `nsqd` `IOLoop` logging (thanks @rexposadas)
 * #458 - switch to [gpm](https://github.com/pote/gpm) for builds

Bugs:

 * #493 - ensure all `nsqd` `Notify()` goroutines have exited prior to shutdown (thanks @allgeek)
 * #492 - ensure `diskqueue` syncs at end of benchmarks (thanks @Dieterbe)
 * #490 - de-flake `TestPauseMetadata` (thanks @allgeek)
 * #486 - require ports to be specified for daemons (thanks @jnewmano)
 * #482 - better bash in `dist.sh` (thanks @losinggeneration)
 * #480 - fix panic when `nsqadmin` checks stats for missing topic (thanks @jnewmano)
 * #469 - fix panic when misbehaving client sends corrupt command (thanks @prio)
 * #461 - fix panic when `nsqd` decodes corrupt message data (thanks @twmb)
 * #454/#455 - fix 32-bit atomic ops in `nsq_to_nsq`/`nsq_to_http` (thanks @leshik)
 * #451 - fix `go get` compatibility (thanks @adams-sarah)

### 0.2.31 - 2014-08-26

**Upgrading from 0.2.30**: No backwards incompatible changes.

This release includes a few key changes. First, we improved feedback and back-pressure when `nsqd`
writes to disk. Previously this was asynchronous and would result in clients not knowing that their
`PUB` had failed. Interestingly, this refactoring improved performance of `PUB` by 41%, by removing
the topic's goroutine responsible for message routing in favor of `N:N` Go channel communication.
For details see #437.

@paddyforan contributed official Dockerfiles that are now built automatically via Docker Hub.
Please begin to use (and improve these) as the various older images we had been maintaining will be
deprecated.

The utility apps deprecated the `--reader-opt` flag in favor of `--consumer-opt` and `nsq_to_nsq`
and `to_nsq` received a `--producer-opt` flag, for configuring details of the connection publishing
to `nsqd`. Additionally, it is now possible to configure client side TLS certificates via
`tls_cert` and `tls_key` opts.

As usual, we fixed a few minor bugs, see below for details.

New Features / Enhancements:

 * #422/#437 - `nsqd`: diskqueue error feedback/backpressure (thanks @boyand)
 * #412 - official Dockerfiles for `nsqd`, `nsqlookupd`, `nsqadmin` (thanks @paddyforan)
 * #442 - utilities: add `--consumer-opt` alias for `--reader-opt` and
          add `--producer-opt` to `nsq_to_nsq` (also support configuration
          of `tls_cert` and `tls_key`)
 * #448 - `nsqd`: improve IOLoop error messages (thanks @rexposadas)

Bugs:

 * #440 - `nsqd`: fixed statsd GC stats reporting (thanks @jphines)
 * #434/#435 - refactored/stabilized tests and logging
 * #429 - `nsqd`: improve handling/documentation of `--worker-id` (thanks @bschwartz)
 * #428 - `nsqd`: `IDENTIFY` should respond with materialized `msg_timeout` (thanks @visionmedia)

### 0.2.30 - 2014-07-28

**Upgrading from 0.2.29**: No backwards incompatible changes.

**IMPORTANT**: this is a quick bug-fix release to address a panic in `nsq_to_nsq` and
`nsq_to_http`, see #425.

New Features / Enhancements:

 * #417 - `nsqadmin`/`nsqd`: expose TLS connection state
 * #425 - `nsq_to_nsq`/`nsq_to_file`: display per-destination-address timings

Bugs:

 * #425 - `nsq_to_nsq`/`nsq_to_file`: fix shared mutable state panic

### 0.2.29 - 2014-07-25

**Upgrading from 0.2.28**: No backwards incompatible changes.

This release includes a slew of new features and bug fixes, with contributions from 8
members of the community, thanks!

The most important new feature is authentication (the `AUTH` command for `nsqd`), added in #356.
When `nsqd` is configured with an `--auth-http-address` it will require clients to send the `AUTH`
command. The `AUTH` command body is opaque to `nsqd`, it simply passes it along to the configured
auth daemon which responds with well formed JSON, indicating which topics/channels and properties
on those entities are accessible to that client (rejecting the client if it accesses anything
prohibited). For more details, see [the spec](http://nsq.io/clients/tcp_protocol_spec.html) or [the
`nsqd` guide](http://nsq.io/components/nsqd.html#auth).

Additionally, we've improved performance in a few areas. First, we refactored in-flight handling in
`nsqd` to reduce garbage creation and improve baseline performance 6%. End-to-end processing
latency calculations are also significantly faster, thanks to improvements in the
[`perks`](https://github.com/bmizerany/perks/pulls/7) package.

HTTP response formats have been improved (removing the redundant response wrapper) and cleaning up
some of the endpoint namespaces. This change is backwards compatible. Clients wishing to move
towards the new response format can either use the new endpoint names or send the following header:

    Accept: application/vnd.nsq version=1.0

Other changes including officially bumping the character limit for topic and channel names to 64
(thanks @svmehta), making the `REQ` timeout limit configurable in `nsqd` (thanks @AlphaB), and
compiling static asset dependencies into `nsqadmin` to simplify deployment (thanks @crossjam).

Finally, `to_nsq` was added to the suite of bundled apps. It takes a stdin stream and publishes to
`nsqd`, an extremely flexible solution (thanks @matryer)!

As for bugs, they're mostly minor, see the pull requests referenced in the section below for
details.

New Features / Enhancements:

 * #304 - apps: added `to_nsq` for piping stdin to NSQ (thanks @matryer)
 * #406 - `nsqadmin`: embed external static asset dependencies (thanks @crossjam)
 * #389 - apps: report app name and version via `user_agent`
 * #378/#390 - `nsqd`: improve in-flight message handling (6% faster, GC reduction)
 * #356/#370/#386 - `nsqd`: introduce `AUTH`
 * #358 - increase topic/channel name max length to 64 (thanks @svmehta)
 * #357 - remove internal `go-nsq` dependencies (GC reduction)
 * #330/#366 - version HTTP endpoints, simplify response format
 * #352 - `nsqd`: make `REQ` timeout limit configurable (thanks @AlphaB)
 * #340 - `nsqd`: bump perks dependency (E2E performance improvement, see 25086e4)

Bugs:

 * #384 - `nsqd`: fix statsd GC time reporting
 * #407 - `nsqd`: fix double `TOUCH` and use of client's configured msg timeout
 * #392 - `nsqadmin`: fix HTTPS warning (thanks @juliangruber)
 * #383 - `nsqlookupd`: fix race on last update timestamp
 * #385 - `nsqd`: properly handle empty `FIN`
 * #365 - `nsqd`: fix `IDENTIFY` `msg_timeout` response (thanks @visionmedia)
 * #345 - `nsq_to_file`: set proper permissions on new directories (thanks @bschwartz)
 * #338 - `nsqd`: fix windows diskqueue filenames (thanks @politician)

### 0.2.28 - 2014-04-28

**Upgrading from 0.2.27**: No backwards incompatible changes.  We've deprecated the `short_id`
and `long_id` options in the `IDENTIFY` command in favor of `client_id` and `hostname`, which
more accurately reflect the data typically used.

This release includes a few important new features, in particular enhanced `nsqd`
TLS support thanks to a big contribution by @chrisroberts.

You can now *require* that clients negotiate TLS with `--tls-required` and you can configure a
client certificate policy via `--tls-client-auth-policy` (`require` or `require-verify`):

 * `require` - the client must offer a certificate, otherwise rejected
 * `require-verify` - the client must offer a valid certificate according to the default CA or
                      the chain specified by `--tls-root-ca-file`, otherwise rejected

This can be used as a form of client authentication.

Additionally, `nsqd` is now structured such that it is importable in other Go applications
via `github.com/nsqio/nsq/nsqd`, thanks to @kzvezdarov.

Finally, thanks to @paddyforan, `nsq_to_file` can now archive *multiple* topics or
optionally archive *all* discovered topics (by specifying no `--topic` params
and using `--lookupd-http-address`).

New Features / Enhancements:

 * #334 - `nsq_to_file` can archive many topics (thanks @paddyforan)
 * #327 - add `nsqd` TLS client certificate verification policy, ability
          to require TLS, and HTTPS support (thanks @chrisroberts)
 * #325 - make `nsqd` importable (`github.com/nsqio/nsq/nsqd`) (thanks @kzvezdarov)
 * #321 - improve `IDENTIFY` options (replace `short_id` and `long_id` with
          `client_id` and `hostname`)
 * #319 - allow path separator in `nsq_to_file` filenames (thanks @jsocol)
 * #324 - display memory depth and total depth in `nsq_stat`

Bug Fixes:

 * nsqio/go-nsq#19 and nsqio/go-nsq#29 - fix deadlocks on `nsq.Reader` connection close/exit, this
                                         impacts the utilities packaged with the NSQ binary
                                         distribution such as `nsq_to_file`, `nsq_to_http`,
                                         `nsq_to_nsq` and `nsq_tail`.
 * #329 - use heartbeat interval for write deadline
 * #321/#326 - improve benchmarking tests
 * #315/#318 - fix test data races / flakiness

### 0.2.27 - 2014-02-17

**Upgrading from 0.2.26**: No backwards incompatible changes.  We deprecated `--max-message-size`
in favor of `--max-msg-size` for consistency with the rest of the flag names.

IMPORTANT: this is another quick bug-fix release to address an issue in `nsqadmin` where templates
were incompatible with older versions of Go (pre-1.2).

 * #306 - fix `nsqadmin` template compatibility (and formatting)
 * #310 - fix `nsqadmin` behavior when E2E stats are disabled
 * #309 - fix `nsqadmin` `INVALID_ERROR` on node page tombstone link
 * #311/#312 - fix `nsqd` client metadata race condition and test flakiness
 * #314 - fix `nsqd` test races (run w/ `-race` and `GOMAXPROCS=4`) deprecate `--max-message-size`

### 0.2.26 - 2014-02-06

**Upgrading from 0.2.25**: No backwards incompatible changes.

IMPORTANT: this is a quick bug-fix release to address a regression identified in `0.2.25` where
`statsd` prefixes were broken when using the default (or any) prefix that contained a `%s` for
automatic host replacement.

 * #303 - fix `nsqd` `--statsd-prefix` when using `%s` host replacement

### 0.2.25 - 2014-02-05

**Upgrading from 0.2.24**: No backwards incompatible changes.

This release adds several commonly requested features.

First, thanks to [@elubow](https://twitter.com/elubow) you can now configure your clients to sample
the stream they're subscribed to. To read more about the details of the implementation see #286 and
the original discussion in #223.  Eric also contributed an improvement to `nsq_tail` to add
the ability to tail the last `N` messages and exit.

We added config file support ([TOML](https://github.com/mojombo/toml/blob/master/README.md)) for
`nsqd`, `nsqlookupd`, and `nsqadmin` - providing even more deployment flexibility. Example configs
are in the `contrib` directory. Command line arguments override the equivalent option in the config
file.

We added the ability to pause a *topic* (it is already possible to pause individual *channels*).
This functionality stops all message flow from topic to channel for *all channels* of a topic,
queueing at the topic level. This enables all kinds of interesting possibilities like atomic
channel renames and trivial infrastructure wide operations.

Finally, we now compile the static assets used by `nsqadmin` into the binary, simplifying
deployment.  This means that `--template-dir` is now deprecated and will be removed in a future
release and you can remove the templates you previously deployed and maintained.

New Features / Enhancements:

 * #286 - add client `IDENTIFY` option to sample a % of messages
 * #279 - add TOML config file support to `nsqd`, `nsqlookupd`, and `nsqadmin`
 * #263 - add ability to pause a topic
 * #291 - compile templates into `nsqadmin` binary
 * #285/#288 - `nsq_tail` support for `-n #` to get recent # messages
 * #287/#294 - display client `IDENTIFY` attributes in `nsqadmin` (sample rate, TLS, compression)
 * #189/#296 - add client user agent to `nsqadmin``
 * #297 - add `nsq_to_nsq` JSON message filtering options

### 0.2.24 - 2013-12-07

**Upgrading from 0.2.23**: No backwards incompatible changes. However, as you'll see below, quite a
few command line flags to the utility apps (`nsq_to_http`, `nsq_to_file`, `nsq_to_http`) were
deprecated and will be removed in the next release. Please use this release to transition over to
the new ones.

NOTE: we are now publishing additional binaries built against go1.2

The most prominent addition is the tracking of end-to-end message processing percentiles. This
measures the amount of time it's taking from `PUB` to `FIN` per topic/channel. The percentiles are
configurable and, because there is *some* overhead in collecting this data, it can be turned off
entirely. Please see [the section in the docs](http://nsq.io/components/nsqd.html) for
implementation details.

Additionally, the utility apps received comprehensive support for all configurable reader options
(including compression, which was previously missing). This necessitated a bit of command line flag
cleanup, as follows:

#### nsq_to_file

 * deprecated `--gzip-compression` in favor of `--gzip-level`
 * deprecated `--verbose` in favor of `--reader-opt=verbose`

#### nsq_to_http

 * deprecated `--throttle-fraction` in favor of `--sample`
 * deprecated `--http-timeout-ms` in favor of `--http-timeout` (which is a
   *duration* flag)
 * deprecated `--verbose` in favor of `--reader-opt=verbose`
 * deprecated `--max-backoff-duration` in favor of
   `--reader-opt=max_backoff_duration=X`

#### nsq_to_nsq

 * deprecated `--verbose` in favor of `--reader-opt=verbose`
 * deprecated `--max-backoff-duration` in favor of
   `--reader-opt=max_backoff_duration=X`

New Features / Enhancements:

 * #280 - add end-to-end message processing latency metrics
 * #267 - comprehensive reader command line flags for utilities

### 0.2.23 - 2013-10-21

**Upgrading from 0.2.22**: No backwards incompatible changes.

We now use [godep](https://github.com/kr/godep) in order to achieve reproducible builds with pinned
dependencies.  If you're on go1.1+ you can now just use `godep get github.com/nsqio/nsq/...`.

This release includes `nsqd` protocol compression feature negotiation.
[Snappy](https://code.google.com/p/snappy/) and [Deflate](http://en.wikipedia.org/wiki/DEFLATE) are
supported, clients can choose their preferred format.

`--statsd-prefix` can now be used to modify the prefix for the `statsd` keys generated by `nsqd`.
This is useful if you want to add datacenter prefixes or remove the default host prefix.

Finally, this release includes a "bug" fix that reduces CPU usage for `nsqd` with many clients by
choosing a more reasonable default for a timer used in client output buffering.  For more details
see #236.

New Features / Enhancements:

 * #266 - use godep for reproducible builds
 * #229 - compression (Snappy/Deflate) feature negotiation
 * #241 - binary support for HTTP /mput
 * #269 - add --statsd-prefix flag

Bug Fixes:

 * #278 - fix nsqd race for client subscription cleanup (thanks @simplereach)
 * #277 - fix nsqadmin counter page
 * #275 - stop accessing simplejson internals
 * #274 - nsqd channel pause state lost during unclean restart (thanks @hailocab)
 * #236 - reduce "idle" CPU usage by 90% with large # of clients

### 0.2.22 - 2013-08-26

**Upgrading from 0.2.21**: message timestamps are now officially nanoseconds.  The protocol docs
always stated this however `nsqd` was actually sending seconds.  This may cause some compatibility
issues for client libraries/clients that were taking advantage of this field.

This release also introduces support for TLS feature negotiation in `nsqd`.  Clients can optionally
enable TLS by using the appropriate handshake via the `IDENTIFY` command. See #227.

Significant improvements were made to the HTTP publish endpoints and in flight message handling to
reduce GC pressure and eliminate memory abuse vectors. See #242, #239, and #245.

This release also includes a new utility `nsq_to_nsq` for performant, low-latency, copying of an NSQ
topic over the TCP protocol.

Finally, a whole suite of debug HTTP endpoints were added (and consolidated) under the
`/debug/pprof` namespace. See #238, #248, and #252. As a result `nsqd` now supports *direct*
profiling via Go's `pprof` tool, ie:

    $ go tool pprof --web http://ip.address:4151/debug/pprof/heap

New Features / Enhancements:

 * #227 - TLS feature negotiation
 * #238/#248/#252 - support for more HTTP debug endpoints
 * #256 - `nsqadmin` single node view (with GC/mem graphs)
 * #255 - `nsq_to_nsq` utility for copying a topic over TCP
 * #230 - `nsq_to_http` takes `--content-type` flag (thanks @michaelhood)
 * #228 - `nsqadmin` displays tombstoned topics in the `/nodes` list
 * #242/#239/#245 - reduced GC pressure for inflight and `/mput`

Bug Fixes:

 * #260 - `tombstone_topic_producer` action in `nsqadmin` missing node info
 * #244 - fix 64bit atomic alignment issues on 32bit platforms
 * #251 - respect configured limits for HTTP publishing
 * #247 - publish methods should not allow 0 length messages
 * #231/#259 - persist `nsqd` metadata on topic/channel changes
 * #237 - fix potential memory leaks with retained channel references
 * #232 - message timestamps are now nano
 * #228 - `nsqlookupd`/`nsqadmin` would display inactive nodes in `/nodes` list
 * #216 - fix edge cases in `nsq_to_file` that caused empty files

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

## go-nsq Client Library

 * #264 moved **go-nsq** to its own [repository](https://github.com/nsqio/go-nsq)

## pynsq Python Client Library

 * #88 moved **pynsq** to its own [repository](https://github.com/nsqio/pynsq)
