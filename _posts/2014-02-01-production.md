--- 
title: Production Configuration
layout: post
category: deployment
permalink: /deployment/production.html
---

Although `nsqd` can run as a single standalone node these notes assume you want to take advantage
of the distributed nature of its design.

There are three separate binaries that need to be installed and running:

### nsqd

`nsqd` is the daemon that receives, buffers, and delivers messages to clients.

All configuration is managed via command line parameters. We strive for the default configuration to
be adequate for most use cases however a few specific options are worth noting:

`--mem-queue-size` adjusts the number of messages queued in memory *per* topic/channel. Messages
over that watermark are transparently written to disk, defined by `--data-path`.

Also, `nsqd` will need to be configured with `nsqlookupd` addresses (see below for details). Specify
`--lookupd-tcp-address` options for each instance.

In terms of topology, we recommend running `nsqd` co-located with services producing messages.

`nsqd` can be configured to push data to [statsd][statsd] by specifying `--statsd-address`. `nsqd`
sends stats under the `nsq.*` namespace. See [nsqd statsd][nsqd_statsd].

### nsqlookupd

`nsqlookupd` is the daemon that provides a runtime discovery service for consumers to find `nsqd`
producers for a specific topic.

It maintains no persistent state and does not need to coordinate with any other `nsqlookupd` 
instances to satisfy a query.

Run as many as you want based on your redundancy requirements. They use few resources and can be
co-located with other services. Our recommendation is to run a cluster of at least 3 per datacenter.

### nsqadmin

`nsqadmin` is a web service for realtime instrumentation and administration of your NSQ cluster. It
talks to `nsqlookupd` instances to locate producers and leverages [graphite][graphite] for charts
(requires enabling `statsd` on the `nsqd` side).

You only need to run 1 of these and make it accessible publicly (securely).

There are a few HTML template files that need to be deployed somewhere.  By default `nsqadmin`
will look in `/usr/local/share/nsqadmin/templates` but can be overridden via `--template-dir`.

To display charts from `graphite`, specify `--graphite-url`.  If you're using a version of `statsd`
that adds those silly prefixes to all keys, also specify `--use-statsd-prefixes`.  Finally, if 
graphite isn't accessible publicly you can have `nsqadmin` proxy those requests by specifying
`--proxy-graphite`.

### Monitoring

Each of the daemons has a `/ping` HTTP endpoint that can be used to create a Nagios check.

For realtime debugging this also works surprisingly well:

    $ watch -n 0.5 "curl -s http://127.0.0.1:4151/stats"

Typically most debugging, analysis, and administration is done via `nsqadmin`.

[statsd]: https://github.com/bitly/statsdaemon
[graphite]: http://graphite.wikidot.com/
[nsqd_statsd]: {{ site.baseurl }}/components/nsqd.html#statsd
