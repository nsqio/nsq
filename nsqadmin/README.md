nsqadmin
========

`nsqadmin` is the Web UI to view message statistics and to perform administrative tasks like removing a channel.

Command Line Options
--------------------

    Usage of ./nsqadmin:
      -graphite-url="": URL to graphite HTTP address
      -http-address="0.0.0.0:4171": <addr>:<port> to listen on for HTTP clients
      -lookupd-http-address=[]: lookupd HTTP address (may be given multiple times)
      -nsqd-http-address=[]: nsqd HTTP address (may be given multiple times)
      -proxy-graphite=true: Proxy HTTP requests to graphite
      -template-dir="templates": path to templates directory
      -use-statsd-prefixes=true: expect statsd prefixed keys in graphite (ie: 'stats_counts.')
      -version=false: print version string

### Statsd / Graphite Integration

When using `nsqd --statsd-address=...` you can specify a `nsqadmin --graphite-url=http://graphite.yourdomain.com` 
to enable graphite charts in nsqadmin. If using a statsd clone (like [gographite](https://github.com/bitly/gographite)) 
that does not prefix keys, also specify `--use-statsd-prefix=false`.