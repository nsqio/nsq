## nsqd

`nsqd` is the daemon that receives, buffers, and delivers messages to clients.

It is normally run alongside `nsqlookupd` instances to announce topic and channels but can be run
standalone.

It listens on two TCP ports, one for clients and another for the HTTP API.

### HTTP API

* `/put?topic=...`

    POST message body
    
    `$ curl -d "<message>" http://127.0.0.1:4151/put?topic=message_topic`

* `/mput?topic=...`

    POST message body (`\n` separated)
    
    `$ curl -d "<message>\n<message>" http://127.0.0.1:4151/put?topic=message_topic`

* `/empty_channel?topic=...&channel=...`
* `/delete_channel?topic=...&channel=...`
* `/pause_channel?topic=...&channel=...`
* `/unpause_channel?topic=...&channel=...`
* `/create_topic?topic=...`
* `/create_channel?topic=...&channel=...`
* `/stats`

    supports both text and JSON via `?format=json`

* `/ping`

    returns `OK`, helpful when monitoring

* `/info`

    returns version information

### Command Line Options

    -data-path="": path to store disk-backed messages
    -http-address="0.0.0.0:4151": <addr>:<port> to listen on for HTTP clients
    -lookupd-tcp-address=[]: lookupd TCP address (may be given multiple times)
    -max-body-size=5123840: maximum size of a single command body
    -max-bytes-per-file=104857600: number of bytes per diskqueue file before rolling
    -max-message-size=1024768: maximum size of a single message in bytes
    -mem-queue-size=10000: number of messages to keep in memory (per topic/channel)
    -msg-timeout=60000: time (ms) to wait before auto-requeing a message
    -statsd-address="": UDP <addr>:<port> of a statsd daemon for writing stats
    -statsd-interval=30: seconds between pushing to statsd
    -sync-every=2500: number of messages between diskqueue syncs
    -tcp-address="0.0.0.0:4150": <addr>:<port> to listen on for TCP clients
    -verbose=false: enable verbose logging
    -version=false: print version string
    -worker-id=0: unique identifier (int) for this worker (will default to a hash of hostname)
    -broadcast-address: the address for this worker.  this is registered with nsqlookupd (defaults to OS hostname)
    
### Statsd / Graphite Integration

When using `--statsd-address` specify the UDP `<addr>:<port>` for
[statsd](https://github.com/etsy/statsd) (or a port of statsd like
[gographite](https://github.com/bitly/gographite)), `nsqd` will push metrics to statsd
periodically based on the interval specified in `--statsd-interval`. With this enabled `nsqadmin`
can be configured to display charts directly from graphite.

We recommend the following configuration for graphite `storage-schemas.conf`

```
[nsq]
pattern = ^nsq\..*
retentions = 1m:1d,5m:30d,15m:1y
````

And the following for `storage-aggregation.conf`

```
[default_nsq]
pattern = ^nsq\..*
xFilesFactor = 0.2 
aggregationMethod = average
```

