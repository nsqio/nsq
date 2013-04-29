## nsqd

`nsqd` is the daemon that receives, buffers, and delivers messages to clients.

It can be run and used standalone but is normally configured to talk to with `nsqlookupd` 
instance(s) in which case it will announce topic and channels for discovery.

It listens on two TCP ports, one for clients and another for the HTTP API.

### HTTP API

 * `/put?topic=...` - **POST** message body, ie `$ curl -d "<message>" http://127.0.0.1:4151/put?topic=message_topic`
 * `/mput?topic=...` - **POST** message body (`\n` separated, TODO: it is incompatible with binary message formats)
 * `/create_channel?topic=...&channel=...`
 * `/delete_channel?topic=...&channel=...`
 * `/empty_channel?topic=...&channel=...`
 * `/pause_channel?topic=...&channel=...`
 * `/unpause_channel?topic=...&channel=...`
 * `/create_topic?topic=...`
 * `/delete_topic?topic=...`
 * `/empty_topic?topic=...`
 * `/stats` - supports both text (default) and JSON via `?format=json`
 * `/ping` - returns `OK` (useful for monitoring)
 * `/info` - returns version information

### Command Line Options

    -broadcast-address="": address that will be registered with lookupd, (default to the OS hostname)
    -data-path="": path to store disk-backed messages
    -http-address="0.0.0.0:4151": <addr>:<port> to listen on for HTTP clients
    -lookupd-tcp-address=[]: lookupd TCP address (may be given multiple times)
    -max-body-size=5123840: maximum size of a single command body
    -max-bytes-per-file=104857600: number of bytes per diskqueue file before rolling
    -max-heartbeat-interval=1m0s: maximum duration of time between heartbeats that a client can configure
    -max-message-size=1024768: maximum size of a single message in bytes
    -max-msg-timeout=15m0s: maximum duration before a message will timeout
    -max-rdy-count=2500: maximum RDY count for a single client
    -mem-queue-size=10000: number of messages to keep in memory (per topic/channel)
    -msg-timeout="60s": duration to wait before auto-requeing a message
    -statsd-address="": UDP <addr>:<port> of a statsd daemon for writing stats
    -statsd-interval=30: seconds between pushing to statsd
    -sync-every=2500: number of messages between diskqueue syncs
    -tcp-address="0.0.0.0:4150": <addr>:<port> to listen on for TCP clients
    -verbose=false: enable verbose logging
    -version=false: print version string
    -worker-id=0: unique identifier (int) for this worker (will default to a hash of hostname)

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
