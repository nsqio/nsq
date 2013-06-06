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

    -broadcast-address="": address that will be registered with lookupd (defaults to the OS hostname)
    -data-path="": path to store disk-backed messages
    -http-address="0.0.0.0:4151": <addr>:<port> to listen on for HTTP clients
    -lookupd-tcp-address=[]: lookupd TCP address (may be given multiple times)
    -max-body-size=5123840: maximum size of a single command body
    -max-bytes-per-file=104857600: number of bytes per diskqueue file before rolling
    -max-heartbeat-interval=1m0s: maximum client configurable duration of time between client heartbeats
    -max-message-size=1024768: maximum size of a single message in bytes
    -max-msg-timeout=15m0s: maximum duration before a message will timeout
    -max-output-buffer-size=65536: maximum client configurable size (in bytes) for a client output buffer
    -max-output-buffer-timeout=1s: maximum client configurable duration of time between flushing to a client
    -max-rdy-count=2500: maximum RDY count for a client
    -mem-queue-size=10000: number of messages to keep in memory (per topic/channel)
    -msg-timeout="60s": duration to wait before auto-requeing a message
    -statsd-address="": UDP <addr>:<port> of a statsd daemon for pushing stats
    -statsd-interval="60s": duration between pushing to statsd
    -sync-every=2500: number of messages per diskqueue fsync
    -sync-timeout=2s: duration of time per diskqueue fsync
    -tcp-address="0.0.0.0:4150": <addr>:<port> to listen on for TCP clients
    -verbose=false: enable verbose logging
    -version=false: print version string
    -worker-id=0: unique identifier (int) for this worker (will default to a hash of hostname)

### Statsd / Graphite Integration

When using `--statsd-address` to specify the UDP `<addr>:<port>` for
[statsd](https://github.com/etsy/statsd) (or a port of statsd like
[statsdaemon](https://github.com/bitly/statsdaemon)), `nsqd` will push metrics to statsd
periodically based on the interval specified in `--statsd-interval` (IMPORTANT: this interval should
**always** be less than or equal to the interval at which statsd flushes to graphite). With this
enabled `nsqadmin` can be configured to display charts directly from graphite.

We recommend the following configuration for graphite (but these choices should be evaluated based
on your available resources and requirements). Again, the important piece to remember is that statsd
should flush at an interval less than or equal to the smallest time bucket in `storage-schemas.conf`
and `nsqd` should be configured to flush at or below that same interval via `--statsd-interval`.


    # storage-schemas.conf

    [nsq]
    pattern = ^nsq\..*
    retentions = 1m:1d,5m:30d,15m:1y

    # storage-aggregation.conf

    [default_nsq]
    pattern = ^nsq\..*
    xFilesFactor = 0.2 
    aggregationMethod = average
