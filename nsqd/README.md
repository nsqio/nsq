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
* `/stats`

    supports both text and JSON via `?format=json`

* `/ping`

    returns `OK`, helpful when monitoring

* `/info`

    returns version information

### Command Line Options

    -data-path="": path to store disk-backed messages
    -debug=false: enable debug mode
    -http-address="0.0.0.0:4151": <addr>:<port> to listen on for HTTP clients
    -lookupd-tcp-address=[]: lookupd TCP address (may be given multiple times)
    -max-bytes-per-file=104857600: number of bytes per diskqueue file before rolling
    -mem-queue-size=10000: number of messages to keep in memory (per topic)
    -msg-timeout=60000: time (ms) to wait before auto-requeing a message
    -sync-every=2500: number of messages between diskqueue syncs
    -tcp-address="0.0.0.0:4150": <addr>:<port> to listen on for TCP clients
    -verbose=false: enable verbose logging
    -version=false: print version string
    -worker-id=0: unique identifier (int) for this worker (will default to a hash of hostname)
