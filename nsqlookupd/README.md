nsqlookupd
==========

`nsqlookupd` is the daemon that manages topology information. Clients query `nsqlookupd` to lookup `nsqd` locations for
a specific topic, and `nsqd` broadcasts topic and channel information.

There are two interfaces: A TCP interface which is used by nsqd for broadcasts, and a HTTP interface for clients to
perform discovery.

The HTTP interface has the following API endpoints

 * `/lookup?topic=....`
 * `/topics`
 * `/delete_channel?topic=...&channel=...`
 * `/ping` (returns "OK" for use with monitoring)

Command Line Options
--------------------

    Usage of ./nsqlookupd:
      -debug=false: enable debug mode
      -http-address="0.0.0.0:4161": <addr>:<port> to listen on for HTTP clients
      -tcp-address="0.0.0.0:4160": <addr>:<port> to listen on for TCP clients
      -version=false: print version string
