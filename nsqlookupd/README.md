## nsqlookupd

`nsqlookupd` is the daemon that manages topology information. Clients query `nsqlookupd` to discover
`nsqd` producers for a specific topic and `nsqd` nodes broadcasts topic and channel information.

There are two interfaces: A TCP interface which is used by `nsqd` for broadcasts and an HTTP
interface for clients to perform discovery and administrative actions.

### Command Line Options

    -http-address="0.0.0.0:4161": <addr>:<port> to listen on for HTTP clients
    -inactive-producer-timeout=5m0s: duration of time a producer will remain in the active list since its last ping
    -tcp-address="0.0.0.0:4160": <addr>:<port> to listen on for TCP clients
    -broadcast-address: external address of this lookupd node, (default to the OS hostname)
    -tombstone-lifetime=45s: duration of time a producer will remain tombstoned if registration remains
    -verbose=false: enable verbose logging
    -version=false: print version string

### HTTP Interface

 * `/ping` (returns "OK" for use with monitoring)
 * `/lookup?topic=....` (returns a list of producers for a topic)
 * `/topics` (returns a list of all the topics it knows about)
 * `/channels?topic=...` (returns a list of all the channels for a topic)
 * `/nodes` (returns a list of all the producers it knows about)
 * `/delete_topic?topic=...` (deletes a topic)
 * `/delete_channel?topic=...&channel=...` (deletes a channel)
 * `/tombstone_topic_producer?topic=...&node=...` (tombstones a specific producer of a topic)
 * `/info` (returns server version information)

### Deletion and Tombstones

When a topic is no longer produced it is a relatively simple operation to purge that information
from the cluster. Assuming all the applications that were producing messages are downed, using the
`/delete_topic` endpoint of your `nsqlookupd` instances is all that is necessary to complete the
operation (internally, it will identify the relevant `nsqd` producers and perform the appropriate
actions on those nodes).

For a channel deletion the process is similar, the only difference being you would use the
`/delete_channel` endpoint on your `nsqlookupd` instances and you would need to ensure that all
consumers that were subscribed the the channel were already downed.

However, it gets a bit more complicated when a topic is no longer produced on a *subset* of nodes.
Because of the way consumers query `nsqlookupd` and connect to all producers you enter into race
conditions with attempting to remove the information from the cluster and consumers discovering that
node and reconnecting (thus pushing updates that the topic is still produced on that node). The
solution in these cases is to use "tombstones". A tombstone in `nsqlookupd` context is producer
specific and lasts for a configurable `--tombstone-lifetime` time. During that window the producer
will not be listed in `/lookup` queries, allowing the node to delete the topic, propagate that
information to `nsqlookupd` (which then *removes* the tombstoned producer), and prevent any consumer
from re-discovering that node.
