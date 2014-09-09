---
title: TCP Protocol Spec
layout: post
category: clients
permalink: /clients/tcp_protocol_spec.html
---

The NSQ protocol is simple enough that building clients should be trivial in any language. We
provide official Go and Python client libraries.

An `nsqd` process listens on a configurable TCP port that accepts client connections.

After connecting, a client must send a 4-byte "magic" identifier indicating what version of the
protocol they will be communicating (upgrades made easy).

 * `V2` (4-byte ASCII `[space][space][V][2]`)
   a push based streaming protocol for consuming (and request/response for publishing)

After authenticating, the client can optionally send an `IDENTIFY` command to provide custom
metadata (like, for example, more descriptive identifiers) and negotiate features. In order to begin
consuming messages, a client must `SUB` to a channel.

Upon subscribing the client is placed in a `RDY` state of 0. This means that no messages
will be sent to the client. When a client is ready to receive messages it should send a command that
updates its `RDY` state to some # it is prepared to handle, say 100. Without any additional
commands, 100 messages will be pushed to the client as they are available (each time decrementing
the server-side `RDY` count for that client).

The **V2** protocol also features client heartbeats. Every 30s (default but configurable), `nsqd`
will send a `_heartbeat_` response and expect a command in return. If the client is idle, send
`NOP`. After 2 unanswered `_heartbeat_` responses, `nsqd` will timeout and forcefully close a client
connection that it has not heard from. The `IDENTIFY` command may be used to change/disable this
behavior.

### <a name="notes">Notes</a>

 * Unless stated otherwise, **all** binary sizes/integers on the wire are **network byte order**
  (ie. *big* endian)

 * Valid *topic* and *channel* names are characters `[.a-zA-Z0-9_-]` and `1 < length <= 64` 
  (max length was `32` prior to `nsqd` `0.2.28`)

### Commands

#### <a name="identify">IDENTIFY</a>

Update client metadata on the server and negotiate features

    IDENTIFY\n
    [ 4-byte size in bytes ][ N-byte JSON data ]

NOTE: this command takes a size prefixed **JSON** body, relevant fields:

 * **`short_id`** (deprecated in favor of **`client_id`** as of `nsqd` `v0.2.28+`) an identifier
                  used as a short-form descriptor (ie. short hostname)

 * **`long_id`** (deprecated in favor of **`hostname`** as of `nsqd` `v0.2.28+`) an identifier
                 used as a long-form descriptor (ie. fully-qualified hostname)

 * **`client_id`** an identifier used to disambiguate this client (ie. something specific
                   to the consumer)

 * **`hostname`** the hostname where the client is deployed

 * **`feature_negotiation`** (`nsqd` `v0.2.19+`) bool used to indicate that the client supports
                             feature negotiation. If the server is capable, it will send back a
                             JSON payload of supported features and metadata.

 * **`heartbeat_interval`** (`nsqd` `v0.2.19+`) milliseconds between heartbeats.

     Valid range: `1000 <= heartbeat_interval <= configured_max` (`-1` disables heartbeats)

     `--max-heartbeat-interval` (nsqd flag) controls the max

     Defaults to `--client-timeout / 2`

 * **`output_buffer_size`** (`nsqd` `v0.2.21+`) the size in bytes of the buffer nsqd will use when
                            writing to this client.

     Valid range: `64 <= output_buffer_size <= configured_max` (`-1` disables output buffering)

     `--max-output-buffer-size` (nsqd flag) controls the max

     Defaults to `16kb`

 * **`output_buffer_timeout`** (`nsqd` `v0.2.21+`) the timeout after which any data that nsqd has
                                 buffered will be flushed to this client.

     Valid range: `1ms <= output_buffer_timeout <= configured_max` (`-1` disables timeouts)

     `--max-output-buffer-timeout` (nsqd flag) controls the max

     Defaults to `250ms`

     **Warning**: configuring clients with an extremely low (`< 25ms`) `output_buffer_timeout`
     has a significant effect on `nsqd` CPU usage (particularly with `> 50` clients connected).

     This is due to the current implementation relying on Go timers which are maintained by the Go
     runtime in a priority queue.  See the [commit message][043b79ac] in
     [pull request #236][pull_req_236] for more details.

 * **`tls_v1`** (`nsqd` `v0.2.22+`) enable TLS for this connection.

     `--tls-cert` and `--tls-key` (nsqd flags) enable TLS and configure the server certificate

     If the server supports TLS it will reply `"tls_v1": true`

     The client should begin the TLS handshake immediately after reading the `IDENTIFY` response

     The server will respond `OK` after completing the TLS handshake

 * **`snappy`** (`nsqd` `v0.2.23+`) enable snappy compression for this connection.

    `--snappy` (nsqd flag) enables support for this server side

    The client should expect an *additional*, snappy compressed `OK` response immediately
    after the `IDENTIFY` response.

    A client cannot enable both `snappy` and `deflate`.

 * **`deflate`** (`nsqd` `v0.2.23+`) enable deflate compression for this connection.

    `--deflate` (nsqd flag) enables support for this server side

    The client should expect an *additional*, deflate compressed `OK` response immediately
    after the `IDENTIFY` response.

    A client cannot enable both `snappy` and `deflate`.

 * **`deflate_level`** (`nsqd` `v0.2.23+`) configure the deflate compression level for this connection.

    `--max-deflate-level` (nsqd flag) configures the maximum allowed value

    Valid range: `1 <= deflate_level <= configured_max`

    Higher values mean better compression but more CPU usage for nsqd.

 * **`sample_rate`** (`nsqd` `v0.2.25+`) deliver a percentage of all messages received to this connection.

    Valid range: `0 <= sample_rate <= 99` (`0` disables sampling)

    Defaults to `0`

 * **`user_agent`** (`nsqd` `v0.2.25+`) a string identifying the agent for this client in
                    the spirit of HTTP

    Default: `<client_library_name>/<version>`

 * **`msg_timeout`** (`nsqd` `v0.2.28+`) configure the server-side message timeout in milliseconds for messages
                     delivered to this client.

Success Response:

    OK

NOTE: if `feature_negotiation` was sent by the client (and the server supports it) the
response will be a JSON payload as described above.

Error Responses:

    E_INVALID
    E_BAD_BODY

#### SUB

Subscribe to a topic/channel

    SUB <topic_name> <channel_name>\n

    <topic_name> - a valid string
    <channel_name> - a valid string (optionally having #ephemeral suffix)

Success response:

    OK

Error Responses:

    E_INVALID
    E_BAD_TOPIC
    E_BAD_CHANNEL

#### PUB

Publish a message to a **topic**:

    PUB <topic_name>\n
    [ 4-byte size in bytes ][ N-byte binary data ]

    <topic_name> - a valid string

Success Response:

    OK

Error Responses:

    E_INVALID
    E_BAD_TOPIC
    E_BAD_MESSAGE
    E_PUB_FAILED

#### MPUB

Publish multiple messages to a **topic** (atomically):

NOTE: available in `nsqd` `v0.2.16+`

    MPUB <topic_name>\n
    [ 4-byte body size ]
    [ 4-byte num messages ]
    [ 4-byte message #1 size ][ N-byte binary data ]
          ... (repeated <num_messages> times)

    <topic_name> - a valid string

Success Response:

    OK

Error Responses:

    E_INVALID
    E_BAD_TOPIC
    E_BAD_BODY
    E_BAD_MESSAGE
    E_MPUB_FAILED

#### RDY

Update `RDY` state (indicate you are ready to receive `N` messages)

NOTE: as of `nsqd` `v0.2.20+` use `--max-rdy-count` to bound this value

    RDY <count>\n

    <count> - a string representation of integer N where 0 < N <= configured_max

NOTE: there is no success response

Error Responses:

    E_INVALID

#### FIN

Finish a message (indicate *successful* processing)

    FIN <message_id>\n

    <message_id> - message id as 16-byte hex string

NOTE: there is no success response

Error Responses:

    E_INVALID
    E_FIN_FAILED

#### REQ

Re-queue a message (indicate *failure* to process)

The re-queued message is placed at the tail of the queue, equivalent to having just published it,
but for various implementation specific reasons that behavior should not be explicitly relied upon
and may change in the future.

Similarly, a message that is in-flight and times out behaves identically to an explicit `REQ`.

    REQ <message_id> <timeout>\n

    <message_id> - message id as 16-byte hex string
    <timeout> - a string representation of integer N where N <= configured max timeout
        0 is a special case that will not defer re-queueing

NOTE: there is no success response

Error Responses:

    E_INVALID
    E_REQ_FAILED

#### TOUCH

Reset the timeout for an in-flight message

NOTE: available in `nsqd` `v0.2.17+`

    TOUCH <message_id>\n

    <message_id> - the hex id of the message

NOTE: there is no success response

Error Responses:

    E_INVALID
    E_TOUCH_FAILED

#### CLS

Cleanly close your connection (no more messages are sent)

    CLS\n

Success Responses:

    CLOSE_WAIT

Error Responses:

    E_INVALID

#### NOP

No-op

    NOP\n

NOTE: there is no response

#### AUTH

NOTE: available in `nsqd` `v0.2.29+`

If the `IDENTIFY` response indicates `auth_required=true` the client must send `AUTH` before any
`SUB`, `PUB` or `MPUB` commands. If `auth_required` is not present (or `false`), a client must not
authorize.

When `nsqd` receives an `AUTH` command it delegates responsibility to the configured
`--auth-http-address` by performing an HTTP request with client metadata in the form of query
parameters: the connection's remote address, TLS state, and the supplied auth secret. See
[AUTH][nsqd_auth] for more details.

    AUTH\n
    [ 4-byte size in bytes ][ N-byte Auth Secret ]

Success Response:

A JSON payload describing the authorized client's identity, an optional URL and a count of
permissions which were authorized.

    {"identity":"...", "identity_url":"...", "permission_count":1}

Error Responses:

    E_AUTH_FAILED - An error occurred contacting an auth server
    E_UNAUTHORIZED - No permissions found


### Data Format

Data is streamed asynchronously to the client and framed in order to support the various reply
bodies, ie:

    [x][x][x][x][x][x][x][x][x][x][x][x]...
    |  (int32) ||  (int32) || (binary)
    |  4-byte  ||  4-byte  || N-byte
    ------------------------------------...
        size     frame type     data

A client should expect one of the following frame types:

    FrameTypeResponse int32 = 0
    FrameTypeError    int32 = 1
    FrameTypeMessage  int32 = 2

And finally, the message format:

    [x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x]...
    |       (int64)        ||    ||      (hex string encoded in ASCII)           || (binary)
    |       8-byte         ||    ||                 16-byte                      || N-byte
    ------------------------------------------------------------------------------------------...
      nanosecond timestamp    ^^                   message ID                       message body
                           (uint16)
                            2-byte
                           attempts

[pull_req_236]: https://github.com/bitly/nsq/pull/236
[043b79ac]: https://github.com/mreiferson/nsq/commit/043b79acda5fe57056b3cc21b2ef536d5615a2c2]
[nsqd_auth]: {{ site.baseurl }}/components/nsqd.html
