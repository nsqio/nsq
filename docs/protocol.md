The NSQ protocol is simple so building clients should be trivial in any language. We provide
official Go and Python client libraries.

An `nsqd` process listens on a configurable TCP port that accepts client connections.

After connecting, a client must send a 4-byte "magic" identifier indicating what version of the
protocol they will be communicating (upgrades made easy).

For `nsqd`, there is currently only one protocol:

  * `V2` (4-byte ASCII `[space][space][V][2]`)
     a push based streaming protocol for consuming (and request/response for publishing)

### Important Notes

  * Unless stated otherwise, **all** binary sizes/integers on the wire are **network byte order**
    (ie. *big* endian)
  * Valid *topic* and *channel* names are characters `[.a-zA-Z0-9_-]` and `1 < length <= 32`

### V2 Protocol

After authenticating, the client can optionally send an `IDENTIFY` command to provide custom
metadata (like, for example, more descriptive identifiers). In order to begin consuming messages, a
client must `SUB` to a channel.

Upon subscribing the client is placed in a `RDY` state of 0. This means that no messages
will be sent to the client. When a client is ready to receive messages it should send a command that
updates its `RDY` state to some # it is prepared to handle, say 100. Without any additional
commands, 100 messages will be pushed to the client as they are available (each time decrementing
the server-side `RDY` count for that client).

The **V2** protocol also features client heartbeats. Every 30 seconds by default, `nsqd` will send a
`_heartbeat_` response and expect a command in return. If the client is idle, send `NOP`. After 2 
unanswered `_heartbeat_` responses, `nsqd` will timeout and forcefully close a client connection that
it has not heard from. The `IDENTIFY` command may be used to change/disable this behavior.

Commands are line oriented and structured as follows:

  * `IDENTIFY` - update client metadata on the server
    
        IDENTIFY\n
        [ 4-byte size in bytes ][ N-byte JSON data ]
    
    NOTE: this command takes a size prefixed JSON body, relevant fields:
    
    **`short_id`** an identifier used as a short-form descriptor (ie. short hostname)
    
    **`long_id`** an identifier used as a long-form descriptor (ie. fully-qualified hostname)
    
    **`heartbeat_interval`** (nsqd 0.2.19+) milliseconds between heartbeats where:
    
        1000 <= heartbeat_interval <= configured_max
        (may also be set to -1 to disable heartbeats)
        
        defaults to --client-timeout / 2
        --max-heartbeat-interval (nsqd flag) controls the max
    
    **`output_buffer_size`** (nsqd 0.2.21+) the size in bytes of the buffer nsqd will use when
                             writing to this client.
        
        64 <= output_buffer_size <= configured_max
        (may also be set to -1 to disable output buffering)
        
        defaults to 16kb
        --max-output-buffer-size (nsqd flag) controls the max
    
    **`output_buffer_timeout`** (nsqd 0.2.21+) the timeout after which any data that nsqd has
                                buffered will be flushed to this client.
        
        5ms <= output_buffer_timeout <= configured_max
        (may also be set to -1 to disable timeouts on output buffering)
        
        defaults to 5ms
        --max-output-buffer-timeout (nsqd flag) controls the max
    
    **`feature_negotiation`** bool used to indicate that the client supports feature negotiation. 
                              If the server is capable, it will send back a JSON payload of 
                              features and metadata.
    
    Success Response:
    
        OK
    
    Error Responses:
    
        E_INVALID
        E_BAD_BODY

  * `SUB` - subscribe to a specified topic/channel
    
        SUB <topic_name> <channel_name>\n
        
        <topic_name> - a valid string
        <channel_name> - a valid string (optionally having #ephemeral suffix)
    
    Success response:
    
        OK
    
    Error Responses:
    
        E_INVALID
        E_BAD_TOPIC
        E_BAD_CHANNEL

  * `PUB` - publish a message to a specified **topic**:
    
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

  * `MPUB` - publish multiple messages to a specified **topic**:
    
    NOTE: available in 0.2.16+
    
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

  * `RDY` - update `RDY` state (indicate you are ready to receive messages)
    
    NOTE: as of 0.2.20+ nsqd has --max-rdy-count to configure its max RDY count
    
        RDY <count>\n
        
        <count> - a string representation of integer N where 0 < N <= configured_max
    
    NOTE: there is no success response
    
    Error Responses:
    
        E_INVALID

  * `FIN` - finish a message (indicate *successful* processing)
    
        FIN <message_id>\n
        
        <message_id> - message id as 16-byte hex string
    
    NOTE: there is no success response

    Error Responses:
    
        E_INVALID
        E_FIN_FAILED

  * `REQ` - re-queue a message (indicate *failure* to procees)
    
        REQ <message_id> <timeout>\n
        
        <message_id> - message id as 16-byte hex string
        <timeout> - a string representation of integer N where N <= configured max timeout
            0 is a special case that will not defer re-queueing
    
    NOTE: there is no success response

    Error Responses:
    
        E_INVALID
        E_REQ_FAILED

  * `TOUCH` - reset the timeout for an in-flight message
    
    NOTE: available in 0.2.17+
    
        TOUCH <message_id>\n
        
        <message_id> - the hex id of the message
    
    NOTE: there is no success response
    
    Error Responses:
    
        E_INVALID
        E_TOUCH_FAILED

  * `CLS` - cleanly close your connection (no more messages are sent)
    
        CLS\n
    
    Success Responses:
    
        CLOSE_WAIT
    
    Error Responses:
    
        E_INVALID

  * `NOP` - no-op
    
        NOP\n
    
    NOTE: there is no response

Data is streamed asynchronously to the client and framed in order to support the various reply
bodies, ie:

    [x][x][x][x][x][x][x][x][x][x][x][x]...
    |  (int32) ||  (int32) || (binary)
    |  4-byte  ||  4-byte  || N-byte
    ------------------------------------...
        size      frame ID     data

A client should expect one of the following frame identifiers:

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
