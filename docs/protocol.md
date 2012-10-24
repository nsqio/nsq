The NSQ protocol is simple so building clients should be trivial in any language. We provide
official Go and Python client libraries.

An `nsqd` process listens on a configurable TCP port that accepts client connections.

After connecting, a client must send a 4-byte "magic" identifier indicating what version of the
protocol they will be communicating (upgrades made easy).

For `nsqd`, there is currently only one protocol:

  * `  V2` - a push based streaming protocol for consuming (and request/response for publishing)

### Important Notes

  * Unless stated otherwise, **all** binary sizes/integers on the wire are **network byte order**
    (ie. *big* endian)
  * Valid *topic* and *channel* names are characters `[.a-zA-Z0-9_-]` and `1 < length < 32`

### V2 Protocol

After authenticating, in order to begin consuming messages, a client must `SUB` to a channel.

Upon subscribing the client is placed in a `RDY` state of 0. This means that no messages
will be sent to the client. When a client is ready to receive messages it should send a command that
updates its `RDY` state to some # it is prepared to handle, say 100. Without any additional
commands, 100 messages will be pushed to the client as they are available (each time decrementing
the server-side `RDY` count for that client).

The **V2** protocol also features client heartbeats. Every 30 seconds, `nsqd` will send a
`_heartbeat_` response and expect a command in return. If the client is idle, send `NOP`. After 60
seconds, `nsqd` will timeout and forcefully close a client connection that it has not heard from.

Commands are line oriented and structured as follows:

  * `SUB` - subscribe to a specified topic/channel
    
        SUB <topic_name> <channel_name> <short_id> <long_id>\n
        
        <topic_name> - a valid string
        <channel_name> - a valid string (optionally having #ephemeral suffix)
        <short_id> - an identifier used as a short-form descriptor (ie. short hostname)
        <long_id> - an identifier used as a long-form descriptor (ie. fully-qualified hostname)
    
    NOTE: there is no success response
    
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
        E_PUT_FAILED

  * `RDY` - update `RDY` state (indicate you are ready to receive messages)
    
        RDY <count>\n
        
        <count> - a string representation of integer N where 0 < N < 2500
    
    NOTE: there is no success response
    
    Error Responses:
    
        E_INVALID

  * `FIN` - finish a message (indicate *successful* processing)
    
        FIN <message_id>\n
        
        <message_id> - the hex id of the message
    
    NOTE: there is no success response
    
    Error Responses:
    
        E_INVALID
        E_FINISH_FAILED

  * `REQ` - re-queue a message (indicate *failure* to procees)
    
        REQ <message_id> <timeout>\n
        
        <message_id> - the hex id of the message
        <timeout> - a string representation of integer N where N < configured max timeout
            0 is a special case that will not defer re-queueing
    
    NOTE: there is no success response
    
    Error Responses:
    
        E_INVALID
        E_REQUEUE_FAILED

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
    |       (int64)        ||    ||                 (binary)                     || (binary)
    |       8-byte         ||    ||                 16-byte                      || N-byte
    ------------------------------------------------------------------------------------------...
      nanosecond timestamp    ^^                   message ID                       message body
                           (uint16)
                            2-byte
                           attempts
