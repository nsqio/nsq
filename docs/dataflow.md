## NSQ Cluster Data Flow

       /------------------------------------------------------------+
       |                                                            |
       |        /----------------+                                  |
       |        | RegistrationDB |                                  |         /----------+
       |        +----------------/                                  |<------->| nsqadmin |
       |              |                                             |         +----------/
       |          nsqlookupd            nsqlookupd      nsqlookupd  |
       |              |                     |               |       |
       +------------------------------------------------------------/
                     |  ^                                   ^
           (OK, err) |  | IDENTIFY                          |
                     |  | REGISTER $topic $channel          | /lookup?topic=... (HTTP)
                     |  | UNREGISTER $topic $channel        |
                     |  | PING(15s)                         \--------------\
                     V  |                                                  |
       /----------------------------------------\                          |
       |                        |               |                          |
       |- lookupLoop()          |               |                          |
       |                        |               |                          |
    /------+                /------+        /------+                       |
    | nsqd |                | nsqd |        | nsqd |                       |
    +------/                +------/        +------/                       |
       |                                                                   |
       |- idPump(workerId)                                                 |
       |                                                                   |
       |- httpServer()                                                     |
       |    /put                                                           |
       |    /mput                                                          | producer
       |    ...                                                            | discovery
       |                                                                   |
       |                                                             /------------+
       |                                                             | NSQ Reader |
       |             NOP                                             +------------/
       |             PUB MPUB                                             ...
       |             IDENTIFY SUB RDY FIN REQ CLS                    /------------+
       |- tcpServer() <--------------------------------------------> | NSQ Reader |
       |    |                                                        +------------/
       |  IOLoop()                                                        ^
       |    |                                                             |
       |    | IDENTIFY                                                    |
       |    | SUB topic/chan                                              |
       |    |                                                             |
       |    \----------------- messagePump()                              |
       |                            |                                     |
       |                            |- heartbeat                      /- Send()
       |                            \- client.Channel.clientMsgChan --|
       |                                            ^                 \- StartInFlightTimeout()
       |                                            |
       \- topic(s)                                  \---------------------------------------\
            |                                                                               |
            |- router()                                                                     |
            |    |                                                                          |
            |    \----> incomingMsgChan => [memoryMsgChan | backend]                        |
            |                                                                               |
            |- messagePump() (lazy)                                                         |
            |    |                                                                          |
            |    \----> [memoryMsgChan | backend] => dup(msg) => channel.incomingMsgChan    |
            |                                                                               |
            \- channel(s)                                                                   |
                  |                                                                         |
                  |- incomingMsg => memoryMsg => inFlightMsg => deferredMsg                 |
                  |                                                                         |
                  |- messagePump()                                                          |
                  |     |                                                                   |
                  |     \---> [memoryMsgChan | backend] => clientMsg -----------------------/
                  |
                  |- router()
                  |     |
                  |     \---------> incomingMsgChan => [memoryMsgChan | backend]
                  |                      ^  ^
                  |                      |  |
                  |                      |  \------------------------------------\
                  |                      |                                       |
                  |                      \---------------------\                 |
                  |                                            |                 |
                  |- deferredWorker() priority queue (heap) ---/                 |
                  |                                                              |
                  \- inFlightWorker() priority queue (heap) ---- (60s timeout) --/
