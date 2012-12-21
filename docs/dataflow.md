## NSQ Basic Data Flow


                             nsqadmin
                                |
        ------------------------------------------------------------
       |                                                            |
       |        /----------------+                                  |
       |        | RegistrationDB |                                  |
       |        +----------------/                                  |
       |              |                                             |
       |        nsqlookupd              nsqlookupd      nsqlookupd  |
       |              |                     |               |       |
        ------------------------------------------------------------
                     |  ^
           (OK, err) |  | IDENTIFY
                     |  | REGISTER $topic $channel
                     |  | UNREGISTER $topic $channel
                     |  | PING(15s)
                     V  |       
        ------------------------------------------------
       |                        |       |       |       |
       | lookupLoop             |       |       |       |
       |                        |       |       |       |
     nsqd                      nsqd    nsqd    nsqd    nsqd
       |                                     
       |- idPump(workerId)                  
       |                                   
       |- httpServer
       |
       |             NOP
       |             PUB MPUB
       |             SUB RDY FIN REQ CLS
       |- tcpServer <----------------------------------------------> nsq reader
       |    |                                                            ^
       |  IOLoop()                                                       |
       |    |                                                            |
       |    | SUB topic/chan                                             |
       |     ----------------- messagePump                               |
       |                            |                                    |
       |                            |- heartbeat                      - Send
       |                             - client.Channel.clientMsgChan -|
       |                                            ^                 - StartInFlightTimeout
       |                                            |
        - topicS                                     ---------------------------------------
            |                                                                               |
            |- router()                                                                     |
            |    |                                                                          |
            |     - incomingMsgChan => [memoryMsgChan | backend]                            |
            |                                                                               |
            |- if any channel, messagePump()                                                |
            |    |                                                                          |
            |     - [memoryMsgChan | backend] => dup(msg) => channel.incomingMsgChan        |
            |                                                                               |
             - channelS                                                                     |
                  |                                                                         |
                  |- incomingMsg => memoryMsg => inflighMsg => deferredMsg                  |
                  |                                                                         |
                  |- messagePump()                                                          |
                  |     |                                                                   |
                  |      - [memoryMsgChan | backend] => clientMsg --------------------------
                  |
                  |- router()
                  |     |
                  |      - incomingMsgChan => [memoryMsgChan | backend]
                  |
                  |- deferredWorker()
                   - inFlightWorker()


