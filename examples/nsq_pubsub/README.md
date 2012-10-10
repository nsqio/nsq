nsq_pubsub
==========

This provides a HTTP streaming interface to nsq channels.


   /sub?topic=....&channel=....

Each connection will get heartbeats pushed to it in the following format every 30 seconds

    {"_heartbeat_":1343400473}

There is also a stats endpoint which will list out information about connected clients

    /stats

It's output looks like this. Total messages is the count of messages delivered to HTTP clients since startup. 

    Total Messages: 0
    
    [127.0.0.1:50386] [test_topic : test_channel] msgs: 0        fin: 0        re-q: 0        connected: 3s

