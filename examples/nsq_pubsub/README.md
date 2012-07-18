nsq_pubsub
==========

This provides a HTTP streaming interface to nsq channels.


   /sub?topic_name=....&channel_name=....

Each connection will get heartbeats pushed to it in the following format every 30 seconds

    {"_heartbeat_":1343400473}


