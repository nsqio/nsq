## nsqd

`nsqd` is the daemon that receives, queues, and delivers messages to clients.

When `nsqd` is configured with `nsqlookupd_tcp_addresses`, `intial-grace-period` controls how long startup will wait for the initial nsqlookupd sync and topic/channel pre-creation before opening for message traffic. The default is `0s`, which means `nsqd` will open immediately if lookupd is unavailable; the maximum supported value is `300s`.

Read the [docs](https://nsq.io/components/nsqd.html)
