## nsqd

`nsqd` is the daemon that receives, queues, and delivers messages to clients.

Read the [docs](http://nsq.io/components/nsqd.html).

### Docker

1. Pull down this image:

        docker pull nsqio/nsqd

2. Run the image:

        docker run --name nsqd -p 4150:4150 -p 4151:4151
            nsqio/nsqd 
            --broadcast-address=<host> 
            --lookupd-tcp-address=<host>:<port>

Take a look at the [NSQ docker documentation](http://nsq.io/deployment/docker.html) to see how this
can be used with the official [nsqlookupd](https://registry.hub.docker.com/u/nsqio/nsqlookupd/)
image.
