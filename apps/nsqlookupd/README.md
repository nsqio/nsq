## nsqlookupd

`nsqlookupd` is the daemon that manages topology metadata and serves client requests to
discover the location of topics at runtime.

Read the [docs](http://nsq.io/components/nsqlookupd.html).

### Docker

1. Pull down this image:

        docker pull nsqio/nsqlookupd

2. Run the image:

        docker run --name nsqlookupd -p 4160:4160 -p 4161:4161 nsqio/nsqlookupd

Take a look at the [NSQ docker documentation](http://nsq.io/deployment/docker.html) to see how this
can be used with the official [nsqd](https://registry.hub.docker.com/u/nsqio/nsqd/) image.
