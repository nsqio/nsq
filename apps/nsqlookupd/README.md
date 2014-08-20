## nsqlookupd 

### Installation

1. Install [Docker](https://www.docker.io/).

2. Pull down this image: `docker pull nsqio/nsqlookupd`.


### Usage

    docker run --name lookupd -p 4160:4160 -p 4161:4161 nsqio/nsqlookupd

Take a look at the [docs](http://nsq.io/deployment/docker.html) to see how this is used with the official [nsqd image](https://registry.hub.docker.com/u/nsqio/nsqd/).