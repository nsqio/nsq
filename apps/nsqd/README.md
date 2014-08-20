## nsqd
 

### Installation

1. Install [Docker](https://www.docker.io/).

2. Pull down this image: `docker pull nsqio/nsqd`.


### Usage

	docker run --name nsqd -p 4150:4150 -p 4151:4151 \
    	nsqio/nsqd \
    	--broadcast-address=<host> \
    	--lookupd-tcp-address=<host>:<port>

Take a look at the [docs](http://nsq.io/deployment/docker.html) to see how this is used with the official [nsqlookupd image](https://registry.hub.docker.com/u/nsqio/nsqlookupd/).