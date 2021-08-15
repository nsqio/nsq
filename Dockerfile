FROM golang:alpine AS build

RUN apk update && apk add make gcc musl-dev

RUN mkdir -p /go/src/github.com/nsqio/nsq
COPY    .    /go/src/github.com/nsqio/nsq
WORKDIR      /go/src/github.com/nsqio/nsq

RUN ./test.sh
RUN CGO_ENABLED=0 make PREFIX=/opt/nsq BLDFLAGS='-ldflags="-s -w"' install


FROM alpine:latest

EXPOSE 4150 4151 4160 4161 4170 4171

RUN mkdir -p /data
WORKDIR      /data

# set up nsswitch.conf for Go's "netgo" implementation
# https://github.com/golang/go/issues/35305
RUN [ ! -e /etc/nsswitch.conf ] && echo 'hosts: files dns' > /etc/nsswitch.conf

# Optional volumes (explicitly configure with "docker run -v ...")
# /data          - used by nsqd for persistent storage across restarts
# /etc/ssl/certs - for SSL Root CA certificates from host

COPY --from=build /opt/nsq/bin/ /usr/local/bin/
RUN ln -s /usr/local/bin/*nsq* / \
 && ln -s /usr/local/bin/*nsq* /bin/
