FROM golang:latest AS build

RUN mkdir -p /go/src/github.com/nsqio/nsq
COPY . /go/src/github.com/nsqio/nsq

WORKDIR /go/src/github.com/nsqio/nsq

RUN wget -O /bin/dep https://github.com/golang/dep/releases/download/v0.4.1/dep-linux-amd64 \
 && chmod +x /bin/dep \
 && /bin/dep ensure \
 && ./test.sh \
 && CGO_ENABLED=0 make DESTDIR=/opt PREFIX=/nsq BLDFLAGS='-ldflags="-s -w"' install

FROM alpine:3.7

EXPOSE 4150 4151 4160 4161 4170 4171

# Optional volumes
# /data - used for persistent storage across reboots
# VOLUME /data
# /etc/ssl/certs - directory for SSL certificates
# VOLUME /etc/ssl/certs

COPY --from=build /opt/nsq/bin/ /usr/local/bin/
RUN ln -s /usr/local/bin/*nsq* / \
 && ln -s /usr/local/bin/*nsq* /bin/
