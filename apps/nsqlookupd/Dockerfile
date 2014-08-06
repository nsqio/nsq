FROM nsqio/nsq:latest

RUN cd /gopath/src/github.com/bitly/nsq/apps/nsqlookupd && godep go build .
EXPOSE 4160
EXPOSE 4161

ENTRYPOINT ["/gopath/src/github.com/bitly/nsq/apps/nsqlookupd/nsqlookupd"]
