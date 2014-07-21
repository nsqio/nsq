FROM nsqio/nsq:latest

RUN cd /gopath/src/github.com/bitly/nsq/nsqadmin && godep go build .
EXPOSE 4171

ENTRYPOINT ["/gopath/src/github.com/bitly/nsq/nsqadmin/nsqadmin"]
