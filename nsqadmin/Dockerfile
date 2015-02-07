FROM nsqio/deprecated_nsq_base:latest

RUN cd /gopath/src/github.com/bitly/nsq/nsqadmin && go build .
EXPOSE 4171

ENTRYPOINT ["/gopath/src/github.com/bitly/nsq/nsqadmin/nsqadmin"]
