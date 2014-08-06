FROM google/golang:latest

RUN go get -u github.com/tools/godep
RUN go get -u github.com/bmizerany/assert

ADD . $GOPATH/src/github.com/bitly/nsq
RUN godep get github.com/bitly/nsq/...
RUN cd $GOPATH/src/github.com/bitly/nsq && godep restore

RUN cd $GOPATH/src/github.com/bitly/nsq && ./test.sh
