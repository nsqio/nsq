FROM google/golang:latest

RUN curl -s https://raw.githubusercontent.com/pote/gpm/v1.2.3/bin/gpm > /usr/local/bin/gpm
RUN chmod +x /usr/local/bin/gpm

ADD . $GOPATH/src/github.com/bitly/nsq
RUN cd $GOPATH/src/github.com/bitly/nsq && gpm install
RUN go get github.com/bitly/nsq/...

RUN cd $GOPATH/src/github.com/bitly/nsq && ./test.sh
