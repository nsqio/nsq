// This is a client exposes a HTTP streaming interface to NSQ channels

package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/nsqio/nsq/internal/app"
	"github.com/nsqio/nsq/internal/http_api"
	"github.com/nsqio/nsq/internal/version"
)

var (
	showVersion      = flag.Bool("version", false, "print version string")
	httpAddress      = flag.String("http-address", "0.0.0.0:8080", "<addr>:<port> to listen on for HTTP clients")
	maxInFlight      = flag.Int("max-in-flight", 100, "max number of messages to allow in flight")
	nsqdTCPAddrs     = app.StringArray{}
	lookupdHTTPAddrs = app.StringArray{}
)

func init() {
	flag.Var(&nsqdTCPAddrs, "nsqd-tcp-address", "nsqd TCP address (may be given multiple times)")
	flag.Var(&lookupdHTTPAddrs, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
}

type StreamServer struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	messageCount uint64

	sync.RWMutex // embed a r/w mutex
	clients      []*StreamReader
}

func (s *StreamServer) Set(sr *StreamReader) {
	s.Lock()
	defer s.Unlock()
	s.clients = append(s.clients, sr)
}

func (s *StreamServer) Del(sr *StreamReader) {
	s.Lock()
	defer s.Unlock()
	n := make([]*StreamReader, len(s.clients)-1)
	for _, x := range s.clients {
		if x != sr {
			n = append(n, x)
		}
	}
	s.clients = n
}

var streamServer *StreamServer

type StreamReader struct {
	sync.RWMutex // embed a r/w mutex
	topic        string
	channel      string
	consumer     *nsq.Consumer
	req          *http.Request
	conn         net.Conn
	bufrw        *bufio.ReadWriter
	connectTime  time.Time
}

func ConnectToNSQAndLookupd(r *nsq.Consumer, nsqAddrs []string, lookupd []string) error {
	for _, addrString := range nsqAddrs {
		err := r.ConnectToNSQD(addrString)
		if err != nil {
			return err
		}
	}

	for _, addrString := range lookupd {
		log.Printf("lookupd addr %s", addrString)
		err := r.ConnectToNSQLookupd(addrString)
		if err != nil {
			return err
		}
	}

	return nil
}

func StatsHandler(w http.ResponseWriter, req *http.Request) {
	totalMessages := atomic.LoadUint64(&streamServer.messageCount)
	io.WriteString(w, fmt.Sprintf("Total Messages: %d\n\n", totalMessages))

	now := time.Now()
	for _, sr := range streamServer.clients {
		duration := now.Sub(sr.connectTime).Seconds()
		secondsDuration := time.Duration(int64(duration)) * time.Second // turncate to the second

		io.WriteString(w, fmt.Sprintf("[%s] [%s : %s] connected: %s\n",
			sr.conn.RemoteAddr().String(),
			sr.topic,
			sr.channel,
			secondsDuration))
	}
}

func (s *StreamServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	path := req.URL.Path
	if path == "/stats" {
		StatsHandler(w, req)
		return
	}
	if path != "/sub" {
		w.WriteHeader(404)
		return
	}

	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	topicName, channelName, err := http_api.GetTopicChannelArgs(reqParams)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "httpserver doesn't support hijacking", http.StatusInternalServerError)
		return
	}
	conn, bufrw, err := hj.Hijack()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	cfg := nsq.NewConfig()
	cfg.UserAgent = fmt.Sprintf("nsq_pubsub/%s go-nsq/%s", version.Binary, nsq.VERSION)
	cfg.MaxInFlight = *maxInFlight
	r, err := nsq.NewConsumer(topicName, channelName, cfg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	r.SetLogger(log.New(os.Stderr, "", log.LstdFlags), nsq.LogLevelInfo)

	sr := &StreamReader{
		topic:       topicName,
		channel:     channelName,
		consumer:    r,
		req:         req,
		conn:        conn,
		bufrw:       bufrw, // TODO: latency writer
		connectTime: time.Now(),
	}
	s.Set(sr)

	log.Printf("[%s] new connection", conn.RemoteAddr().String())
	bufrw.WriteString("HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n")
	bufrw.Flush()

	r.AddHandler(sr)

	// TODO: handle the error cases better (ie. at all :) )
	errors := ConnectToNSQAndLookupd(r, nsqdTCPAddrs, lookupdHTTPAddrs)
	log.Printf("connected to NSQ %v", errors)

	// this read allows us to detect clients that disconnect
	go func(rw *bufio.ReadWriter) {
		b, err := rw.ReadByte()
		if err != nil {
			log.Printf("got connection err %s", err.Error())
		} else {
			log.Printf("unexpected data on request socket (%c); closing", b)
		}
		sr.consumer.Stop()
	}(bufrw)

	go sr.HeartbeatLoop()
}

func (sr *StreamReader) HeartbeatLoop() {
	heartbeatTicker := time.NewTicker(30 * time.Second)
	defer func() {
		sr.conn.Close()
		heartbeatTicker.Stop()
		streamServer.Del(sr)
	}()
	for {
		select {
		case <-sr.consumer.StopChan:
			return
		case ts := <-heartbeatTicker.C:
			sr.Lock()
			sr.bufrw.WriteString(fmt.Sprintf("{\"_heartbeat_\":%d}\n", ts.Unix()))
			sr.bufrw.Flush()
			sr.Unlock()
		}
	}
}

func (sr *StreamReader) HandleMessage(message *nsq.Message) error {
	sr.Lock()
	sr.bufrw.Write(message.Body)
	sr.bufrw.WriteString("\n")
	sr.bufrw.Flush()
	sr.Unlock()
	atomic.AddUint64(&streamServer.messageCount, 1)
	return nil
}

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Printf("nsq_pubsub v%s\n", version.Binary)
		return
	}

	if *maxInFlight <= 0 {
		log.Fatalf("--max-in-flight must be > 0")
	}

	if len(nsqdTCPAddrs) == 0 && len(lookupdHTTPAddrs) == 0 {
		log.Fatalf("--nsqd-tcp-address or --lookupd-http-address required.")
	}

	if len(nsqdTCPAddrs) != 0 && len(lookupdHTTPAddrs) != 0 {
		log.Fatalf("use --nsqd-tcp-address or --lookupd-http-address not both")
	}

	httpAddr, err := net.ResolveTCPAddr("tcp", *httpAddress)
	if err != nil {
		log.Fatal(err)
	}
	httpListener, err := net.Listen("tcp", httpAddr.String())
	if err != nil {
		log.Fatalf("FATAL: listen (%s) failed - %s", httpAddr.String(), err.Error())
	}
	log.Printf("listening on %s", httpAddr.String())

	streamServer = &StreamServer{}
	server := &http.Server{Handler: streamServer}
	err = server.Serve(httpListener)

	// theres no direct way to detect this error because it is not exposed
	if err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
		log.Printf("ERROR: http.Serve() - %s", err.Error())
	}
}
