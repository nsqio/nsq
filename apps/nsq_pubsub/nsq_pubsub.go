// This is a client exposes a HTTP streaming interface to NSQ channels

package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/bitly/go-nsq"
	"github.com/bitly/nsq/util"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	showVersion      = flag.Bool("version", false, "print version string")
	httpAddress      = flag.String("http-address", "0.0.0.0:8080", "<addr>:<port> to listen on for HTTP clients")
	maxInFlight      = flag.Int("max-in-flight", 100, "max number of messages to allow in flight")
	nsqdTCPAddrs     = util.StringArray{}
	lookupdHTTPAddrs = util.StringArray{}
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
	reader       *nsq.Reader
	req          *http.Request
	conn         net.Conn
	bufrw        *bufio.ReadWriter
	connectTime  time.Time
}

func ConnectToNSQAndLookupd(r *nsq.Reader, nsqAddrs []string, lookupd []string) error {
	for _, addrString := range nsqAddrs {
		err := r.ConnectToNSQ(addrString)
		if err != nil {
			return err
		}
	}

	for _, addrString := range lookupd {
		log.Printf("lookupd addr %s", addrString)
		err := r.ConnectToLookupd(addrString)
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

		io.WriteString(w, fmt.Sprintf("[%s] [%s : %s] msgs: %-8d fin: %-8d re-q: %-8d connected: %s\n",
			sr.conn.RemoteAddr().String(),
			sr.topic,
			sr.channel,
			sr.reader.MessagesReceived,
			sr.reader.MessagesFinished,
			sr.reader.MessagesRequeued,
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

	reqParams, err := util.NewReqParams(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	topicName, channelName, err := util.GetTopicChannelArgs(reqParams)
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

	r, err := nsq.NewReader(topicName, channelName)
	r.SetMaxInFlight(*maxInFlight)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	sr := &StreamReader{
		topic:       topicName,
		channel:     channelName,
		reader:      r,
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
			log.Printf("unexpected data on request socket (%s); closing", b)
		}
		sr.reader.Stop()
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
		case <-sr.reader.ExitChan:
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
		fmt.Printf("nsq_pubsub v%s\n", util.BINARY_VERSION)
		return
	}

	if *maxInFlight <= 0 {
		log.Fatalf("--max-in-flight must be > 0")
	}

	if len(nsqdTCPAddrs) == 0 && len(lookupdHTTPAddrs) == 0 {
		log.Fatalf("--nsqd-tcp-address or --lookupd-tcp-address required.")
	}

	if len(nsqdTCPAddrs) != 0 && len(lookupdHTTPAddrs) != 0 {
		log.Fatalf("use --nsqd-tcp-address or --lookupd-tcp-address not both")
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
