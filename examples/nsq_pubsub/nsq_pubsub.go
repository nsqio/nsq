// This is a client exposes a HTTP streaming interface to NSQ channels

package main

import (
	"../../nsq"
	"../../util"
	"bufio"
	"errors"
	"flag"
	"fmt"
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
	webAddress       = flag.String("web-address", "0.0.0.0:8080", "<addr>:<port> to listen on for HTTP clients")
	buffer           = flag.Int("buffer", 100, "number of messages to buffer in channel for clients")
	nsqAddresses     = util.StringArray{}
	lookupdAddresses = util.StringArray{}
)

func init() {
	flag.Var(&nsqAddresses, "nsq-address", "nsq address (may be given multiple times)")
	flag.Var(&lookupdAddresses, "lookupd-address", "lookupd address (may be given multiple times)")
}

type StreamServer struct {
	sync.RWMutex // embed a r/w mutex
	clients      []*StreamReader
	messageCount uint64
}

var streamServer *StreamServer

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
		addr, _ := net.ResolveTCPAddr("tcp", addrString)
		err := r.ConnectToNSQ(addr)
		if err != nil {
			return err
		}
	}

	for _, addrString := range lookupd {
		log.Printf("lookupd addr %s", addrString)
		addr, _ := net.ResolveTCPAddr("tcp", addrString)
		err := r.ConnectToLookupd(addr)
		if err != nil {
			return err
		}
	}
	return nil
}

func main() {
	flag.Parse()

	if *buffer < 0 {
		log.Fatalf("--buffer must be > 0")
	}

	if len(nsqAddresses) == 0 && len(lookupdAddresses) == 0 {
		log.Fatalf("--nsq-address or --lookupd-address required.")
	}
	if len(nsqAddresses) != 0 && len(lookupdAddresses) != 0 {
		log.Fatalf("use --nsq-address or --lookupd-address not both")
	}

	httpAddr, err := net.ResolveTCPAddr("tcp", *webAddress)
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

func getTopicChannelArgs(rp *util.ReqParams) (string, string, error) {
	topicName, err := rp.Query("topic")
	if err != nil {
		return "", "", errors.New("MISSING_ARG_TOPIC")
	}

	channelName, err := rp.Query("channel")
	if err != nil {
		return "", "", errors.New("MISSING_ARG_CHANNEL")
	}

	return topicName, channelName, nil
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
		log.Printf("ERROR: failed to parse request params - %s", err.Error())
		w.Write(util.ApiResponse(500, "INVALID_REQUEST", nil))
		return
	}

	topicName, channelName, err := getTopicChannelArgs(reqParams)
	if err != nil {
		w.Write(util.ApiResponse(500, err.Error(), nil))
		return
	}

	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "webserver doesn't support hijacking", http.StatusInternalServerError)
		return
	}
	conn, bufrw, err := hj.Hijack()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	r, err := nsq.NewReader(topicName, channelName)

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
		bufrw:       bufrw, // todo: latency writer
		connectTime: time.Now(),
	}
	s.Set(sr)

	log.Printf("new connection from %s", conn.RemoteAddr().String())
	bufrw.WriteString("HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n")
	bufrw.Flush()

	r.AddHandler(sr)
	errors := ConnectToNSQAndLookupd(r, nsqAddresses, lookupdAddresses)
	log.Printf("connected to NSQ %v", errors)

	go func(r *bufio.ReadWriter) {
		b, err := r.ReadByte()
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
	defer func(){
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
