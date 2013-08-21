package nsq

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// returned from ConnectToNSQ() when already connected
var ErrAlreadyConnected = errors.New("already connected")

// return from updateRdy if over max-in-flight
var ErrOverMaxInFlight = errors.New("over configure max-inflight")

// Handler is the synchronous interface to Reader.
//
// Implement this interface for handlers that return whether or not message
// processing completed successfully.
//
// When the return value is nil Reader will automatically handle FINishing.
//
// When the returned value is non-nil Reader will automatically handle REQueing.
type Handler interface {
	HandleMessage(message *Message) error
}

// AsyncHandler is the asynchronous interface to Reader.
//
// Implement this interface for handlers that wish to defer responding until later.
// This is particularly useful if you want to batch work together.
//
// An AsyncHandler must send:
//
//     &FinishedMessage{messageID, requeueDelay, true|false}
//
// To the supplied responseChannel to indicate that a message is processed.
type AsyncHandler interface {
	HandleMessage(message *Message, responseChannel chan *FinishedMessage)
}

// FinishedMessage is the data type used over responseChannel in AsyncHandlers
type FinishedMessage struct {
	Id             MessageID
	RequeueDelayMs int
	Success        bool
}

// FailedMessageLogger is an interface that can be implemented by handlers that wish
// to receive a callback when a message is deemed "failed" (i.e. the number of attempts
// exceeded the Reader specified MaxAttemptCount)
type FailedMessageLogger interface {
	LogFailedMessage(message *Message)
}

type incomingMessage struct {
	*Message
	responseChannel chan *FinishedMessage
}

type nsqConn struct {
	sync.Mutex
	net.Conn

	tlsConn          *tls.Conn
	r                io.Reader
	w                io.Writer
	addr             string
	stopFlag         int32
	finishedMessages chan *FinishedMessage
	messagesInFlight int64
	messagesReceived uint64
	messagesFinished uint64
	messagesRequeued uint64
	maxRdyCount      int64
	rdyCount         int64
	lastRdyCount     int64
	lastMsgTimestamp int64
	readTimeout      time.Duration
	writeTimeout     time.Duration
	stopper          sync.Once
	dying            chan int
	drainReady       chan int
	readyChan        chan int
	exitChan         chan int
	backoffCounter   int32
	rdyRetryTimer    *time.Timer
}

func newNSQConn(addr string, readTimeout time.Duration, writeTimeout time.Duration) (*nsqConn, error) {
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return nil, err
	}

	nc := &nsqConn{
		Conn:             conn,
		r:                bufio.NewReader(conn),
		w:                conn,
		addr:             addr,
		finishedMessages: make(chan *FinishedMessage),
		readTimeout:      readTimeout,
		writeTimeout:     writeTimeout,
		dying:            make(chan int, 1),
		drainReady:       make(chan int),
		readyChan:        make(chan int, 1),
		exitChan:         make(chan int),
		maxRdyCount:      2500,
		lastMsgTimestamp: time.Now().UnixNano(),
	}

	_, err = nc.Write(MagicV2)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("[%s] failed to write magic - %s", addr, err.Error())
	}

	return nc, nil
}

func (c *nsqConn) String() string {
	return c.addr
}

func (c *nsqConn) Read(p []byte) (int, error) {
	c.SetReadDeadline(time.Now().Add(c.readTimeout))
	return c.r.Read(p)
}

func (c *nsqConn) Write(p []byte) (int, error) {
	c.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	return c.w.Write(p)
}

func (c *nsqConn) sendCommand(buf *bytes.Buffer, cmd *Command) error {
	c.Lock()
	defer c.Unlock()

	buf.Reset()
	err := cmd.Write(buf)
	if err != nil {
		return err
	}

	_, err = buf.WriteTo(c)
	if err != nil {
		return err
	}

	return nil
}

func (c *nsqConn) readUnpackedResponse() (int32, []byte, error) {
	resp, err := ReadResponse(c)
	if err != nil {
		return -1, nil, err
	}
	return UnpackResponse(resp)
}

func (c *nsqConn) upgradeTLS(conf *tls.Config) error {
	c.tlsConn = tls.Client(c.Conn, conf)
	err := c.tlsConn.Handshake()
	if err != nil {
		return err
	}
	c.r = bufio.NewReader(c.tlsConn)
	c.w = c.tlsConn
	frameType, data, err := c.readUnpackedResponse()
	if err != nil {
		return err
	}
	if frameType != FrameTypeResponse || !bytes.Equal(data, []byte("OK")) {
		return errors.New("invalid response from TLS upgrade")
	}
	return nil
}

func (c *nsqConn) tryUpdateRDY() {
	select {
	case c.readyChan <- 1:
	default:
	}
}

// Reader is a high-level type to consume from NSQ.
//
// A Reader instance is supplied handler(s) that will be executed
// concurrently via goroutines to handle processing the stream of messages
// consumed from the specified topic/channel.  See: AsyncHandler and Handler
// for details on implementing those interfaces to create handlers.
//
// If configured, it will poll nsqlookupd instances and handle connection (and
// reconnection) to any discovered nsqds.
type Reader struct {
	sync.RWMutex

	TopicName           string        // name of topic to subscribe to
	ChannelName         string        // name of channel to subscribe to
	LookupdPollInterval time.Duration // duration between polling lookupd for new connections
	LookupdPollJitter   float64       // Maximum fractional amount of jitter to add to the lookupd pool loop. This helps evenly distribute requests even if multiple consumers restart at the same time.
	MaxAttemptCount     uint16        // maximum number of times this reader will attempt to process a message
	DefaultRequeueDelay time.Duration // the default duration when REQueueing
	MaxRequeueDelay     time.Duration // the maximum duration when REQueueing (for doubling backoff)
	LowRdyIdleTimeout   time.Duration // the amount of time in seconds to wait for a message from a producer when in a state where RDY counts are re-distributed (ie. max_in_flight < num_producers)
	VerboseLogging      bool          // enable verbose logging
	ShortIdentifier     string        // an identifier to send to nsqd when connecting (defaults: short hostname)
	LongIdentifier      string        // an identifier to send to nsqd when connecting (defaults: long hostname)
	ReadTimeout         time.Duration // the deadline set for network reads
	WriteTimeout        time.Duration // the deadline set for network writes
	MessagesReceived    uint64        // an atomic counter - # of messages received
	MessagesFinished    uint64        // an atomic counter - # of messages FINished
	MessagesRequeued    uint64        // an atomic counter - # of messages REQueued
	ExitChan            chan int      // read from this channel to block your main loop
	TLSv1               bool          // negotiate enabling TLS
	TLSConfig           *tls.Config   // client TLS configuration

	// internal variables
	totalRdyCount      int64
	redistributeOnce   sync.Once
	maxBackoffDuration time.Duration
	maxBackoffCount    int32
	maxInFlight        int
	incomingMessages   chan *incomingMessage
	pendingConnections map[string]bool
	nsqConnections     map[string]*nsqConn
	lookupdExitChan    chan int
	lookupdRecheckChan chan int
	stopFlag           int32
	runningHandlers    int32
	messagesInFlight   int64
	lookupdHTTPAddrs   []string
	stopHandler        sync.Once
	lookupdQueryIndex  int
}

// NewReader creates a new instance of Reader for the specified topic/channel
//
// The returned Reader instance is setup with sane default values.  To modify
// configuration, update the values on the returned instance before connecting.
func NewReader(topic string, channel string) (*Reader, error) {
	if !IsValidTopicName(topic) {
		return nil, errors.New("invalid topic name")
	}

	if !IsValidChannelName(channel) {
		return nil, errors.New("invalid channel name")
	}

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("ERROR: unable to get hostname %s", err.Error())
	}
	q := &Reader{
		TopicName:           topic,
		ChannelName:         channel,
		incomingMessages:    make(chan *incomingMessage),
		ExitChan:            make(chan int),
		pendingConnections:  make(map[string]bool),
		nsqConnections:      make(map[string]*nsqConn),
		MaxAttemptCount:     5,
		LookupdPollInterval: 60 * time.Second,
		LookupdPollJitter:   0.3,
		LowRdyIdleTimeout:   10 * time.Second,
		lookupdExitChan:     make(chan int),
		lookupdRecheckChan:  make(chan int, 1), // used at connection close to force a possible reconnect
		DefaultRequeueDelay: 90 * time.Second,
		MaxRequeueDelay:     15 * time.Minute,
		ShortIdentifier:     strings.Split(hostname, ".")[0],
		LongIdentifier:      hostname,
		ReadTimeout:         DefaultClientTimeout,
		WriteTimeout:        time.Second,
		maxInFlight:         1,
	}
	q.SetMaxBackoffDuration(120 * time.Second)
	return q, nil
}

// ConnectionMaxInFlight calculates the per-connection max-in-flight count.
//
// This may change dynamically based on the number of connections to nsqd the Reader
// is responsible for.
func (q *Reader) ConnectionMaxInFlight() int64 {
	q.RLock()
	defer q.RUnlock()

	b := float64(q.maxInFlight)
	s := b / float64(len(q.nsqConnections))
	return int64(math.Min(math.Max(1, s), b))
}

// IsStarved indicates whether any connections for this reader are blocked on processing
// before being able to receive more messages (ie. RDY count of 0 and not exiting)
func (q *Reader) IsStarved() bool {
	q.RLock()
	defer q.RUnlock()

	for _, conn := range q.nsqConnections {
		threshold := int64(float64(atomic.LoadInt64(&conn.lastRdyCount)) * 0.85)
		inFlight := atomic.LoadInt64(&conn.messagesInFlight)
		if inFlight >= threshold && inFlight > 0 && atomic.LoadInt32(&conn.stopFlag) != 1 {
			return true
		}
	}
	return false
}

// SetMaxInFlight sets the maximum number of messages this reader instance
// will allow in-flight.
//
// If already connected, it updates the reader RDY state for each connection.
func (q *Reader) SetMaxInFlight(maxInFlight int) {
	if atomic.LoadInt32(&q.stopFlag) == 1 {
		return
	}

	if q.maxInFlight == maxInFlight {
		return
	}
	q.maxInFlight = maxInFlight

	q.RLock()
	defer q.RUnlock()

	for _, c := range q.nsqConnections {
		c.tryUpdateRDY()
	}
}

// SetMaxBackoffDuration sets the maximum duration a connection will backoff from message processing
func (q *Reader) SetMaxBackoffDuration(duration time.Duration) {
	q.maxBackoffDuration = duration
	q.maxBackoffCount = int32(math.Max(1, math.Ceil(math.Log2(duration.Seconds()))))
}

// MaxInFlight returns the configured maximum number of messages to allow in-flight.
func (q *Reader) MaxInFlight() int {
	return q.maxInFlight
}

// ConnectToLookupd adds a nsqlookupd address to the list for this Reader instance.
//
// If it is the first to be added, it initiates an HTTP request to discover nsqd
// producers for the configured topic.
//
// A goroutine is spawned to handle continual polling.
func (q *Reader) ConnectToLookupd(addr string) error {
	// make a HTTP req to the lookupd, and ask it for endpoints that have the
	// topic we are interested in.
	// this is a go loop that fires every x seconds
	for _, x := range q.lookupdHTTPAddrs {
		if x == addr {
			return errors.New("lookupd address already exists")
		}
	}
	q.lookupdHTTPAddrs = append(q.lookupdHTTPAddrs, addr)

	// if this is the first one, kick off the go loop
	if len(q.lookupdHTTPAddrs) == 1 {
		q.queryLookupd()
		go q.lookupdLoop()
	}

	return nil
}

// poll all known lookup servers every LookupdPollInterval
func (q *Reader) lookupdLoop() {
	// add some jitter so that multiple consumers discovering the same topic,
	// when restarted at the same time, dont all connect at once.
	rand.Seed(time.Now().UnixNano())

	jitter := time.Duration(int64(rand.Float64() * q.LookupdPollJitter * float64(q.LookupdPollInterval)))
	ticker := time.NewTicker(q.LookupdPollInterval)

	select {
	case <-time.After(jitter):
	case <-q.lookupdExitChan:
		goto exit
	}

	for {
		select {
		case <-ticker.C:
			q.queryLookupd()
		case <-q.lookupdRecheckChan:
			q.queryLookupd()
		case <-q.lookupdExitChan:
			goto exit
		}
	}

exit:
	ticker.Stop()
	log.Printf("exiting lookupdLoop")
}

// make a HTTP req to the /lookup endpoint on one lookup server
// to find what nsq's provide the topic we are consuming.
// for any new topics, initiate a connection to those NSQ's
func (q *Reader) queryLookupd() {
	i := q.lookupdQueryIndex

	if i >= len(q.lookupdHTTPAddrs) {
		i = 0
	}
	q.lookupdQueryIndex += 1

	addr := q.lookupdHTTPAddrs[i]
	endpoint := fmt.Sprintf("http://%s/lookup?topic=%s", addr, url.QueryEscape(q.TopicName))

	log.Printf("LOOKUPD: querying %s", endpoint)

	data, err := ApiRequest(endpoint)
	if err != nil {
		log.Printf("ERROR: lookupd %s - %s", addr, err.Error())
		return
	}

	// {"data":{"channels":[],"producers":[{"address":"jehiah-air.local", "tpc_port":4150, "http_port":4151}],"timestamp":1340152173},"status_code":200,"status_txt":"OK"}
	producers, _ := data.Get("producers").Array()
	for _, producer := range producers {
		producerData, _ := producer.(map[string]interface{})
		address := producerData["address"].(string)
		broadcastAddress, ok := producerData["broadcast_address"]
		if ok {
			address = broadcastAddress.(string)
		}
		port := int(producerData["tcp_port"].(float64))

		// make an address, start a connection
		joined := net.JoinHostPort(address, strconv.Itoa(port))
		err = q.ConnectToNSQ(joined)
		if err != nil && err != ErrAlreadyConnected {
			log.Printf("ERROR: failed to connect to nsqd (%s) - %s", joined, err.Error())
			continue
		}
	}
}

// ConnectToNSQ takes a nsqd address to connect directly to.
//
// It is recommended to use ConnectToLookupd so that topics are discovered
// automatically.  This method is useful when you want to connect to a single, local,
// instance.
func (q *Reader) ConnectToNSQ(addr string) error {
	var buf bytes.Buffer

	if atomic.LoadInt32(&q.stopFlag) == 1 {
		return errors.New("reader stopped")
	}

	if atomic.LoadInt32(&q.runningHandlers) == 0 {
		return errors.New("no handlers")
	}

	q.RLock()
	_, ok := q.nsqConnections[addr]
	_, pendingOk := q.pendingConnections[addr]
	if ok || pendingOk {
		q.RUnlock()
		return ErrAlreadyConnected
	}
	q.RUnlock()

	q.redistributeOnce.Do(func() { go q.redistributeRdyState() })

	log.Printf("[%s] connecting to nsqd", addr)

	connection, err := newNSQConn(addr, q.ReadTimeout, q.WriteTimeout)
	if err != nil {
		return err
	}
	cleanupConnection := func() {
		q.Lock()
		delete(q.pendingConnections, addr)
		q.Unlock()
		connection.Close()
	}
	q.pendingConnections[addr] = true

	ci := make(map[string]interface{})
	ci["short_id"] = q.ShortIdentifier
	ci["long_id"] = q.LongIdentifier
	ci["tls_v1"] = q.TLSv1
	ci["feature_negotiation"] = true
	cmd, err := Identify(ci)
	if err != nil {
		cleanupConnection()
		return fmt.Errorf("[%s] failed to create identify command - %s", connection, err.Error())
	}

	err = connection.sendCommand(&buf, cmd)
	if err != nil {
		cleanupConnection()
		return fmt.Errorf("[%s] failed to identify - %s", connection, err.Error())
	}

	_, data, err := connection.readUnpackedResponse()
	if err != nil {
		cleanupConnection()
		return fmt.Errorf("[%s] error reading response %s", connection, err.Error())
	}

	// check to see if the server was able to respond w/ capabilities
	if data[0] == '{' {
		resp := struct {
			MaxRdyCount int64 `json:"max_rdy_count"`
			TLSv1       bool  `json:"tls_v1"`
		}{}
		err := json.Unmarshal(data, &resp)
		if err != nil {
			cleanupConnection()
			return fmt.Errorf("[%s] error (%s) unmarshaling IDENTIFY response %s", connection, err.Error(), data)
		}

		log.Printf("[%s] IDENTIFY response: %+v", connection, resp)

		connection.maxRdyCount = resp.MaxRdyCount
		if resp.MaxRdyCount < int64(q.maxInFlight) {
			log.Printf("[%s] max RDY count %d < reader max in flight %d, truncation possible",
				connection, resp.MaxRdyCount, q.maxInFlight)
		}

		if resp.TLSv1 {
			log.Printf("[%s] upgrading to TLS", connection)
			err := connection.upgradeTLS(q.TLSConfig)
			if err != nil {
				cleanupConnection()
				return fmt.Errorf("[%s] error (%s) upgrading to TLS", connection, err.Error())
			}
		}
	}

	cmd = Subscribe(q.TopicName, q.ChannelName)
	err = connection.sendCommand(&buf, cmd)
	if err != nil {
		cleanupConnection()
		return fmt.Errorf("[%s] failed to subscribe to %s:%s - %s", connection, q.TopicName, q.ChannelName, err.Error())
	}

	q.Lock()
	delete(q.pendingConnections, addr)
	q.nsqConnections[connection.String()] = connection
	// signal to existing connections to lower their RDY count
	for _, c := range q.nsqConnections {
		c.tryUpdateRDY()
	}
	q.Unlock()

	go q.readLoop(connection)
	go q.finishLoop(connection)
	go q.rdyLoop(connection)

	// prime RDY state
	connection.tryUpdateRDY()

	return nil
}

func handleError(q *Reader, c *nsqConn, errMsg string) {
	log.Printf(errMsg)
	atomic.StoreInt32(&c.stopFlag, 1)

	if len(q.lookupdHTTPAddrs) == 0 {
		go func(addr string) {
			for {
				log.Printf("[%s] re-connecting in 15 seconds...", addr)
				time.Sleep(15 * time.Second)
				if atomic.LoadInt32(&q.stopFlag) == 1 {
					break
				}
				err := q.ConnectToNSQ(addr)
				if err != nil {
					log.Printf("ERROR: failed to connect to %s - %s", addr, err.Error())
					continue
				}
				break
			}
		}(c.RemoteAddr().String())
	}
}

func (q *Reader) readLoop(c *nsqConn) {
	for {
		if atomic.LoadInt32(&c.stopFlag) == 1 || atomic.LoadInt32(&q.stopFlag) == 1 {
			// start the connection close
			if atomic.LoadInt64(&c.messagesInFlight) == 0 {
				q.stopFinishLoop(c)
			} else {
				log.Printf("[%s] delaying close of FinishedMesages channel; %d outstanding messages", c, c.messagesInFlight)
			}
			goto exit
		}

		frameType, data, err := c.readUnpackedResponse()
		if err != nil {
			handleError(q, c, fmt.Sprintf("[%s] error (%s) reading response %d %s", c, err.Error(), frameType, data))
			continue
		}

		switch frameType {
		case FrameTypeMessage:
			msg, err := DecodeMessage(data)
			if err != nil {
				handleError(q, c, fmt.Sprintf("[%s] error (%s) decoding message %s", c, err.Error(), data))
				continue
			}

			remain := atomic.AddInt64(&c.rdyCount, -1)
			atomic.AddInt64(&q.totalRdyCount, -1)
			atomic.AddUint64(&c.messagesReceived, 1)
			atomic.AddUint64(&q.MessagesReceived, 1)
			atomic.AddInt64(&c.messagesInFlight, 1)
			atomic.AddInt64(&q.messagesInFlight, 1)
			atomic.StoreInt64(&c.lastMsgTimestamp, time.Now().UnixNano())

			if q.VerboseLogging {
				log.Printf("[%s] (remain %d) FrameTypeMessage: %s - %s", c, remain, msg.Id, msg.Body)
			}

			q.incomingMessages <- &incomingMessage{msg, c.finishedMessages}

			c.tryUpdateRDY()
		case FrameTypeResponse:
			switch {
			case bytes.Equal(data, []byte("CLOSE_WAIT")):
				// server is ready for us to close (it ack'd our StartClose)
				// we can assume we will not receive any more messages over this channel
				// (but we can still write back responses)
				log.Printf("[%s] received ACK from nsqd - now in CLOSE_WAIT", c)
				atomic.StoreInt32(&c.stopFlag, 1)
			case bytes.Equal(data, []byte("_heartbeat_")):
				var buf bytes.Buffer
				log.Printf("[%s] received heartbeat from nsqd", c)
				err := c.sendCommand(&buf, Nop())
				if err != nil {
					handleError(q, c, fmt.Sprintf("[%s] error sending NOP - %s", c, err.Error()))
					goto exit
				}
			}
		case FrameTypeError:
			log.Printf("[%s] error from nsqd %s", c, data)
		default:
			log.Printf("[%s] unknown message type %d", c, frameType)
		}
	}

exit:
	log.Printf("[%s] readLoop exiting", c)
}

func (q *Reader) finishLoop(c *nsqConn) {
	var buf bytes.Buffer
	var backoffCounter int32
	var backoffUpdated bool
	var backoffDeadline time.Time

	for {
		select {
		case <-c.dying:
			log.Printf("[%s] breaking out of finish loop ", c)
			// Indicate drainReady because we will not pull any more off finishedMessages
			c.drainReady <- 1
			goto exit
		case msg := <-c.finishedMessages:
			// Decrement this here so it is correct even if we can't respond to nsqd
			atomic.AddInt64(&q.messagesInFlight, -1)
			atomic.AddInt64(&c.messagesInFlight, -1)
			now := time.Now()

			if msg.Success {
				if q.VerboseLogging {
					log.Printf("[%s] finishing %s", c, msg.Id)
				}

				err := c.sendCommand(&buf, Finish(msg.Id))
				if err != nil {
					log.Printf("[%s] error finishing %s - %s", c, msg.Id, err.Error())
					q.stopFinishLoop(c)
					continue
				}

				atomic.AddUint64(&c.messagesFinished, 1)
				atomic.AddUint64(&q.MessagesFinished, 1)

				if backoffCounter > 0 && now.After(backoffDeadline) {
					backoffCounter--
					backoffUpdated = true
				}
			} else {
				if q.VerboseLogging {
					log.Printf("[%s] requeuing %s", c, msg.Id)
				}

				err := c.sendCommand(&buf, Requeue(msg.Id, msg.RequeueDelayMs))
				if err != nil {
					log.Printf("[%s] error requeueing %s - %s", c, msg.Id, err.Error())
					q.stopFinishLoop(c)
					continue
				}

				atomic.AddUint64(&c.messagesRequeued, 1)
				atomic.AddUint64(&q.MessagesRequeued, 1)

				if backoffCounter < q.maxBackoffCount && now.After(backoffDeadline) {
					backoffCounter++
					backoffUpdated = true
				}
			}

			atomic.StoreInt32(&c.backoffCounter, backoffCounter)
			// prevent many async failures/successes from immediately resulting in
			// max backoff/normal rate (by ensuring that we dont continually incr/decr
			// the counter during a backoff period)
			if backoffCounter > 0 && backoffUpdated {
				backoffDuration := q.backoffDuration(backoffCounter)
				backoffDeadline = now.Add(backoffDuration)

				// going into backoff. set all RDY to 0 immediately
				q.RLock()
				for _, c := range q.nsqConnections {
					if q.VerboseLogging {
						log.Printf("[%s] in backoff. sending RDY 0", c)
					}
					q.updateRDY(c, 0)

					// prime the rdy loop so it starts waiting on the backoff timer (if not already doing so)
					c.tryUpdateRDY()
				}
				q.RUnlock()
			}

			if atomic.LoadInt64(&c.messagesInFlight) == 0 &&
				(atomic.LoadInt32(&c.stopFlag) == 1 || atomic.LoadInt32(&q.stopFlag) == 1) {
				q.stopFinishLoop(c)
				continue
			}
		}
	}

exit:
	log.Printf("[%s] finishLoop exiting", c)
}

func (q *Reader) stopFinishLoop(c *nsqConn) {
	c.stopper.Do(func() {
		log.Printf("[%s] beginning stopFinishLoop logic", c)
		// This doesn't block because dying has buffer of 1
		c.dying <- 1

		// Drain the finishedMessages channel
		go func() {
			<-c.drainReady
			for atomic.AddInt64(&c.messagesInFlight, -1) >= 0 {
				<-c.finishedMessages
			}
		}()
		close(c.exitChan)
		c.Close()

		atomic.AddInt64(&q.totalRdyCount, atomic.LoadInt64(&c.rdyCount)*-1)

		q.Lock()
		delete(q.nsqConnections, c.String())
		left := len(q.nsqConnections)
		q.Unlock()

		log.Printf("there are %d connections left alive", left)

		// ie: we were the last one, and stopping
		if left == 0 && atomic.LoadInt32(&q.stopFlag) == 1 {
			q.stopHandlers()
		}

		if len(q.lookupdHTTPAddrs) != 0 && atomic.LoadInt32(&q.stopFlag) == 0 {
			// trigger a poll of the lookupd
			select {
			case q.lookupdRecheckChan <- 1:
			default:
			}
		}
	})
}

func (q *Reader) backoffDuration(count int32) time.Duration {
	backoffDuration := time.Second * time.Duration(math.Pow(2, float64(count)))
	if backoffDuration > q.maxBackoffDuration {
		backoffDuration = q.maxBackoffDuration
	}
	return backoffDuration
}

func (q *Reader) rdyLoop(c *nsqConn) {
	readyChan := c.readyChan
	var backoffTimer *time.Timer
	var backoffTimerChan <-chan time.Time

	for {
		select {
		case <-backoffTimerChan:
			log.Printf("[%s] backoff time expired, continuing with RDY 1...", c)
			// while in backoff only ever let 1 message at a time through
			q.updateRDY(c, 1)
			readyChan = c.readyChan
			backoffTimer = nil
		case <-readyChan:
			backoffCounter := atomic.LoadInt32(&c.backoffCounter)

			if backoffCounter != 0 && q.maxBackoffDuration != 0 {
				if backoffTimer != nil {
					// dont overwrite an existing backoff timer
					continue
				}
				backoffDuration := q.backoffDuration(backoffCounter)
				backoffTimer = time.NewTimer(backoffDuration)
				backoffTimerChan = backoffTimer.C
				readyChan = nil
				log.Printf("[%s] backing off for %.02f seconds (backoff level %d)", c, backoffDuration.Seconds(), backoffCounter)
				continue
			}

			// send ready immediately
			remain := atomic.LoadInt64(&c.rdyCount)
			lastRdyCount := atomic.LoadInt64(&c.lastRdyCount)
			count := q.ConnectionMaxInFlight()
			// refill when at 1, or at 25%, or if connections have changed and we have too many RDY
			if remain <= 1 || remain < (lastRdyCount/4) || (count > 0 && count < remain) {
				if q.VerboseLogging {
					log.Printf("[%s] sending RDY %d (%d remain from last RDY %d)", c, count, remain, lastRdyCount)
				}
				q.updateRDY(c, count)
			} else {
				if q.VerboseLogging {
					log.Printf("[%s] skip sending RDY %d (%d remain out of last RDY %d)", c, count, remain, lastRdyCount)
				}
			}
		case <-c.exitChan:
			if backoffTimer != nil {
				backoffTimer.Stop()
			}
			goto exit
		}
	}

exit:
	log.Printf("[%s] rdyLoop exiting", c)
}

func (q *Reader) updateRDY(c *nsqConn, count int64) error {
	if atomic.LoadInt32(&c.stopFlag) != 0 {
		return nil
	}

	// never exceed the nsqd's configured max RDY count
	if count > c.maxRdyCount {
		count = c.maxRdyCount
	}

	// stop any pending retry of an old RDY update
	if c.rdyRetryTimer != nil {
		c.rdyRetryTimer.Stop()
		c.rdyRetryTimer = nil
	}

	// never exceed our global max in flight. truncate if possible.
	// this could help a new connection get partial max-in-flight
	maxPossibleRdy := int64(q.maxInFlight) - atomic.LoadInt64(&q.totalRdyCount) + atomic.LoadInt64(&c.rdyCount)
	if maxPossibleRdy > 0 && maxPossibleRdy < count {
		count = maxPossibleRdy
	}
	if maxPossibleRdy <= 0 && count > 0 {
		if atomic.LoadInt64(&c.rdyCount) == 0 {
			// we wanted a to exit a zero RDY count but we couldn't send it...
			// in order to prevent eternal starvation we reschedule this attempt
			// (if any other RDY update succeeds this timer will be stopped)
			c.rdyRetryTimer = time.AfterFunc(5*time.Second, func() {
				q.updateRDY(c, count)
			})
		}
		return ErrOverMaxInFlight
	}

	return q.sendRDY(c, count)
}

func (q *Reader) sendRDY(c *nsqConn, count int64) error {
	var buf bytes.Buffer

	if count == 0 && atomic.LoadInt64(&c.lastRdyCount) == 0 {
		// no need to send. It's already that RDY count
		return nil
	}

	atomic.AddInt64(&q.totalRdyCount, atomic.LoadInt64(&c.rdyCount)*-1+count)
	atomic.StoreInt64(&c.rdyCount, count)
	atomic.StoreInt64(&c.lastRdyCount, count)
	err := c.sendCommand(&buf, Ready(int(count)))
	if err != nil {
		handleError(q, c, fmt.Sprintf("[%s] error sending RDY %d - %s", c, count, err.Error()))
		return err
	}
	return nil
}

func (q *Reader) redistributeRdyState() {
	for {
		time.Sleep(5 * time.Second)

		q.RLock()
		l := len(q.nsqConnections)
		q.RUnlock()
		if l <= q.maxInFlight {
			continue
		}

		log.Printf("redistributing ready state (%d conns > %d max_in_flight)", l, q.maxInFlight)
		q.RLock()
		possibleConns := make([]*nsqConn, 0, len(q.nsqConnections))
		for _, c := range q.nsqConnections {
			lastMsgTimestamp := atomic.LoadInt64(&c.lastMsgTimestamp)
			lastMsgDuration := time.Now().Sub(time.Unix(0, lastMsgTimestamp))
			rdyCount := atomic.LoadInt64(&c.rdyCount)
			if q.VerboseLogging {
				log.Printf("[%s] rdy: %d (last message received %s)", c, rdyCount, lastMsgDuration)
			}
			if rdyCount > 0 && lastMsgDuration > q.LowRdyIdleTimeout {
				log.Printf("[%s] idle connection, giving up RDY count", c)
				q.updateRDY(c, 0)
			}
			possibleConns = append(possibleConns, c)
		}
		availableMaxInFlight := int64(q.maxInFlight) - atomic.LoadInt64(&q.totalRdyCount)
		for len(possibleConns) > 0 && availableMaxInFlight > 0 {
			availableMaxInFlight--
			i := rand.Int() % len(possibleConns)
			c := possibleConns[i]
			// delete
			possibleConns = append(possibleConns[:i], possibleConns[i+1:]...)
			log.Printf("[%s] redistributing RDY", c)
			q.updateRDY(c, 1)
		}
		q.RUnlock()
	}
}

// Stop will gracefully stop the Reader
func (q *Reader) Stop() {
	var buf bytes.Buffer

	if !atomic.CompareAndSwapInt32(&q.stopFlag, 0, 1) {
		return
	}

	log.Printf("stopping reader")

	q.RLock()
	l := len(q.nsqConnections)
	q.RUnlock()

	if l == 0 {
		q.stopHandlers()
	} else {
		q.RLock()
		for _, c := range q.nsqConnections {
			err := c.sendCommand(&buf, StartClose())
			if err != nil {
				log.Printf("[%s] failed to start close - %s", c, err.Error())
			}
		}
		q.RUnlock()

		go func() {
			<-time.After(time.Duration(30) * time.Second)
			q.stopHandlers()
		}()
	}

	if len(q.lookupdHTTPAddrs) != 0 {
		q.lookupdExitChan <- 1
	}
}

func (q *Reader) stopHandlers() {
	q.stopHandler.Do(func() {
		log.Printf("closing incomingMessages")
		close(q.incomingMessages)
	})
}

// AddHandler adds a Handler for messages received by this Reader.
//
// See Handler for details on implementing this interface.
//
// It's ok to start more than one handler simultaneously, they
// are concurrently executed in goroutines.
func (q *Reader) AddHandler(handler Handler) {
	atomic.AddInt32(&q.runningHandlers, 1)
	log.Println("starting Handler go-routine")
	go func() {
		for {
			message, ok := <-q.incomingMessages
			if !ok {
				log.Printf("closing Handler (after self.incomingMessages closed)")
				if atomic.AddInt32(&q.runningHandlers, -1) == 0 {
					q.ExitChan <- 1
				}
				break
			}

			err := handler.HandleMessage(message.Message)
			if err != nil {
				log.Printf("ERROR: handler returned %s for msg %s %s", err.Error(), message.Id, message.Body)
			}

			// message passed the max number of attempts
			if err != nil && q.MaxAttemptCount > 0 && message.Attempts > q.MaxAttemptCount {
				log.Printf("WARNING: msg attempted %d times. giving up %s %s", message.Attempts, message.Id, message.Body)
				logger, ok := handler.(FailedMessageLogger)
				if ok {
					logger.LogFailedMessage(message.Message)
				}
				message.responseChannel <- &FinishedMessage{message.Id, 0, true}
				continue
			}

			// linear delay
			requeueDelay := q.DefaultRequeueDelay * time.Duration(message.Attempts)
			// bound the requeueDelay to configured max
			if requeueDelay > q.MaxRequeueDelay {
				requeueDelay = q.MaxRequeueDelay
			}

			message.responseChannel <- &FinishedMessage{message.Id, int(requeueDelay / time.Millisecond), err == nil}
		}
	}()
}

// AddAsyncHandler adds an AsyncHandler for messages received by this Reader.
//
// See AsyncHandler for details on implementing this interface.
//
// It's ok to start more than one handler simultaneously, they
// are concurrently executed in goroutines.
func (q *Reader) AddAsyncHandler(handler AsyncHandler) {
	atomic.AddInt32(&q.runningHandlers, 1)
	log.Println("starting AsyncHandler go-routine")
	go func() {
		for {
			message, ok := <-q.incomingMessages
			if !ok {
				log.Printf("closing AsyncHandler (after self.incomingMessages closed)")
				if atomic.AddInt32(&q.runningHandlers, -1) == 0 {
					q.ExitChan <- 1
				}
				break
			}

			// message passed the max number of attempts
			// note: unfortunately it's not straight forward to do this after passing to async handler, so we don't.
			if q.MaxAttemptCount > 0 && message.Attempts > q.MaxAttemptCount {
				log.Printf("WARNING: msg attempted %d times. giving up %s %s", message.Attempts, message.Id, message.Body)
				logger, ok := handler.(FailedMessageLogger)
				if ok {
					logger.LogFailedMessage(message.Message)
				}
				message.responseChannel <- &FinishedMessage{message.Id, 0, true}
				continue
			}

			handler.HandleMessage(message.Message, message.responseChannel)
		}
	}()
}
