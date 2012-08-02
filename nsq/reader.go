package nsq

import (
	"bitly/simplejson"
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var ErrNoHandlers = errors.New("no handlers")
var ErrAlreadyConnected = errors.New("already connected")
var ErrReaderStopped = errors.New("reader stopped")

// a syncronous handler that returns an error (or nil to indicate success)
type Handler interface {
	HandleMessage(message *Message) error
}

type FailedMessageLogger interface {
	LogFailedMessage(message *Message)
}

// an async handler that must send a &FinishedMessage{messageID, requeueDelay, true|false} onto 
// responseChannel to indicate that a message has been finished. This is usefull 
// if you want to batch work together and delay response that processing is complete
type AsyncHandler interface {
	HandleMessage(message *Message, responseChannel chan *FinishedMessage)
}

type incomingMessage struct {
	*Message
	responseChannel chan *FinishedMessage
}

type FinishedMessage struct {
	Id             []byte
	RequeueDelayMs int
	Success        bool
}

type nsqConn struct {
	net.Conn
	stopFlag            int32
	finishedMessages    chan *FinishedMessage
	messagesInFlight    int64
	messagesReceived    uint64
	messagesFinished    uint64
	messagesRequeued    uint64
	bufferSizeRemaining int64
	readTimeout         time.Duration
	writeTimeout        time.Duration
}

func newNSQConn(addr *net.TCPAddr, readTimeout time.Duration, writeTimeout time.Duration) (*nsqConn, error) {
	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		return nil, err
	}

	nc := &nsqConn{
		net.Conn:         conn,
		finishedMessages: make(chan *FinishedMessage),
		readTimeout:      readTimeout,
		writeTimeout:     writeTimeout,
	}

	nc.SetWriteDeadline(time.Now().Add(nc.writeTimeout))
	_, err = nc.Write(MagicV2)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("[%s] failed to write magic - %s", addr, err.Error())
	}

	return nc, nil
}

func (c *nsqConn) String() string {
	return c.RemoteAddr().String()
}

func (c *nsqConn) sendCommand(cmd *Command) error {
	c.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	return SendCommand(c, cmd)
}

func (c *nsqConn) readResponse() ([]byte, error) {
	c.SetReadDeadline(time.Now().Add(c.readTimeout))
	return ReadResponse(c)
}

type Reader struct {
	TopicName           string        // Name of Topic to subscribe to
	ChannelName         string        // Named Channel to consume messages from
	BufferSize          int           // number of messages to allow in-flight at a time
	LookupdPollInterval time.Duration // seconds between polling lookupd's (+/- random 1/10th this value)
	MaxAttemptCount     uint16
	DefaultRequeueDelay time.Duration
	VerboseLogging      bool
	ShortIdentifier     string // an identifier to send to nsqd when connecting (defaults: short hostname)
	LongIdentifier      string // an identifier to send to nsqd when connecting (defaults: long hostname)
	ReadTimeout         time.Duration
	WriteTimeout        time.Duration
	MessagesReceived    uint64
	MessagesFinished    uint64
	MessagesRequeued    uint64
	ExitChan            chan int

	incomingMessages   chan *incomingMessage
	nsqConnections     map[string]*nsqConn
	lookupdExitChan    chan int
	lookupdRecheckChan chan int
	stopFlag           int32
	runningHandlers    int32
	messagesInFlight   int64
	lookupdAddresses   []*net.TCPAddr
	stopHandler        sync.Once
}

func NewReader(topic string, channel string) (*Reader, error) {
	if len(topic) == 0 || len(topic) > MaxTopicNameLength {
		return nil, errors.New("INVALID_TOPIC_NAME")
	}
	if len(channel) == 0 || len(channel) > MaxChannelNameLength {
		return nil, errors.New("INVALID_CHANNEL_NAME")
	}

	hostname, _ := os.Hostname()
	q := &Reader{
		TopicName:           topic,
		ChannelName:         channel,
		incomingMessages:    make(chan *incomingMessage),
		ExitChan:            make(chan int),
		nsqConnections:      make(map[string]*nsqConn),
		BufferSize:          1,
		MaxAttemptCount:     5,
		LookupdPollInterval: 120 * time.Second,
		lookupdExitChan:     make(chan int),
		lookupdRecheckChan:  make(chan int), // used at connection close to force a possible reconnect
		DefaultRequeueDelay: 90 * time.Second,
		ShortIdentifier:     strings.Split(hostname, ".")[0],
		LongIdentifier:      hostname,
		ReadTimeout:         time.Second,
		WriteTimeout:        time.Second,
	}
	return q, nil
}

// get an appropriate buffer size for a single connection
// essentially the global buffer size distributed amongst # connection
// this may change dynamically based on the number of NSQD connection
func (q *Reader) ConnectionBufferSize() int {
	b := float64(q.BufferSize)
	s := b / (float64(len(q.nsqConnections)) * .75)
	return int(math.Min(math.Max(1, s), b))
}

func (q *Reader) ConnectToLookupd(addr *net.TCPAddr) error {
	// make a HTTP req to the lookupd, and ask it for endpoints that have the
	// topic we are interested in.
	// this is a go loop that fires every x seconds
	for _, x := range q.lookupdAddresses {
		if x == addr {
			return errors.New("address already exists")
		}
	}
	q.lookupdAddresses = append(q.lookupdAddresses, addr)

	// if this is the first one, kick off the go loop
	if len(q.lookupdAddresses) == 1 {
		q.queryLookupd()
		go q.lookupdLoop()
	}
	return nil
}

// poll all known lookup servers every LookupdPollInterval
func (q *Reader) lookupdLoop() {
	rand.Seed(time.Now().UnixNano())
	time.Sleep(time.Duration(rand.Int63n(int64(q.LookupdPollInterval / 10))))
	ticker := time.Tick(q.LookupdPollInterval)
	for {
		select {
		case <-ticker:
			q.queryLookupd()
		case <-q.lookupdRecheckChan:
			q.queryLookupd()
		case <-q.lookupdExitChan:
			return
		}
	}
}

// make a HTTP req to the /lookup endpoint on each lookup server
// to find what nsq's provide the topic we are consuming.
// for any new topics, initiate a connection to those NSQ's
func (q *Reader) queryLookupd() {
	httpclient := &http.Client{}
	for _, addr := range q.lookupdAddresses {
		endpoint := fmt.Sprintf("http://%s/lookup?topic=%s", addr, url.QueryEscape(q.TopicName))

		log.Printf("LOOKUPD: querying %s", endpoint)
		req, err := http.NewRequest("GET", endpoint, nil)
		if err != nil {
			log.Printf("ERROR: lookupd %s - %s", addr, err.Error())
			continue
		}

		resp, err := httpclient.Do(req)
		if err != nil {
			log.Printf("ERROR: lookupd %s - %s", addr, err.Error())
			continue
		}

		body, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			log.Printf("ERROR: lookupd %s - %s", addr, err.Error())
			continue
		}

		data, err := simplejson.NewJson(body)
		if err != nil {
			log.Printf("ERROR: lookupd %s - %s", addr, err.Error())
			continue
		}

		statusCode, err := data.Get("status_code").Int()
		if err != nil || statusCode != 200 {
			log.Printf("ERROR: lookupd %s - invalid status code", addr)
			continue
		}

		// do something with the data
		// {"data":{"channels":[],"producers":[{"address":"jehiah-air.local","port":"4150"}],"timestamp":1340152173},"status_code":200,"status_txt":"OK"}
		producers, _ := data.Get("data").Get("producers").Array()
		for _, producer := range producers {
			producerData, _ := producer.(map[string]interface{})
			address := producerData["address"].(string)
			port := int(producerData["port"].(float64))

			// make an address, start a connection
			joined := net.JoinHostPort(address, fmt.Sprintf("%d", port))
			nsqAddr, err := net.ResolveTCPAddr("tcp", joined)
			if err != nil {
				log.Printf("ERROR: could not resolve tcp address (%s) from lookupd - %s", joined, err.Error())
				continue
			}

			err = q.ConnectToNSQ(nsqAddr)
			if err != nil && err != ErrAlreadyConnected {
				log.Printf("ERROR: failed to connect to nsqd (%s) - %s", nsqAddr.String(), err.Error())
				continue
			}
		}
	}
}

func (q *Reader) ConnectToNSQ(addr *net.TCPAddr) error {
	if atomic.LoadInt32(&q.stopFlag) == 1 {
		return ErrReaderStopped
	}

	if atomic.LoadInt32(&q.runningHandlers) == 0 {
		return ErrNoHandlers
	}

	_, ok := q.nsqConnections[addr.String()]
	if ok {
		return ErrAlreadyConnected
	}

	log.Printf("[%s] connecting to nsqd", addr)

	connection, err := newNSQConn(addr, q.ReadTimeout, q.WriteTimeout)
	if err != nil {
		return err
	}

	err = connection.sendCommand(Subscribe(q.TopicName, q.ChannelName, q.ShortIdentifier, q.LongIdentifier))
	if err != nil {
		connection.Close()
		return fmt.Errorf("[%s] failed to subscribe to %s:%s - %s", q.TopicName, q.ChannelName, err.Error())
	}

	q.nsqConnections[addr.String()] = connection

	go q.readLoop(connection)
	go q.finishLoop(connection)

	return nil
}

func handleReadError(q *Reader, c *nsqConn, errMsg string) {
	log.Printf(errMsg)
	atomic.StoreInt32(&c.stopFlag, 1)
	if len(q.nsqConnections) == 1 && len(q.lookupdAddresses) == 0 {
		// This is the only remaining connection, so stop the queue
		atomic.StoreInt32(&q.stopFlag, 1)
	}
}

func (q *Reader) readLoop(c *nsqConn) {
	// prime our ready state
	s := q.ConnectionBufferSize()
	if q.VerboseLogging {
		log.Printf("[%s] RDY %d", c, s)
	}
	atomic.StoreInt64(&c.bufferSizeRemaining, int64(s))

	err := c.sendCommand(Ready(s))
	if err != nil {
		// even though this is a *write* that failed, this function does all the work we'd
		// need to do anyway
		handleReadError(q, c, fmt.Sprintf("[%s] failed to send initial ready - %s", c, err.Error()))
		stopFinishLoop(c)
		return
	}

	for {
		if atomic.LoadInt32(&c.stopFlag) == 1 || atomic.LoadInt32(&q.stopFlag) == 1 {
			// start the connection close
			if atomic.LoadInt64(&c.messagesInFlight) == 0 {
				stopFinishLoop(c)
			} else {
				log.Printf("[%s] delaying close of FinishedMesages channel; %d outstanding messages", c, c.messagesInFlight)
			}
			log.Printf("[%s] stopped read loop ", c)
			break
		}

		resp, err := c.readResponse()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				// TODO: until we have heartbeats, continue indefinitely
				continue
			}
			handleReadError(q, c, fmt.Sprintf("[%s] error reading response %s", c, err.Error()))
			continue
		}

		frameType, data, err := UnpackResponse(resp)
		if err != nil {
			handleReadError(q, c, fmt.Sprintf("[%s] error (%s) unpacking response %s %s", c, err.Error(), frameType, data))
			continue
		}

		switch frameType {
		case FrameTypeMessage:
			msg, err := DecodeMessage(data)
			if err != nil {
				handleReadError(q, c, fmt.Sprintf("[%s] error (%s) decoding message %s", c, err.Error(), data))
				continue
			}

			remain := atomic.AddInt64(&c.bufferSizeRemaining, -1)
			atomic.AddUint64(&c.messagesReceived, 1)
			atomic.AddUint64(&q.MessagesReceived, 1)
			atomic.AddInt64(&c.messagesInFlight, 1)
			atomic.AddInt64(&q.messagesInFlight, 1)

			if q.VerboseLogging {
				log.Printf("[%s] (remain %d) FrameTypeMessage: %s - %s", c, remain, msg.Id, msg.Body)
			}

			q.incomingMessages <- &incomingMessage{msg, c.finishedMessages}
		case FrameTypeResponse:
			if bytes.Equal(data, []byte("CLOSE_WAIT")) {
				// server is ready for us to close (it ack'd our StartClose)
				// we can assume we will not receive any more messages over this channel
				// (but we can still write back responses)
				log.Printf("[%s] received ACK from server. now in CloseWait %d", c, frameType)
				atomic.StoreInt32(&c.stopFlag, 1)
			}
		default:
			log.Printf("[%s] unknown message type %d", c, frameType)
		}
	}
}

func (q *Reader) finishLoop(c *nsqConn) {
	for {
		msg, ok := <-c.finishedMessages
		if !ok {
			log.Printf("[%s] stopping finish loop ", c)
			c.Close()
			delete(q.nsqConnections, c.String())

			log.Printf("there are %d connections left alive", len(q.nsqConnections))

			if len(q.nsqConnections) == 0 && len(q.lookupdAddresses) == 0 {
				// no lookupd entry means no reconnection
				if atomic.LoadInt32(&q.stopFlag) == 1 {
					q.stopHandlers()
				}
				return
			}

			// ie: we were the last one, and stopping
			if len(q.nsqConnections) == 0 && atomic.LoadInt32(&q.stopFlag) == 1 {
				q.stopHandlers()
			}

			if len(q.lookupdAddresses) != 0 && atomic.LoadInt32(&q.stopFlag) == 0 {
				// trigger a poll of the lookupd
				q.lookupdRecheckChan <- 1
			}
			return
		}

		if msg.Success {
			if q.VerboseLogging {
				log.Printf("[%s] finishing %s", c, msg.Id)
			}
			err := c.sendCommand(Finish(msg.Id))
			if err != nil {
				log.Printf("[%s] error finishing %s - %s", c, msg.Id, err.Error())
				stopFinishLoop(c)
				continue
			}
			atomic.AddUint64(&c.messagesFinished, 1)
			atomic.AddUint64(&q.MessagesFinished, 1)
		} else {
			if q.VerboseLogging {
				log.Printf("[%s] requeuing %s", c, msg.Id)
			}
			err := c.sendCommand(Requeue(msg.Id, msg.RequeueDelayMs))
			if err != nil {
				log.Printf("[%s] error requeueing %s - %s", c, msg.Id, err.Error())
				stopFinishLoop(c)
				continue
			}
			atomic.AddUint64(&c.messagesRequeued, 1)
			atomic.AddUint64(&q.MessagesRequeued, 1)
		}

		atomic.AddInt64(&q.messagesInFlight, -1)
		if atomic.AddInt64(&c.messagesInFlight, -1) == 0 && atomic.LoadInt32(&c.stopFlag) == 1 {
			stopFinishLoop(c)
			continue
		}

		if atomic.LoadInt32(&c.stopFlag) == 0 {
			remain := atomic.LoadInt64(&c.bufferSizeRemaining)
			s := q.ConnectionBufferSize()
			// refill when at 1, or at 25% whichever comes first
			if remain <= 1 || remain < (int64(s)/int64(4)) {
				if q.VerboseLogging {
					log.Printf("[%s] RDY %d (%d remaining from last RDY)", c, s, remain)
				}
				atomic.StoreInt64(&c.bufferSizeRemaining, int64(s))
				err := c.sendCommand(Ready(s))
				if err != nil {
					log.Printf("[%s] error sending rdy %d - %s", c, s, err.Error())
					stopFinishLoop(c)
					continue
				}
			} else {
				if q.VerboseLogging {
					log.Printf("[%s] no RDY; remain %d (out of %d)", c, remain, s)
				}
			}
		}
	}
}

func stopFinishLoop(c *nsqConn) {
	log.Printf("[%s] closing finishedMessages channel", c)
	close(c.finishedMessages)
}

// stops a Reader gracefully
func (q *Reader) Stop() {
	if !atomic.CompareAndSwapInt32(&q.stopFlag, 0, 1) {
		return
	}

	log.Printf("Stopping reader")

	if len(q.nsqConnections) == 0 {
		q.stopHandlers()
	} else {
		for _, c := range q.nsqConnections {
			err := c.sendCommand(StartClose())
			if err != nil {
				log.Printf("[%s] failed to start close - %s", c, err.Error())
			}
		}
		go func() {
			<-time.After(time.Duration(30) * time.Second)
			q.stopHandlers()
		}()
	}

	if len(q.lookupdAddresses) != 0 {
		q.lookupdExitChan <- 1
	}
}

func (q *Reader) stopHandlers() {
	q.stopHandler.Do(func() {
		log.Printf("closing incomingMessages")
		close(q.incomingMessages)
	})
}

// this starts a handler on the Reader
// it's ok to start more than one handler simultaneously
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
				log.Printf("ERR: handler returned %s for msg %s %s", err.Error(), message.Id, message.Body)
			}

			// message passed the max number of attempts
			if err != nil && message.Attempts > q.MaxAttemptCount {
				log.Printf("WARNING: msg attempted %d times. giving up %s %s", message.Attempts, message.Id, message.Body)
				logger, ok := handler.(FailedMessageLogger)
				if ok {
					logger.LogFailedMessage(message.Message)
				}
				message.responseChannel <- &FinishedMessage{message.Id, 0, true}
				continue
			}

			// default to an exponential delay
			requeueDelay := int(q.DefaultRequeueDelay.Nanoseconds() / 1e6 * int64(message.Attempts))
			message.responseChannel <- &FinishedMessage{message.Id, requeueDelay, err == nil}
		}
	}()
}

// this starts an async handler on the Reader
// it's ok to start more than one handler simultaneously
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
			if message.Attempts > q.MaxAttemptCount {
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
