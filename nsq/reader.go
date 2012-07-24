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
	"sync/atomic"
	"time"
)

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

type Reader struct {
	TopicName           string // Name of Topic to subscribe to
	ChannelName         string // Named Channel to consume messages from
	IncomingMessages    chan *IncomingMessage
	ExitChan            chan int
	BufferSize          int // number of messages to allow in-flight from NSQ's at a time
	LookupdPollInterval int // seconds between polling lookupd's (+/- random 15 seconds)
	MaxAttemptCount     uint16
	DefaultRequeueDelay int // miliseconds to delay a message on failure
	VerboseLogging      bool

	MessagesReceived uint64
	MessagesFinished uint64
	MessagesRequeued uint64

	nsqConnections map[string]*nsqConn

	lookupdExitChan    chan int
	lookupdRecheckChan chan int
	stopFlag           bool
	runningFlag        bool
	runningHandlers    int64
	messagesInFlight   int64
	lookupdAddresses   []*net.TCPAddr
}

type nsqConn struct {
	net.Conn
	stopFlag            bool
	FinishedMessages    chan *FinishedMessage
	messagesInFlight    int64
	messagesReceived    uint64
	messagesFinished    uint64
	messagesRequeued    uint64
	bufferSizeRemaining int64
}

func (c *nsqConn) String() string {
	return c.RemoteAddr().String()
}

type IncomingMessage struct {
	*Message
	responseChannel chan *FinishedMessage
}

type FinishedMessage struct {
	Id             []byte
	RequeueDelayMs int
	Success        bool
}

func NewReader(topic string, channel string) (*Reader, error) {
	q := &Reader{
		TopicName:           topic,
		ChannelName:         channel,
		IncomingMessages:    make(chan *IncomingMessage),
		ExitChan:            make(chan int),
		nsqConnections:      make(map[string]*nsqConn),
		BufferSize:          1,
		MaxAttemptCount:     5,
		LookupdPollInterval: 120,
		lookupdExitChan:     make(chan int),
		lookupdRecheckChan:  make(chan int), // used at connection close to force a possible reconnect
		DefaultRequeueDelay: 90000,
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
		q.QueryLookupd()
		go q.LookupdLoop()
	}
	return nil
}

// poll all known lookup servers every LookupdPollInterval seconds
func (q *Reader) LookupdLoop() {
	r := rand.New(rand.NewSource(1))
	interval := q.LookupdPollInterval + r.Intn(30) - 15
	if interval < 1 {
		interval = 15
	}
	ticker := time.Tick(time.Duration(interval) * time.Second)
	for {
		select {
		case <-ticker:
			q.QueryLookupd()
		case <-q.lookupdRecheckChan:
			q.QueryLookupd()
		case <-q.lookupdExitChan:
			return
		}
	}
}

// make a HTTP req to the /lookup endpoint on each lookup server
// to find what nsq's provide the topic we are consuming.
// for any new topics, initiate a connection to those NSQ's
func (q *Reader) QueryLookupd() {
	httpclient := &http.Client{}
	for _, addr := range q.lookupdAddresses {
		endpoint := fmt.Sprintf("http://%s/lookup?topic=%s", addr, url.QueryEscape(q.TopicName))
		req, err := http.NewRequest("GET", endpoint, nil)
		if err != nil {
			log.Printf("LOOKUPD %s error %s", addr, err.Error())
			continue
		}
		resp, err := httpclient.Do(req)
		if err != nil {
			log.Printf("LOOKUPD %s error %s", addr, err.Error())
			continue
		}
		body, _ := ioutil.ReadAll(resp.Body)
		if q.VerboseLogging {
			log.Printf("LOOKUPD %s responded with %s", addr, body)
		}
		data, err := simplejson.NewJson(body)
		resp.Body.Close()
		if err != nil {
			log.Printf("LOOKUPD %s error %s", addr, err.Error())
			continue
		}

		statusCode, err := data.Get("status_code").Int()
		if err != nil || statusCode != 200 {
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
			nsqAddr, err := net.ResolveTCPAddr("tcp", net.JoinHostPort(address, fmt.Sprintf("%d", port)))
			if err == nil {
				q.ConnectToNSQ(nsqAddr)
			}
		}
	}
}

func (q *Reader) ConnectToNSQ(addr *net.TCPAddr) error {
	if q.stopFlag {
		return errors.New("Queue has been stopped")
	}

	if q.runningHandlers == 0 {
		return errors.New("There are no handlers running")
	}

	_, ok := q.nsqConnections[addr.String()]
	if ok {
		return errors.New("already connected")
	}

	log.Printf("[%s] connecting to nsqd", addr)
	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		log.Printf("[%s] %s", addr, err.Error())
		return err
	}
	conn.Write(MagicV2)
	SendCommand(conn, Subscribe(q.TopicName, q.ChannelName))

	connection := &nsqConn{
		net.Conn:         conn,
		FinishedMessages: make(chan *FinishedMessage),
		stopFlag:         false,
	}

	q.nsqConnections[addr.String()] = connection

	go ConnectionReadLoop(q, connection)
	go ConnectionFinishLoop(q, connection)
	return nil
}

func handleReadError(q *Reader, c *nsqConn, errMsg string) {
	log.Printf(errMsg)
	c.stopFlag = true
	if len(q.nsqConnections) == 1 && len(q.lookupdAddresses) == 0 {
		// This is the only remaining connection, so stop the queue
		q.stopFlag = true
	}
}

func ConnectionReadLoop(q *Reader, c *nsqConn) {
	// prime our ready state
	s := q.ConnectionBufferSize()
	if q.VerboseLogging {
		log.Printf("[%s] RDY %d", c, s)
	}
	atomic.StoreInt64(&c.bufferSizeRemaining, int64(s))
	SendCommand(c.Conn, Ready(s))
	for {
		if c.stopFlag || q.stopFlag {
			// start the connection close
			if atomic.LoadInt64(&c.messagesInFlight) == 0 {
				log.Printf("[%s] closing FinishedMessages channel", c)
				close(c.FinishedMessages)
			} else {
				log.Printf("[%s] delaying close of FinishedMesages channel; %d outstanding messages", c, c.messagesInFlight)
			}
			log.Printf("[%s] stopped read loop ", c)
			break
		}

		resp, err := ReadResponse(c.Conn)
		if err != nil {
			handleReadError(q, c, fmt.Sprintf("[%s] error on response %s", c, err.Error()))
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

			q.IncomingMessages <- &IncomingMessage{msg, c.FinishedMessages}
		case FrameTypeResponse:
			if bytes.Equal(data, []byte("CLOSE_WAIT")) {
				// server is ready for us to close (it ack'd our StartClose)
				// we can assume we will not receive any more messages over this channel
				// (but we can still write back responses)
				log.Printf("[%s] received ACK from server. now in CloseWait %d", c, frameType)
				c.stopFlag = true
			}
		default:
			log.Printf("[%s] unknown message type %d", c, frameType)
		}

	}
}

func ConnectionFinishLoop(q *Reader, c *nsqConn) {
	for {
		msg, ok := <-c.FinishedMessages
		if !ok {
			log.Printf("[%s] stopping finish loop ", c)
			c.Close()
			delete(q.nsqConnections, c.String())

			log.Printf("there are %d connections left alive", len(q.nsqConnections))

			if len(q.nsqConnections) == 0 && len(q.lookupdAddresses) == 0 {
				// no lookupd entry means no reconnection
				if q.stopFlag {
					q.stopHandlers()
				}
				return
			}
			// ie: we were the last one, and stopping
			if len(q.nsqConnections) == 0 && q.stopFlag {
				q.stopHandlers()
			}

			if len(q.lookupdAddresses) != 0 && !q.stopFlag {
				// trigger a poll of the lookupd
				q.lookupdRecheckChan <- 1
			}
			return
		}
		if msg.Success {
			if q.VerboseLogging {
				log.Printf("[%s] finishing %s", c, msg.Id)
			}
			SendCommand(c.Conn, Finish(msg.Id))
			atomic.AddUint64(&c.messagesFinished, 1)
			atomic.AddUint64(&q.MessagesFinished, 1)
		} else {
			if q.VerboseLogging {
				log.Printf("[%s] requeuing %s", c, msg.Id)
			}
			SendCommand(c.Conn, Requeue(msg.Id, msg.RequeueDelayMs))
			atomic.AddUint64(&c.messagesRequeued, 1)
			atomic.AddUint64(&q.MessagesRequeued, 1)
		}

		atomic.AddInt64(&q.messagesInFlight, -1)
		if atomic.AddInt64(&c.messagesInFlight, -1) == 0 && c.stopFlag {
			log.Printf("[%s] closing c.FinishedMessages", c)
			close(c.FinishedMessages)
			continue
		}

		if !c.stopFlag {
			remain := atomic.LoadInt64(&c.bufferSizeRemaining)
			s := q.ConnectionBufferSize()
			// refill when at 1, or at 25% whichever comes first
			if remain <= 1 || remain < (int64(s)/int64(4)) {
				if q.VerboseLogging {
					log.Printf("[%s] RDY %d (%d remaining from last RDY)", c, s, remain)
				}
				atomic.StoreInt64(&c.bufferSizeRemaining, int64(s))
				SendCommand(c.Conn, Ready(s))
			} else {
				if q.VerboseLogging {
					log.Printf("[%s] no RDY; remain %d (out of %d)", c, remain, s)
				}
			}
		}
	}
}

// stops a Reader gracefully
func (q *Reader) Stop() {
	if q.stopFlag == true {
		return
	}
	log.Printf("Stopping reader")
	q.stopFlag = true
	if len(q.nsqConnections) == 0 {
		close(q.IncomingMessages)
	} else {
		for _, c := range q.nsqConnections {
			SendCommand(c.Conn, StartClose())
		}
		go func() {
			<-time.After(time.Duration(30) * time.Second)
			close(q.IncomingMessages)
		}()
	}
	if len(q.lookupdAddresses) != 0 {
		q.lookupdExitChan <- 1
	}
}

func (q *Reader) stopHandlers() {
	log.Printf("closing IncomingMessages")
	close(q.IncomingMessages)
}

// this starts a handler on the Reader
// it's ok to start more than one handler simultaneously
func (q *Reader) AddHandler(handler Handler) {
	atomic.AddInt64(&q.runningHandlers, 1)
	log.Println("starting Handler go-routine")
	go func() {
		for {
			message, ok := <-q.IncomingMessages
			if !ok {
				log.Printf("closing Handler (after self.IncomingMessages closed)")
				if atomic.AddInt64(&q.runningHandlers, -1) == 0 {
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
			requeueDelay := q.DefaultRequeueDelay * int(message.Attempts)
			message.responseChannel <- &FinishedMessage{message.Id, requeueDelay, err == nil}
		}
	}()
}

// this starts an async handler on the Reader
// it's ok to start more than one handler simultaneously
func (q *Reader) AddAsyncHandler(handler AsyncHandler) {
	atomic.AddInt64(&q.runningHandlers, 1)
	log.Println("starting AsyncHandler go-routine")
	go func() {
		for {
			message, ok := <-q.IncomingMessages
			if !ok {
				log.Printf("closing AsyncHandler (after self.IncomingMessages closed)")
				if atomic.AddInt64(&q.runningHandlers, -1) == 0 {
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
