package nsq

import (
	"bitly/simplejson"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

// a syncronous handler that returns an error (or nil to indicate success)
type Handler interface {
	HandleMessage(message *Message) error
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
	LookupdPoolInterval int // seconds between polling lookupd's (+/- random 15 seconds)

	MessagesReceived uint64
	MessagesFinished uint64
	MessagesReQueued uint64

	nsqConnections map[string]*nsqConn

	lookupdExitChan    chan int
	lookupdRecheckChan chan int
	stopFlag           bool
	runningFlag        bool
	runningHandlers    int
	messagesInFlight   int
	consumer           *Consumer
	lookupdAddresses   []*net.TCPAddr

	sync.RWMutex
}

type nsqConn struct {
	ServerAddress    *net.TCPAddr
	FinishedMessages chan *FinishedMessage

	consumer         *Consumer
	stopFlag         bool
	messagesInFlight int
	messagesReceived uint64
	messagesFinished uint64
	messagesReQueued uint64
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
		LookupdPoolInterval: 300,
		lookupdExitChan:     make(chan int),
		lookupdRecheckChan:  make(chan int), // used at connection close to force a possible reconnect
	}
	return q, nil
}

// get an appropriate buffer size for a single connection
// essentially the global buffer size distributed amongst # connection
func (q *Reader) getBufferSize() int {
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

// poll all known lookup servers every LookupdPoolInterval seconds
func (q *Reader) LookupdLoop() {
	r := rand.New(rand.NewSource(1))
	interval := q.LookupdPoolInterval + r.Intn(30) - 15
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
		log.Printf("LOOKUPD %s responded with %s", addr, body)
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
			port := producerData["port"].(int)

			// make an address, start a connection
			nsqAddr, err := net.ResolveTCPAddr("tcp", net.JoinHostPort(address, strconv.Itoa(port)))
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
	consumer := NewConsumer(addr)
	err := consumer.Connect()
	if err != nil {
		log.Printf("[%s] %s", addr, err.Error())
		return err
	}

	consumer.Version(ProtocolV2Magic)
	consumer.WriteCommand(consumer.Subscribe(q.TopicName, q.ChannelName))

	connection := &nsqConn{
		ServerAddress:    addr,
		FinishedMessages: make(chan *FinishedMessage),
		consumer:         consumer,
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
	c.consumer.WriteCommand(c.consumer.Ready(q.getBufferSize()))
	for {
		if c.stopFlag || q.stopFlag {
			// start the connection close
			q.RLock()
			cInFlight := c.messagesInFlight
			q.RUnlock()
			if cInFlight == 0 {
				log.Printf("[%s] closing FinishedMessages channel", c.ServerAddress)
				close(c.FinishedMessages)
			} else {
				log.Printf("[%s] delaying close of FinishedMesages channel; %d outstanding messages", c.ServerAddress, c.messagesInFlight)
			}
			log.Printf("[%s] stopped read loop ", c.ServerAddress)
			break
		}

		resp, err := c.consumer.ReadResponse()
		if err != nil {
			handleReadError(q, c, fmt.Sprintf("[%s] error on response %s", c.ServerAddress, err.Error()))
			continue
		}

		frameType, data, err := c.consumer.UnpackResponse(resp)
		if err != nil {
			handleReadError(q, c, fmt.Sprintf("[%s] error (%s) unpacking response %s %s", c.ServerAddress, err.Error(), frameType, data))
			continue
		}

		switch frameType {
		case FrameTypeMessage:
			c.messagesReceived += 1
			q.MessagesReceived += 1
			msg := data.(*Message)
			log.Printf("[%s] FrameTypeMessage: %s - %s", c.ServerAddress, msg.Id, msg.Body)
			q.Lock()
			c.messagesInFlight += 1
			q.messagesInFlight += 1
			q.Unlock()
			q.IncomingMessages <- &IncomingMessage{msg, c.FinishedMessages}
		case FrameTypeCloseWait:
			// server is ready for us to close (it ack'd our StartClose)
			// we can assume we will not receive any more messages over this channel
			// (but we can still write back responses)
			log.Printf("[%s] received ACK from server. now in CloseWait %d", c.ServerAddress, frameType)
			c.stopFlag = true
		default:
			log.Printf("[%s] unknown message type %d", c.ServerAddress, frameType)
		}

	}
}

func ConnectionFinishLoop(q *Reader, c *nsqConn) {
	for {
		msg, ok := <-c.FinishedMessages
		if !ok {
			log.Printf("[%s] stopping finish loop ", c.ServerAddress)
			c.consumer.Close()
			delete(q.nsqConnections, c.ServerAddress.String())

			log.Printf("there are %d connections left alive", len(q.nsqConnections))

			if len(q.nsqConnections) == 0 && len(q.lookupdAddresses) == 0 {
				// no lookupd entry means no reconnection
				if q.stopFlag {
					q.stopHandlers()
				}
			}
			if len(q.lookupdAddresses) != 0 && !q.stopFlag {
				// trigger a poll of the lookupd
				q.lookupdRecheckChan <- 1
			}
			return
		}
		if msg.Success {
			log.Printf("[%s] successfully finished %s", c.ServerAddress, msg.Id)
			c.consumer.WriteCommand(c.consumer.Finish(msg.Id))
			c.messagesFinished += 1
			q.MessagesFinished += 1
		} else {
			log.Printf("[%s] failed message %s", c.ServerAddress, msg.Id)
			c.consumer.WriteCommand(c.consumer.Requeue(msg.Id, msg.RequeueDelayMs))
			c.messagesReQueued += 1
			q.MessagesReQueued += 1
		}
		var cInFlight int
		q.Lock()
		c.messagesInFlight -= 1
		q.messagesInFlight -= 1
		cInFlight = c.messagesInFlight
		q.Unlock()

		if c.stopFlag && cInFlight == 0 {
			log.Printf("[%s] closing c.FinishedMessages", c.ServerAddress)
			close(c.FinishedMessages)
			continue
		}

		// TODO: don't write this every time (for when we have batch requesting enabled)
		if !c.stopFlag {
			s := q.getBufferSize() - cInFlight
			if s >= 1 {
				log.Printf("RDY %d", s)
				c.consumer.WriteCommand(c.consumer.Ready(s))
			}
		}
	}
}

// stops a Reader gracefully
func (q *Reader) Stop() {
	q.stopFlag = true
	for _, c := range q.nsqConnections {
		c.consumer.WriteCommand(c.consumer.StartClose())
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
	q.runningHandlers += 1
	log.Println("starting Handler go-routine")
	go func() {
		for {
			message, ok := <-q.IncomingMessages
			if !ok {
				log.Printf("closing Handler (after self.IncomingMessages closed)")
				q.runningHandlers -= 1
				if q.runningHandlers == 0 {
					q.ExitChan <- 1
				}
				break
			}

			err := handler.HandleMessage(message.Message)
			if err != nil {
				log.Printf("ERR: handler returned %s for msg %s %s", err.Error(), message.Id, message.Body)
			}
			// default to an exponential delay
			requeueDelay := 90000 * int(message.Retries)
			message.responseChannel <- &FinishedMessage{message.Id, requeueDelay, err == nil}
		}
	}()
}

// this starts an async handler on the Reader
// it's ok to start more than one handler simultaneously
func (q *Reader) AddAsyncHandler(handler AsyncHandler) {
	q.runningHandlers += 1
	log.Println("starting AsyncHandler go-routine")
	go func() {
		for {
			message, ok := <-q.IncomingMessages
			if !ok {
				log.Printf("closing AsyncHandler (after self.IncomingMessages closed)")
				q.runningHandlers -= 1
				if q.runningHandlers == 0 {
					q.ExitChan <- 1
				}
				break
			}
			handler.HandleMessage(message.Message, message.responseChannel)
		}
	}()
}
