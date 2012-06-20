package nsqreader

import (
	"../nsq"
	"../util"
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
	"time"
)

// a syncronous handler that returns an error (or nil to indicate success)
type Reader interface {
	HandleMessage([]byte) error
}

// an async handler that must send a &FinishedMessage{messageID, true|false} onto 
// responseChannel to indicate that a message has been finished. This is usefull 
// if you want to batch work together and delay response that processing is complete
type AsyncReader interface {
	HandleMessage(messageID []byte, body []byte, responseChannel chan *FinishedMessage)
}

type NSQReader struct {
	TopicName           string // Name of Topic to subscribe to
	ChannelName         string // Named Channel to consume messages from
	IncomingMessages    chan *IncomingMessage
	ExitChan            chan int
	BufferSize          int // number of messages to allow in-flight from NSQ's at a time
	LookupdPoolInterval int // seconds between polling lookupd's (+/- random 15 seconds)

	MessagesReceived uint64
	MessagesFinished uint64
	MessagesReQueued uint64

	nsqConnections map[string]*NSQConnection

	stopFlag         bool
	runningFlag      bool
	runningHandlers  int
	messagesInFlight int
	consumer         *nsq.Consumer
	lookupdAddresses []*net.TCPAddr
}

type NSQConnection struct {
	ServerAddress    *net.TCPAddr
	FinishedMessages chan *FinishedMessage

	consumer         *nsq.Consumer
	stopFlag         bool
	messagesInFlight int
	messagesReceived uint64
	messagesFinished uint64
	messagesReQueued uint64
}

type IncomingMessage struct {
	msg             *nsq.Message
	responseChannel chan *FinishedMessage
}

type FinishedMessage struct {
	MessageID []byte
	Success   bool
}

func NewNSQReader(topic string, channel string) (*NSQReader, error) {
	q := &NSQReader{
		TopicName:           topic,
		ChannelName:         channel,
		IncomingMessages:    make(chan *IncomingMessage),
		ExitChan:            make(chan int),
		nsqConnections:      make(map[string]*NSQConnection),
		BufferSize:          1,
		LookupdPoolInterval: 300,
	}
	return q, nil
}

// get an appropriate buffer size for a single connection
// essentially the global buffer size distributed amongst # connection
func (q *NSQReader) getBufferSize() int {
	b := float64(q.BufferSize)
	s := b / (float64(len(q.nsqConnections)) * .75)
	return int(math.Min(math.Max(1, s), b))

}

func (q *NSQReader) ConnectToLookupd(addr *net.TCPAddr) error {
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
func (q *NSQReader) LookupdLoop() {
	r := rand.New(rand.NewSource(1))
	interval := q.LookupdPoolInterval + r.Intn(30) - 15
	if interval < 1 {
		interval = 15
	}
	ticker := time.Tick(time.Duration(interval) * time.Second)
	select {
	case <-ticker:
		// TODO: should this kick out sooner?
		if q.stopFlag {
			return
		}
		q.QueryLookupd()
	}
}

// make a HTTP req to the /lookup endpoint on each lookup server
// to find what nsq's provide the topic we are consuming.
// for any new topics, initiate a connection to those NSQ's
func (q *NSQReader) QueryLookupd() {
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
		// {"data":{"channels":[],"producers":[{"address":"jehiah-air.local","port":"5150"}],"timestamp":1340152173},"status_code":200,"status_txt":"OK"}
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

func (q *NSQReader) ConnectToNSQ(addr *net.TCPAddr) error {
	if q.runningHandlers == 0 {
		return errors.New("There are no handlers running")
	}

	_, ok := q.nsqConnections[addr.String()]
	if ok {
		return errors.New("already connected")
	}

	log.Printf("[%s] connecting to nsqd", addr)
	consumer := nsq.NewConsumer(addr)
	err := consumer.Connect()
	if err != nil {
		log.Printf("[%s] %s", addr, err.Error())
		return err
	}

	consumer.Version(nsq.ProtocolV2Magic)
	consumer.WriteCommand(consumer.Subscribe(q.TopicName, q.ChannelName))

	connection := &NSQConnection{
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

func ConnectionReadLoop(q *NSQReader, c *NSQConnection) {
	// prime our ready state
	c.consumer.WriteCommand(c.consumer.Ready(q.getBufferSize()))
	for {
		if c.stopFlag || q.stopFlag {
			// start the connection close
			log.Printf("[%s] stopping read loop ", c.ServerAddress)
			if c.messagesInFlight == 0 {
				close(c.FinishedMessages)
			}
			break
		}

		resp, err := c.consumer.ReadResponse()
		if err != nil {
			log.Printf("[%s] error on response %s", c.ServerAddress, err.Error())
			c.stopFlag = true
			continue
		}

		frameType, data, err := c.consumer.UnpackResponse(resp)
		if err != nil {
			log.Printf("[%s] error (%s) unpacking response %s %s", c.ServerAddress, err.Error(), frameType, data)
			c.stopFlag = true
			continue
		}

		switch frameType {
		case nsq.FrameTypeMessage:
			c.messagesReceived += 1
			q.MessagesReceived += 1
			msg := data.(*nsq.Message)
			log.Printf("[%s] FrameTypeMessage: %s - %s", c.ServerAddress, util.UuidToStr(msg.Uuid), msg.Body)
			c.messagesInFlight += 1
			q.messagesInFlight += 1
			q.IncomingMessages <- &IncomingMessage{msg, c.FinishedMessages}
		case nsq.FrameTypeCloseWait:
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

func ConnectionFinishLoop(q *NSQReader, c *NSQConnection) {
	for {
		msg, ok := <-c.FinishedMessages
		if !ok {
			log.Printf("[%s] stopping finish loop ", c.ServerAddress)
			c.consumer.Close()
			delete(q.nsqConnections, c.ServerAddress.String())

			log.Printf("there are %d connections left alive", len(q.nsqConnections))

			if len(q.nsqConnections) == 0 && len(q.lookupdAddresses) != 0 {
				// assume that if we have a lookupd entry, we will be reconnecting
				if q.stopFlag {
					q.stopHandlers()
				}
			}
			break
		}
		if msg.Success {
			log.Printf("[%s] successfully finished %s", c.ServerAddress, util.UuidToStr(msg.MessageID))
			c.consumer.WriteCommand(c.consumer.Finish(util.UuidToStr(msg.MessageID)))
			c.messagesFinished += 1
			q.MessagesFinished += 1
		} else {
			log.Printf("[%s] failed message %s", c.ServerAddress, util.UuidToStr(msg.MessageID))
			c.consumer.WriteCommand(c.consumer.Requeue(util.UuidToStr(msg.MessageID)))
			c.messagesReQueued += 1
			q.MessagesReQueued += 1
		}
		c.messagesInFlight -= 1
		q.messagesInFlight -= 1

		// TODO: don't write this every time (for when we have batch requesting enabled)
		if !c.stopFlag {
			s := q.getBufferSize() - c.messagesInFlight
			if s >= 1 {
				log.Printf("RDY %d", s)
				c.consumer.WriteCommand(c.consumer.Ready(s))
			}
		}
	}
}

// stops a NSQReaderReader gracefully
func (q *NSQReader) Stop() {
	q.stopFlag = true // kicks out the GET loop
	// start the stop cycle for each client
	for _, c := range q.nsqConnections {
		c.consumer.WriteCommand(c.consumer.StartClose())
	}

}

func (q *NSQReader) stopHandlers() {
	close(q.IncomingMessages)
}

// this starts a handler on the NSQReader
// it's ok to start more than one handler simultaneously
func (q *NSQReader) AddHandler(handler Reader) {
	q.runningHandlers += 1
	log.Println("starting handle go-routine")
	go func() {
		for {
			message, ok := <-q.IncomingMessages
			if !ok {
				log.Printf("closing handler (after self.IncomingMessages closed)")
				q.runningHandlers -= 1
				if q.runningHandlers == 0 {
					// we are done?
					log.Printf("closed last handler")
					q.ExitChan <- 1
				}
				break
			}

			msg := message.msg
			err := handler.HandleMessage(msg.Body)
			message.responseChannel <- &FinishedMessage{msg.Uuid, err == nil}
		}
	}()
}

// this starts an async handler on the NSQReader
// it's ok to start more than one handler simultaneously
func (q *NSQReader) AddAsyncHandler(handler AsyncReader) {
	q.runningHandlers += 1
	log.Println("starting handle go-routine")
	go func() {
		for {
			message, ok := <-q.IncomingMessages
			if !ok {
				log.Printf("closing handler (after self.IncomingMessages closed)")
				q.runningHandlers -= 1
				if q.runningHandlers == 0 {
					// we are done?
					log.Printf("closed last handler")
					q.ExitChan <- 1
				}
				break
			}
			handler.HandleMessage(message.msg.Uuid, message.msg.Body, message.responseChannel)
		}
	}()
}
