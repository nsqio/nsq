package nsqreader

import (
	"../nsq"
	"../util"
	"errors"
	"log"
	"math"
	"net"
)

type Reader interface {
	HandleMessage([]byte) error
}

type AsyncReader interface {
	HandleMessage(messageID []byte, body []byte, responseChannel chan *FinishedMessage)
}

type NSQReader struct {
	TopicName        string // Name of Topic to subscribe to
	ChannelName      string // Named Channel to consume messages from
	IncomingMessages chan *IncomingMessage
	ExitChan         chan int
	BufferSize       int

	nsqConnections map[string]*NSQConnection

	stopFlag         bool
	runningFlag      bool
	runningHandlers  int
	messagesInFlight int
	consumer         *nsq.Consumer
}

type NSQConnection struct {
	ServerAddress    *net.TCPAddr
	FinishedMessages chan *FinishedMessage

	consumer         *nsq.Consumer
	stopFlag         bool
	messagesInFlight int
	messagesReceived int
	messagesFinished int
	messagesReQueued int
}

type IncomingMessage struct {
	msg             *nsq.Message
	responseChannel chan *FinishedMessage
}

type FinishedMessage struct {
	Uuid    []byte
	Success bool
}

func NewNSQReader(topic string, channel string) (*NSQReader, error) {
	q := &NSQReader{
		TopicName:        topic,
		ChannelName:      channel,
		IncomingMessages: make(chan *IncomingMessage),
		ExitChan:         make(chan int),
		nsqConnections:   make(map[string]*NSQConnection),
		BufferSize:       1,
	}
	return q, nil
}

func (q *NSQReader) getBufferSize() int {
	b := float64(q.BufferSize)
	s := b / (float64(len(q.nsqConnections)) * .75)
	return int(math.Min(math.Max(1, s), b))

}

func (q *NSQReader) ConnectToNSQ(addr *net.TCPAddr) error {
	if q.runningHandlers == 0 {
		return errors.New("There are no handlers running")
	}

	// TODO: disconnect first?
	_, ok := q.nsqConnections[addr.String()]
	if ok {
		return errors.New("already connected")
	}

	log.Printf("[%s] connecting to NSQ", addr)
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
		messagesReceived: 0,
		messagesFinished: 0,
		messagesReQueued: 0,
	}

	q.nsqConnections[addr.String()] = connection

	// start read loop
	go ConnectionReadLoop(q, connection)
	go ConnectionFinishLoop(q, connection)
	return nil
}

func ConnectionReadLoop(q *NSQReader, c *NSQConnection) {
	// TODO calculate ready number better
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
			// on error, close the connection
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

		// TODO: handle close response
		// or timeout and close anyway
		switch frameType {
		case nsq.FrameTypeMessage:
			c.messagesReceived += 1
			msg := data.(*nsq.Message)
			log.Printf("[%s] FrameTypeMessage: %s - %s", c.ServerAddress, util.UuidToStr(msg.Uuid), msg.Body)
			c.messagesInFlight += 1
			q.messagesInFlight += 1
			q.IncomingMessages <- &IncomingMessage{msg, c.FinishedMessages}
		// case nsq.FrameTypeCloseRequest:
		// 	// server is gracefully requesting us to drop off
		// 	c.stopFlag = true
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

			// reconnect ?
			// if we are the last one, and stopFlag is on
			log.Printf("there are %d connections left alive", len(q.nsqConnections))

			if len(q.nsqConnections) == 0 {
				// TODO: reconnect (if lookupd?)
				if q.stopFlag {
					q.stopHandlers()
				}
			}
			break
		}
		if msg.Success {
			log.Printf("[%s] successfully finished %s", c.ServerAddress, util.UuidToStr(msg.Uuid))
			c.consumer.WriteCommand(c.consumer.Finish(util.UuidToStr(msg.Uuid)))
			c.messagesFinished += 1
		} else {
			log.Printf("[%s] failed message %s", c.ServerAddress, util.UuidToStr(msg.Uuid))
			c.consumer.WriteCommand(c.consumer.Requeue(util.UuidToStr(msg.Uuid)))
			c.messagesReQueued += 1
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

			// log.Println("got IncomingMessages", msg)
			err := handler.HandleMessage(msg.Body)
			if err != nil {
				message.responseChannel <- &FinishedMessage{msg.Uuid, false}
			} else {
				message.responseChannel <- &FinishedMessage{msg.Uuid, true}
			}
		}
	}()
}

// this starts a handler on the NSQReader
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
