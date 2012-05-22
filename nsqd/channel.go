package main

import (
	"../nsq"
	"../util"
	"errors"
	"log"
)

type Channel struct {
	name                string
	addClientChan       chan util.ChanReq
	removeClientChan    chan util.ChanReq
	backend             nsq.BackendQueue
	incomingMessageChan chan *nsq.Message
	msgChan             chan *nsq.Message
	inFlightMessageChan chan *nsq.Message
	ClientMessageChan   chan *nsq.Message
	requeueMessageChan  chan util.ChanReq
	finishMessageChan   chan util.ChanReq
	exitChan            chan int
	inFlightMessages    map[string]*nsq.Message
}

// Channel constructor
func NewChannel(channelName string, inMemSize int) *Channel {
	channel := &Channel{name: channelName,
		addClientChan:       make(chan util.ChanReq),
		removeClientChan:    make(chan util.ChanReq),
		backend:             nsq.NewDiskQueue(channelName),
		incomingMessageChan: make(chan *nsq.Message, 5),
		msgChan:             make(chan *nsq.Message, inMemSize),
		inFlightMessageChan: make(chan *nsq.Message),
		ClientMessageChan:   make(chan *nsq.Message),
		requeueMessageChan:  make(chan util.ChanReq),
		finishMessageChan:   make(chan util.ChanReq),
		exitChan:            make(chan int),
		inFlightMessages:    make(map[string]*nsq.Message)}
	go channel.Router()
	return channel
}

// PutMessage writes to the appropriate incoming
// message channel
func (c *Channel) PutMessage(msg *nsq.Message) {
	c.incomingMessageChan <- msg
}

func (c *Channel) FinishMessage(uuidStr string) error {
	errChan := make(chan interface{})
	c.finishMessageChan <- util.ChanReq{uuidStr, errChan}
	err, _ := (<-errChan).(error)
	return err
}

func (c *Channel) RequeueMessage(uuidStr string) error {
	errChan := make(chan interface{})
	c.requeueMessageChan <- util.ChanReq{uuidStr, errChan}
	err, _ := (<-errChan).(error)
	return err
}

// Router handles the muxing of Channel messages including
// the addition of a Client to the Channel
func (c *Channel) Router() {
	requeueRouterCloseChan := make(chan int)
	messagePumpCloseChan := make(chan int)

	go c.RequeueRouter(requeueRouterCloseChan)
	go c.MessagePump(messagePumpCloseChan)

	for {
		select {
		case msg := <-c.incomingMessageChan:
			select {
			case c.msgChan <- msg:
				log.Printf("CHANNEL(%s): wrote to msgChan", c.name)
			default:
				err := c.backend.Put(msg.Data)
				if err != nil {
					log.Printf("ERROR: t.backend.Put() - %s", err.Error())
					// TODO: requeue?
				}
				log.Printf("CHANNEL(%s): wrote to backend", c.name)
			}
		case <-c.exitChan:
			requeueRouterCloseChan <- 1
			messagePumpCloseChan <- 1
			return
		}
	}
}

func (c *Channel) RequeueRouter(closeChan chan int) {
	for {
		select {
		case msg := <-c.inFlightMessageChan:
			c.pushInFlightMessage(msg)
			go func(msg *nsq.Message) {
				if msg.ShouldRequeue(60000) {
					err := c.RequeueMessage(util.UuidToStr(msg.Uuid()))
					if err != nil {
						log.Printf("ERROR: channel(%s) - %s", c.name, err.Error())
					}
				}
			}(msg)
		case requeueReq := <-c.requeueMessageChan:
			uuidStr := requeueReq.Variable.(string)
			msg, err := c.popInFlightMessage(uuidStr)
			if err != nil {
				log.Printf("ERROR: failed to requeue message(%s) - %s", uuidStr, err.Error())
			} else {
				go func(msg *nsq.Message) {
					c.PutMessage(msg)
				}(msg)
			}
			requeueReq.RetChan <- err
		case finishReq := <-c.finishMessageChan:
			uuidStr := finishReq.Variable.(string)
			_, err := c.popInFlightMessage(uuidStr)
			if err != nil {
				log.Printf("ERROR: failed to finish message(%s) - %s", uuidStr, err.Error())
			}
			finishReq.RetChan <- err
		case <-closeChan:
			return
		}
	}
}

func (c *Channel) pushInFlightMessage(msg *nsq.Message) {
	uuidStr := util.UuidToStr(msg.Uuid())
	c.inFlightMessages[uuidStr] = msg
}

func (c *Channel) popInFlightMessage(uuidStr string) (*nsq.Message, error) {
	msg, ok := c.inFlightMessages[uuidStr]
	if !ok {
		return nil, errors.New("UUID not in flight")
	}
	delete(c.inFlightMessages, uuidStr)
	msg.EndTimer()
	return msg, nil
}

func (c *Channel) MessagePump(closeChan chan int) {
	var msg *nsq.Message

	for {
		select {
		case msg = <-c.msgChan:
		case <-c.backend.ReadReadyChan():
			buf, err := c.backend.Get()
			if err != nil {
				log.Printf("ERROR: c.backend.Get() - %s", err.Error())
				continue
			}
			msg = nsq.NewMessage(buf)
		case <-closeChan:
			return
		}

		if msg != nil {
			c.inFlightMessageChan <- msg
		}

		c.ClientMessageChan <- msg
	}
}

func (c *Channel) Close() error {
	var err error

	log.Printf("CHANNEL(%s): closing", c.name)

	c.exitChan <- 1

	err = c.backend.Close()
	if err != nil {
		return err
	}

	return nil
}
