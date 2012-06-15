package main

import (
	"../nsq"
	"../util"
	"../util/notify"
	"errors"
	"log"
)

type Channel struct {
	name                string
	backend             nsq.BackendQueue
	incomingMessageChan chan *nsq.Message
	memoryMsgChan       chan *nsq.Message
	inFlightMessageChan chan *nsq.Message
	ClientMessageChan   chan *nsq.Message
	requeueMessageChan  chan util.ChanReq
	finishMessageChan   chan util.ChanReq
	inFlightMessages    map[string]*nsq.Message
}

// Channel constructor
func NewChannel(channelName string, inMemSize int, dataPath string) *Channel {
	channel := &Channel{name: channelName,
		backend:             nsq.NewDiskQueue(channelName, dataPath),
		incomingMessageChan: make(chan *nsq.Message, 5),
		memoryMsgChan:       make(chan *nsq.Message, inMemSize),
		inFlightMessageChan: make(chan *nsq.Message),
		ClientMessageChan:   make(chan *nsq.Message),
		requeueMessageChan:  make(chan util.ChanReq),
		finishMessageChan:   make(chan util.ChanReq),
		inFlightMessages:    make(map[string]*nsq.Message)}
	go channel.Router()
	notify.Post("new_channel", channel)
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
	go c.RequeueRouter()
	go c.MessagePump()

	exitChan := make(chan interface{})
	notify.Observe(c.name+".channel_close", exitChan)
	for {
		select {
		case msg := <-c.incomingMessageChan:
			select {
			case c.memoryMsgChan <- msg:
				log.Printf("CHANNEL(%s): wrote to memoryMsgChan", c.name)
			default:
				data, err := msg.Encode()
				if err != nil {
					log.Printf("ERROR: failed to Encode() message - %s", err.Error())
					// TODO: shrug
					continue
				}
				err = c.backend.Put(data)
				if err != nil {
					log.Printf("ERROR: t.backend.Put() - %s", err.Error())
					// TODO: requeue?
				}
				log.Printf("CHANNEL(%s): wrote to backend", c.name)
			}
		case <-exitChan:
			notify.Ignore(c.name+".channel_close", exitChan)
			return
		}
	}
}

func (c *Channel) RequeueRouter() {
	exitChan := make(chan interface{})
	notify.Observe(c.name+".channel_close", exitChan)
	for {
		select {
		case msg := <-c.inFlightMessageChan:
			c.pushInFlightMessage(msg)
			go func(msg *nsq.Message) {
				if msg.ShouldRequeue(60000) {
					err := c.RequeueMessage(util.UuidToStr(msg.Uuid))
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
		case <-exitChan:
			notify.Ignore(c.name+".channel_close", exitChan)
			return
		}
	}
}

func (c *Channel) pushInFlightMessage(msg *nsq.Message) {
	uuidStr := util.UuidToStr(msg.Uuid)
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

func (c *Channel) MessagePump() {
	var msg *nsq.Message

	exitChan := make(chan interface{})
	notify.Observe(c.name+".channel_close", exitChan)
	for {
		select {
		case msg = <-c.memoryMsgChan:
		case <-c.backend.ReadReadyChan():
			buf, err := c.backend.Get()
			if err != nil {
				log.Printf("ERROR: c.backend.Get() - %s", err.Error())
				continue
			}
			msg, err = nsq.DecodeMessage(buf)
			if err != nil {
				log.Printf("ERROR: failed to decode message - %s", err.Error())
				continue
			}
		case <-exitChan:
			notify.Ignore(c.name+".channel_close", exitChan)
			return
		}

		// TODO: not sure how msg would ever be nil here
		if msg != nil {
			msg.Retries += 1
			c.inFlightMessageChan <- msg
		}

		c.ClientMessageChan <- msg
	}
}

func (c *Channel) Close() error {
	var err error

	log.Printf("CHANNEL(%s): closing", c.name)

	notify.Post(c.name+".channel_close", nil)

	err = c.backend.Close()
	if err != nil {
		return err
	}

	return nil
}
