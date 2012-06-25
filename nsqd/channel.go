package main

import (
	"../nsq"
	"bitly/notify"
	"errors"
	"log"
	"sync"
	"time"
)

const (
	msgTimeoutMs = 60000
)

type Channel struct {
	sync.RWMutex
	name                string
	backend             nsq.BackendQueue
	incomingMessageChan chan *nsq.Message
	memoryMsgChan       chan *nsq.Message
	ClientMessageChan   chan *nsq.Message
	inFlightMessages    map[string]*nsq.Message
	RequeueCount        int64
	GetCount            int64
	PutCount            int64
	TimeoutCount        int64
	topicName           string
}

// Channel constructor
func NewChannel(topicName string, channelName string, inMemSize int64, dataPath string, maxBytesPerFile int64) *Channel {
	channel := &Channel{
		topicName:           topicName,
		name:                channelName,
		backend:             nsq.NewDiskQueue(channelName, dataPath, maxBytesPerFile),
		incomingMessageChan: make(chan *nsq.Message, 5),
		memoryMsgChan:       make(chan *nsq.Message, inMemSize),
		ClientMessageChan:   make(chan *nsq.Message),
		inFlightMessages:    make(map[string]*nsq.Message),
	}
	go channel.Router()
	notify.Post("new_channel", channel)
	return channel
}

// PutMessage writes to the appropriate incoming
// message channel
func (c *Channel) PutMessage(msg *nsq.Message) {
	c.PutCount += 1
	c.incomingMessageChan <- msg
}

func (c *Channel) FinishMessage(id []byte) error {
	_, err := c.popInFlightMessage(id)
	if err != nil {
		log.Printf("ERROR: failed to finish message(%s) - %s", id, err.Error())
	}
	return err
}

// NOTE: the technique used to defer requeueing a message can be 
// problematic in the event of daemon failure.  these deferred messages
// are essentially in limbo until they are requeued.
// 
// arguably this is not terribly different from the data loss that would
// occur if a topic/channel had messages in memory and the daemon failed.
func (c *Channel) RequeueMessage(id []byte, timeoutMs int) error {
	if timeoutMs == 0 {
		return c.doRequeue(id)
	}
	go func() {
		msg, err := c.getInFlightMessage(id)
		if err != nil {
			log.Printf("ERROR: failed to defer requeue message(%s) - %s", id, err.Error())
			return
		}
		msg.EndTimer()
		<-time.After(time.Duration(timeoutMs) * time.Millisecond)
		c.doRequeue(id)
	}()
	return nil
}

func (c *Channel) doRequeue(id []byte) error {
	msg, err := c.popInFlightMessage(id)
	if err != nil {
		log.Printf("ERROR: failed to requeue message(%s) - %s", id, err.Error())
	} else {
		c.PutCount -= 1
		c.RequeueCount += 1
		go c.PutMessage(msg)
	}
	return err
}

// Router handles the muxing of Channel messages including
// the addition of a Client to the Channel
func (c *Channel) Router() {
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

func (c *Channel) pushInFlightMessage(msg *nsq.Message) {
	c.Lock()
	defer c.Unlock()

	c.inFlightMessages[string(msg.Id)] = msg
	go func(msg *nsq.Message) {
		if msg.ShouldRequeue(msgTimeoutMs) {
			err := c.RequeueMessage(msg.Id, 0)
			if err != nil {
				c.TimeoutCount += 1
				log.Printf("ERROR: channel(%s) RequeueMessage(%s) - %s", c.name, msg.Id, err.Error())
			}
		}
	}(msg)
}

func (c *Channel) popInFlightMessage(id []byte) (*nsq.Message, error) {
	c.Lock()
	defer c.Unlock()

	msg, ok := c.inFlightMessages[string(id)]
	if !ok {
		return nil, errors.New("E_ID_NOT_IN_FLIGHT")
	}
	delete(c.inFlightMessages, string(id))
	msg.EndTimer()

	return msg, nil
}

func (c *Channel) getInFlightMessage(id []byte) (*nsq.Message, error) {
	c.RLock()
	defer c.RUnlock()

	msg, ok := c.inFlightMessages[string(id)]
	if !ok {
		return nil, errors.New("E_ID_NOT_IN_FLIGHT")
	}

	return msg, nil
}

func (c *Channel) MessagePump() {
	var msg *nsq.Message
	var buf []byte
	var err error

	exitChan := make(chan interface{})
	notify.Observe(c.name+".channel_close", exitChan)
	for {
		select {
		case msg = <-c.memoryMsgChan:
		case buf = <-c.backend.ReadChan():
			msg, err = nsq.DecodeMessage(buf)
			if err != nil {
				log.Printf("ERROR: failed to decode message - %s", err.Error())
				continue
			}
		case <-exitChan:
			notify.Ignore(c.name+".channel_close", exitChan)
			return
		}

		msg.Retries += 1
		c.pushInFlightMessage(msg)
		c.GetCount += 1
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
