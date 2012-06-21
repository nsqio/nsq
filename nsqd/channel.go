package main

import (
	"../nsq"
	"../util/notify"
	"errors"
	"log"
	"sync"
)

type Channel struct {
	name                string
	backend             nsq.BackendQueue
	incomingMessageChan chan *nsq.Message
	memoryMsgChan       chan *nsq.Message
	ClientMessageChan   chan *nsq.Message
	inFlightMutex       sync.RWMutex
	inFlightMessages    map[string]*nsq.Message
}

// Channel constructor
func NewChannel(channelName string, inMemSize int, dataPath string) *Channel {
	channel := &Channel{
		name:                channelName,
		backend:             nsq.NewDiskQueue(channelName, dataPath),
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
	c.incomingMessageChan <- msg
}

func (c *Channel) FinishMessage(id []byte) error {
	_, err := c.popInFlightMessage(id)
	if err != nil {
		log.Printf("ERROR: failed to finish message(%s) - %s", id, err.Error())
	}
	return err
}

func (c *Channel) RequeueMessage(id []byte) error {
	msg, err := c.popInFlightMessage(id)
	if err != nil {
		log.Printf("ERROR: failed to requeue message(%s) - %s", id, err.Error())
	} else {
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
	c.inFlightMutex.Lock()
	defer c.inFlightMutex.Unlock()

	c.inFlightMessages[string(msg.Id)] = msg
}

func (c *Channel) popInFlightMessage(id []byte) (*nsq.Message, error) {
	c.inFlightMutex.Lock()
	defer c.inFlightMutex.Unlock()

	msg, ok := c.inFlightMessages[string(id)]
	if !ok {
		return nil, errors.New("E_ID_NOT_IN_FLIGHT")
	}
	delete(c.inFlightMessages, string(id))
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

		msg.Retries += 1
		c.pushInFlightMessage(msg)
		go func(msg *nsq.Message) {
			if msg.ShouldRequeue(60000) {
				err := c.RequeueMessage(msg.Id)
				if err != nil {
					log.Printf("ERROR: channel(%s) - %s", c.name, err.Error())
				}
			}
		}(msg)

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
