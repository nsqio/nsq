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
	// the amount of time to wait for a client to respond to a message
	msgTimeoutMs = 60000
)

// Channel represents the concrete type for a NSQ channel (and also
// implements the Queue interface)
//
// There can be many channels per topic and each with there own distinct 
// clients subscribed.
//
// Channels maintain all client and message, orchestrating in-flight
// messages, timeouts, requeueing, etc.
type Channel struct {
	sync.RWMutex // embed a r/w mutex

	topicName string
	name      string

	backend nsq.BackendQueue

	incomingMessageChan chan *nsq.Message
	memoryMsgChan       chan *nsq.Message
	clientMessageChan   chan *nsq.Message
	exitChan            chan int

	// state tracking
	clients          []*nsq.ServerClient
	inFlightMessages map[string]*nsq.Message

	// stat counters
	requeueCount int64
	getCount     int64
	putCount     int64
	timeoutCount int64
}

// NewChannel creates a new instance of the Channel type and returns a pointer
func NewChannel(topicName string, channelName string, inMemSize int64, dataPath string, maxBytesPerFile int64) *Channel {
	c := &Channel{
		topicName: topicName,
		name:      channelName,
		// backend names, for uniqueness, automatically include the topic... <topic>:<channel>
		backend:             NewDiskQueue(topicName+":"+channelName, dataPath, maxBytesPerFile),
		incomingMessageChan: make(chan *nsq.Message, 5),
		memoryMsgChan:       make(chan *nsq.Message, inMemSize),
		clientMessageChan:   make(chan *nsq.Message),
		exitChan:            make(chan int),
		clients:             make([]*nsq.ServerClient, 0, 5),
		inFlightMessages:    make(map[string]*nsq.Message),
	}
	go c.router()
	go c.messagePump()
	notify.Post("new_channel", c)
	return c
}

// MemoryChan implements the Queue interface
func (c *Channel) MemoryChan() chan *nsq.Message {
	return c.memoryMsgChan
}

// BackendQueue implements the Queue interface
func (c *Channel) BackendQueue() nsq.BackendQueue {
	return c.backend
}

// InFlight implements the Queue interface
func (c *Channel) InFlight() map[string]*nsq.Message {
	return c.inFlightMessages
}

// PutMessage writes to the appropriate incoming message channel
// (which will be routed asynchronously)
func (c *Channel) PutMessage(msg *nsq.Message) {
	c.putCount += 1
	c.incomingMessageChan <- msg
}

// FinishMessage successfully discards an in-flight message
func (c *Channel) FinishMessage(id []byte) error {
	msg, err := c.popInFlightMessage(id)
	if err != nil {
		log.Printf("ERROR: failed to finish message(%s) - %s", id, err.Error())
	} else {
		c.endWaitToAutoRequeue(msg)
	}
	return err
}

// RequeueMessage requeues a message based on `timeoutMs`, ie:
//
// `timeoutMs` == 0 - requeue a message immediately
// `timeoutMs`  > 0 - asynchronously wait for the specified timeout
//     and requeue a message (aka "deferred requeue")
//
func (c *Channel) RequeueMessage(id []byte, timeoutMs int) error {
	if timeoutMs == 0 {
		return c.doRequeue(id)
	}
	go func() {
		// TODO: we need cleanup for these goroutines
		msg, err := c.getInFlightMessage(id)
		if err != nil {
			log.Printf("ERROR: failed to defer requeue message(%s) - %s", id, err.Error())
			return
		}
		c.endWaitToAutoRequeue(msg)
		<-time.After(time.Duration(timeoutMs) * time.Millisecond)
		c.doRequeue(id)
	}()
	return nil
}

// doRequeue performs the low level operations to requeue a message
func (c *Channel) doRequeue(id []byte) error {
	msg, err := c.popInFlightMessage(id)
	if err != nil {
		log.Printf("ERROR: failed to requeue message(%s) - %s", id, err.Error())
	} else {
		c.endWaitToAutoRequeue(msg)
		c.requeueCount += 1
		// decr putCount so that we don't double count requeue puts
		c.putCount -= 1
		c.PutMessage(msg)
	}
	return err
}

// Router handles the muxing of incoming Channel messages, either writing
// to the in-memory channel or to the backend
func (c *Channel) router() {
	for {
		select {
		case msg := <-c.incomingMessageChan:
			select {
			case c.memoryMsgChan <- msg:
			default:
				err := WriteMessageToBackend(msg, c)
				if err != nil {
					log.Printf("ERROR: failed to write message to backend - %s", err.Error())
					// TODO: requeue?
				}
			}
		case <-c.exitChan:
			return
		}
	}
}

// pushInFlightMessage atomically adds a message to the in-flight dictionary
func (c *Channel) pushInFlightMessage(msg *nsq.Message) error {
	c.Lock()
	defer c.Unlock()

	_, ok := c.inFlightMessages[string(msg.Id)]
	if ok {
		return errors.New("E_ID_ALREADY_IN_FLIGHT")
	}
	c.inFlightMessages[string(msg.Id)] = msg

	return nil
}

// popInFlightMessage atomically removes a message from the in-flight dictionary
func (c *Channel) popInFlightMessage(id []byte) (*nsq.Message, error) {
	c.Lock()
	defer c.Unlock()

	msg, ok := c.inFlightMessages[string(id)]
	if !ok {
		return nil, errors.New("E_ID_NOT_IN_FLIGHT")
	}
	delete(c.inFlightMessages, string(id))

	return msg, nil
}

// getInFlightMessage retrieves a message from the in-flight dictionary by ID
func (c *Channel) getInFlightMessage(id []byte) (*nsq.Message, error) {
	c.RLock()
	defer c.RUnlock()

	msg, ok := c.inFlightMessages[string(id)]
	if !ok {
		return nil, errors.New("E_ID_NOT_IN_FLIGHT")
	}

	return msg, nil
}

// waitToAutoRequeue sleeps until forcefully closed (doing nothing) or 
// times out (requeue the message automatically)
func (c *Channel) waitToAutoRequeue(msg *nsq.Message) {
	// TODO: we need cleanup for these goroutines
	timer := time.NewTimer(time.Duration(msgTimeoutMs) * time.Millisecond)
	defer timer.Stop()

	select {
	case <-timer.C:
	case <-msg.UtilChan:
		return
	}

	c.timeoutCount += 1
	err := c.RequeueMessage(msg.Id, 0)
	if err != nil {
		log.Printf("ERROR: channel(%s) RequeueMessage(%s) - %s", c.name, msg.Id, err.Error())
	}
}

// endWaitToAutoRequeue forcefully closes the goroutine for auto-requeue
// for the specified message
func (c *Channel) endWaitToAutoRequeue(msg *nsq.Message) {
	select {
	case msg.UtilChan <- 1:
	default:
	}
}

// messagePump reads messages from either memory or backend and writes
// to the client output go channel
//
// it is also performs in-flight accounting and initiates the auto-requeue
// goroutine
func (c *Channel) messagePump() {
	var msg *nsq.Message
	var buf []byte
	var err error

	for {
		select {
		case msg = <-c.memoryMsgChan:
		case buf = <-c.backend.ReadChan():
			msg, err = nsq.DecodeMessage(buf)
			if err != nil {
				log.Printf("ERROR: failed to decode message - %s", err.Error())
				continue
			}
		case <-c.exitChan:
			return
		}

		msg.Retries += 1
		c.pushInFlightMessage(msg)
		c.clientMessageChan <- msg
		go c.waitToAutoRequeue(msg)
		c.getCount += 1
	}
}

// Close cleanly closes the Channel
func (c *Channel) Close() error {
	var err error

	log.Printf("CHANNEL(%s): closing", c.name)

	close(c.exitChan)
	FlushQueue(c)

	err = c.backend.Close()
	if err != nil {
		return err
	}

	return nil
}

// AddClient adds the ServerClient the Channel's client list
func (c *Channel) AddClient(client *nsq.ServerClient) {
	c.Lock()
	defer c.Unlock()

	found := false
	for _, cli := range c.clients {
		if cli == client {
			found = true
			break
		}
	}

	if !found {
		c.clients = append(c.clients, client)
	}
}

// RemoveClient removes the ServerClient from the Channel's client list
func (c *Channel) RemoveClient(client *nsq.ServerClient) {
	c.Lock()
	defer c.Unlock()

	finalClients := make([]*nsq.ServerClient, 0, len(c.clients))
	for _, cli := range c.clients {
		if cli != client {
			finalClients = append(finalClients, cli)
		}
	}

	c.clients = finalClients
}
