package main

import (
	"../nsq"
	"../util/pqueue"
	"bitly/notify"
	"container/heap"
	"errors"
	"log"
	"sync"
	"time"
)

const (
	// TODO: move this into the nsqd object so its overwritable for tests
	// the amount of time to wait for a client to respond to a message
	msgTimeout = int64(60 * time.Second)

	// the amount of time a worker will wait when idle
	defaultWorkerWait = 250 * time.Millisecond
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
	requeuePQ        pqueue.PriorityQueue
	requeueMutex     sync.Mutex
	inFlightMessages map[string]interface{}
	inFlightPQ       pqueue.PriorityQueue
	inFlightMutex    sync.Mutex

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
		inFlightMessages:    make(map[string]interface{}),
		inFlightPQ:          make(pqueue.PriorityQueue, 0, inMemSize/10),
		requeuePQ:           make(pqueue.PriorityQueue, 0, inMemSize/10),
	}
	go c.router()
	go c.messagePump()
	go c.requeueWorker()
	go c.inFlightWorker()
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
func (c *Channel) InFlight() map[string]interface{} {
	return c.inFlightMessages
}

// PutMessage writes to the appropriate incoming message channel
// (which will be routed asynchronously)
func (c *Channel) PutMessage(msg *nsq.Message) {
	c.incomingMessageChan <- msg
}

// FinishMessage successfully discards an in-flight message
func (c *Channel) FinishMessage(id []byte) error {
	item, err := c.popInFlightMessage(id)
	if err != nil {
		log.Printf("ERROR: failed to finish message(%s) - %s", id, err.Error())
	} else {
		c.removeFromInFlightPQ(item)
	}
	return err
}

// RequeueMessage requeues a message based on `timeoutMs`, ie:
//
// `timeoutMs` == 0 - requeue a message immediately
// `timeoutMs`  > 0 - asynchronously wait for the specified timeout
//     and requeue a message (aka "deferred requeue")
//
func (c *Channel) RequeueMessage(id []byte, timeout time.Duration) error {
	if timeout == 0 {
		return c.doRequeue(id)
	}

	item, err := c.getInFlightMessage(id)
	if err != nil {
		return err
	}
	c.removeFromInFlightPQ(item)
	c.addToDeferredPQ(item.Value.(*nsq.Message), timeout)

	return nil
}

// doRequeue performs the low level operations to requeue a message
func (c *Channel) doRequeue(id []byte) error {
	item, err := c.popInFlightMessage(id)
	if err != nil {
		log.Printf("ERROR: failed to requeue message(%s) - %s", id, err.Error())
	} else {
		msg := item.Value.(*nsq.Message)
		c.incomingMessageChan <- msg
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
					continue
				}
			}
			c.putCount++
		case <-c.exitChan:
			return
		}
	}
}

// pushInFlightMessage atomically adds a message to the in-flight dictionary
func (c *Channel) pushInFlightMessage(item *pqueue.Item) error {
	c.Lock()
	defer c.Unlock()

	id := item.Value.(*nsq.Message).Id
	_, ok := c.inFlightMessages[string(id)]
	if ok {
		return errors.New("E_ID_ALREADY_IN_FLIGHT")
	}
	c.inFlightMessages[string(id)] = item

	return nil
}

// popInFlightMessage atomically removes a message from the in-flight dictionary
func (c *Channel) popInFlightMessage(id []byte) (*pqueue.Item, error) {
	c.Lock()
	defer c.Unlock()

	item, ok := c.inFlightMessages[string(id)]
	if !ok {
		return nil, errors.New("E_ID_NOT_IN_FLIGHT")
	}
	delete(c.inFlightMessages, string(id))

	return item.(*pqueue.Item), nil
}

// getInFlightMessage retrieves a message from the in-flight dictionary by ID
func (c *Channel) getInFlightMessage(id []byte) (*pqueue.Item, error) {
	c.RLock()
	defer c.RUnlock()

	item, ok := c.inFlightMessages[string(id)]
	if !ok {
		return nil, errors.New("E_ID_NOT_IN_FLIGHT")
	}

	return item.(*pqueue.Item), nil
}

func (c *Channel) addToInFlightPQ(item *pqueue.Item) {
	c.inFlightMutex.Lock()
	defer c.inFlightMutex.Unlock()
	heap.Push(&c.inFlightPQ, item)
}

func (c *Channel) removeFromInFlightPQ(item *pqueue.Item) {
	c.inFlightMutex.Lock()
	defer c.inFlightMutex.Unlock()
	heap.Remove(&c.inFlightPQ, item.Index)
}

func (c *Channel) addToDeferredPQ(msg *nsq.Message, timeout time.Duration) {
	c.requeueMutex.Lock()
	defer c.requeueMutex.Unlock()
	absTs := time.Now().UnixNano() + int64(timeout)
	heap.Push(&c.requeuePQ, &pqueue.Item{Value: msg, Priority: -absTs})
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

		msg.Retries++

		c.clientMessageChan <- msg

		absTs := time.Now().UnixNano() + msgTimeout
		item := &pqueue.Item{Value: msg, Priority: -absTs}
		c.pushInFlightMessage(item)
		c.addToInFlightPQ(item)

		c.getCount++
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

func (c *Channel) requeueWorker() {
	pqWorker(&c.requeuePQ, &c.requeueMutex, func(item *pqueue.Item) {
		msg := item.Value.(*nsq.Message)
		c.requeueCount++
		c.doRequeue(msg.Id)
	})
}

func (c *Channel) inFlightWorker() {
	pqWorker(&c.inFlightPQ, &c.inFlightMutex, func(item *pqueue.Item) {
		msg := item.Value.(*nsq.Message)
		c.timeoutCount++
		c.requeueCount++
		c.doRequeue(msg.Id)
	})
}

func pqWorker(pq *pqueue.PriorityQueue, mutex *sync.Mutex, callback func(item *pqueue.Item)) {
	waitTime := defaultWorkerWait
	for {
		<-time.After(waitTime)
		now := time.Now().UnixNano()
		for {
			mutex.Lock()
			if pq.Len() == 0 {
				mutex.Unlock()
				waitTime = defaultWorkerWait
				break
			}
			item := pq.Peek().(*pqueue.Item)
			if now < -item.Priority {
				waitTime = time.Duration((-item.Priority)-now) + time.Millisecond
				mutex.Unlock()
				break
			}
			item = heap.Pop(pq).(*pqueue.Item)
			mutex.Unlock()

			callback(item)
		}
	}
}
