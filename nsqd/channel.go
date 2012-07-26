package main

import (
	"../nsq"
	"../util/pqueue"
	"bitly/notify"
	"container/heap"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// the amount of time a worker will wait when idle
const defaultWorkerWait = 250 * time.Millisecond

type ClientStatTracker interface {
	TimedOutMessage()
	Stats() ClientStats
}

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

	topicName  string
	name       string
	msgTimeout int64

	backend BackendQueue

	incomingMessageChan chan *nsq.Message
	memoryMsgChan       chan *nsq.Message
	clientMessageChan   chan *nsq.Message
	exitChan            chan int

	// state tracking
	clients []ClientStatTracker

	// TODO: these can be DRYd up
	deferredMessages map[string]*pqueue.Item
	deferredPQ       pqueue.PriorityQueue
	deferredMutex    sync.Mutex
	inFlightMessages map[string]*pqueue.Item
	inFlightPQ       pqueue.PriorityQueue
	inFlightMutex    sync.Mutex

	// stat counters
	requeueCount uint64
	messageCount uint64
	timeoutCount uint64
}

type inFlightMessage struct {
	msg    *nsq.Message
	client ClientStatTracker
}

// NewChannel creates a new instance of the Channel type and returns a pointer
func NewChannel(topicName string, channelName string, inMemSize int64, dataPath string, maxBytesPerFile int64, syncEvery int64, msgTimeout int64) *Channel {
	// backend names, for uniqueness, automatically include the topic... <topic>:<channel>
	backendName := topicName + ":" + channelName
	c := &Channel{
		topicName:           topicName,
		name:                channelName,
		msgTimeout:          msgTimeout,
		backend:             NewDiskQueue(backendName, dataPath, maxBytesPerFile, syncEvery),
		incomingMessageChan: make(chan *nsq.Message, 5),
		memoryMsgChan:       make(chan *nsq.Message, inMemSize),
		clientMessageChan:   make(chan *nsq.Message),
		exitChan:            make(chan int),
		clients:             make([]ClientStatTracker, 0, 5),
		inFlightMessages:    make(map[string]*pqueue.Item),
		inFlightPQ:          pqueue.New(int(inMemSize / 10)),
		deferredMessages:    make(map[string]*pqueue.Item),
		deferredPQ:          pqueue.New(int(inMemSize / 10)),
	}
	go c.router()
	go c.messagePump()
	go c.deferredWorker()
	go c.inFlightWorker()
	notify.Post("new_channel", c)
	return c
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

// MemoryChan implements the Queue interface
func (c *Channel) MemoryChan() chan *nsq.Message {
	return c.memoryMsgChan
}

// BackendQueue implements the Queue interface
func (c *Channel) BackendQueue() BackendQueue {
	return c.backend
}

// InFlight implements the Queue interface
func (c *Channel) InFlight() map[string]*pqueue.Item {
	return c.inFlightMessages
}

// Deferred implements the Queue interface
func (c *Channel) Deferred() map[string]*pqueue.Item {
	return c.deferredMessages
}

// PutMessage writes to the appropriate incoming message channel
// (which will be routed asynchronously)
func (c *Channel) PutMessage(msg *nsq.Message) {
	c.incomingMessageChan <- msg
	atomic.AddUint64(&c.messageCount, 1)
}

// FinishMessage successfully discards an in-flight message
func (c *Channel) FinishMessage(client ClientStatTracker, id []byte) error {
	item, err := c.popInFlightMessage(client, id)
	if err != nil {
		log.Printf("ERROR: failed to finish message(%s) - %s", id, err.Error())
	} else {
		c.removeFromInFlightPQ(item)
	}
	return err
}

// RequeueMessage requeues a message based on `time.Duration`, ie:
//
// `timeoutMs` == 0 - requeue a message immediately
// `timeoutMs`  > 0 - asynchronously wait for the specified timeout
//     and requeue a message (aka "deferred requeue")
//
func (c *Channel) RequeueMessage(client ClientStatTracker, id []byte, timeout time.Duration) error {
	// remove from inflight first
	item, err := c.popInFlightMessage(client, id)
	if err != nil {
		return err
	}
	c.removeFromInFlightPQ(item)

	msg := item.Value.(*inFlightMessage).msg

	if timeout == 0 {
		return c.doRequeue(msg)
	}

	// deferred requeue
	return c.StartDeferredTimeout(msg, timeout)
}

// AddClient adds a client to the Channel's client list
func (c *Channel) AddClient(client ClientStatTracker) {
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

// RemoveClient removes a client from the Channel's client list
func (c *Channel) RemoveClient(client ClientStatTracker) {
	c.Lock()
	defer c.Unlock()

	if len(c.clients) == 0 {
		return
	}

	finalClients := make([]ClientStatTracker, 0, len(c.clients)-1)
	for _, cli := range c.clients {
		if cli != client {
			finalClients = append(finalClients, cli)
		}
	}

	c.clients = finalClients
}

func (c *Channel) StartInFlightTimeout(msg *nsq.Message, client ClientStatTracker) error {
	value := &inFlightMessage{msg, client}
	absTs := time.Now().UnixNano() + c.msgTimeout
	item := &pqueue.Item{Value: value, Priority: absTs}
	err := c.pushInFlightMessage(item)
	if err != nil {
		return err
	}
	c.addToInFlightPQ(item)
	return nil
}

func (c *Channel) StartDeferredTimeout(msg *nsq.Message, timeout time.Duration) error {
	absTs := time.Now().UnixNano() + int64(timeout)
	item := &pqueue.Item{Value: msg, Priority: absTs}
	err := c.pushDeferredMessage(item)
	if err != nil {
		return err
	}
	c.addToDeferredPQ(item)
	return nil
}

// doRequeue performs the low level operations to requeue a message
func (c *Channel) doRequeue(msg *nsq.Message) error {
	c.incomingMessageChan <- msg
	atomic.AddUint64(&c.requeueCount, 1)
	return nil
}

// pushInFlightMessage atomically adds a message to the in-flight dictionary
func (c *Channel) pushInFlightMessage(item *pqueue.Item) error {
	c.Lock()
	defer c.Unlock()

	id := item.Value.(*inFlightMessage).msg.Id
	_, ok := c.inFlightMessages[string(id)]
	if ok {
		return errors.New("E_ID_IN_FLIGHT")
	}
	c.inFlightMessages[string(id)] = item

	return nil
}

// popInFlightMessage atomically removes a message from the in-flight dictionary
func (c *Channel) popInFlightMessage(client ClientStatTracker, id []byte) (*pqueue.Item, error) {
	c.Lock()
	defer c.Unlock()

	item, ok := c.inFlightMessages[string(id)]
	if !ok {
		return nil, errors.New("E_ID_NOT_IN_FLIGHT")
	}

	if item.Value.(*inFlightMessage).client != client {
		return nil, errors.New("E_INVALID_CLIENT")
	}

	delete(c.inFlightMessages, string(id))

	return item, nil
}

func (c *Channel) addToInFlightPQ(item *pqueue.Item) {
	c.inFlightMutex.Lock()
	defer c.inFlightMutex.Unlock()
	heap.Push(&c.inFlightPQ, item)
}

func (c *Channel) removeFromInFlightPQ(item *pqueue.Item) {
	c.inFlightMutex.Lock()
	defer c.inFlightMutex.Unlock()
	if item.Index == -1 {
		// this item has already been Pop'd off the pqueue
		return
	}
	heap.Remove(&c.inFlightPQ, item.Index)
}

func (c *Channel) pushDeferredMessage(item *pqueue.Item) error {
	c.Lock()
	defer c.Unlock()

	id := item.Value.(*nsq.Message).Id
	_, ok := c.deferredMessages[string(id)]
	if ok {
		return errors.New("E_ID_ALREADY_DEFERRED")
	}
	c.deferredMessages[string(id)] = item

	return nil
}

func (c *Channel) popDeferredMessage(id []byte) (*pqueue.Item, error) {
	c.Lock()
	defer c.Unlock()

	item, ok := c.deferredMessages[string(id)]
	if !ok {
		return nil, errors.New("E_ID_NOT_DEFERRED")
	}
	delete(c.deferredMessages, string(id))

	return item, nil
}

func (c *Channel) addToDeferredPQ(item *pqueue.Item) {
	c.deferredMutex.Lock()
	defer c.deferredMutex.Unlock()
	heap.Push(&c.deferredPQ, item)
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
					// theres not really much we can do at this point, you're certainly
					// going to lose messages...
				}
			}
		case <-c.exitChan:
			return
		}
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

		msg.Attempts++

		c.clientMessageChan <- msg
		// the client will call back to mark as in-flight w/ it's info
	}
}

func (c *Channel) deferredWorker() {
	pqWorker(&c.deferredPQ, &c.deferredMutex, func(item *pqueue.Item) {
		msg := item.Value.(*nsq.Message)
		_, err := c.popDeferredMessage(msg.Id)
		if err != nil {
			return
		}
		c.doRequeue(msg)
	})
}

func (c *Channel) inFlightWorker() {
	pqWorker(&c.inFlightPQ, &c.inFlightMutex, func(item *pqueue.Item) {
		client := item.Value.(*inFlightMessage).client
		msg := item.Value.(*inFlightMessage).msg
		_, err := c.popInFlightMessage(client, msg.Id)
		if err != nil {
			return
		}
		atomic.AddUint64(&c.timeoutCount, 1)
		client.TimedOutMessage()
		c.doRequeue(msg)
	})
}

// generic loop (executed in a goroutine) that periodically wakes up to walk
// the priority queue and call the callback
//
// if the first element on the queue is not ready (not enough time has elapsed)
// the amount of time to wait before the next iteration is adjusted to optimize
//
// TODO: this should be re-written to use interfaces not callbacks
func pqWorker(pq *pqueue.PriorityQueue, mutex *sync.Mutex, callback func(item *pqueue.Item)) {
	waitTime := defaultWorkerWait
	for {
		time.Sleep(waitTime)
		now := time.Now().UnixNano()
		for {
			mutex.Lock()
			item, diff := pq.PeekAndShift(now)
			mutex.Unlock()

			// never wait a negative amount of time
			// or longer than a second
			duration := time.Duration(diff)
			if duration <= 0 || duration > time.Second {
				waitTime = defaultWorkerWait
			} else {
				waitTime = duration + time.Millisecond
			}

			if item == nil {
				break
			}

			callback(item)
		}
	}
}
