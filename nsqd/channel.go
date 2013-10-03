package main

import (
	"bytes"
	"container/heap"
	"errors"
	"github.com/bitly/go-nsq"
	"github.com/bitly/nsq/util"
	"github.com/bitly/nsq/util/pqueue"
	"log"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// the amount of time a worker will wait when idle
const defaultWorkerWait = 100 * time.Millisecond

type Consumer interface {
	UnPause()
	Pause()
	Close() error
	TimedOutMessage()
	Stats() ClientStats
	Empty()
}

// Channel represents the concrete type for a NSQ channel (and also
// implements the Queue interface)
//
// There can be multiple channels per topic, each with there own unique set
// of subscribers (clients).
//
// Channels maintain all client and message metadata, orchestrating in-flight
// messages, timeouts, requeueing, etc.
type Channel struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	requeueCount uint64
	messageCount uint64
	timeoutCount uint64

	sync.RWMutex

	topicName string
	name      string
	context   *Context

	backend BackendQueue

	incomingMsgChan chan *nsq.Message
	memoryMsgChan   chan *nsq.Message
	clientMsgChan   chan *nsq.Message
	exitChan        chan int
	waitGroup       util.WaitGroupWrapper
	exitFlag        int32

	// state tracking
	clients          map[int64]Consumer
	paused           int32
	ephemeralChannel bool
	deleteCallback   func(*Channel)
	deleter          sync.Once

	// TODO: these can be DRYd up
	deferredMessages map[nsq.MessageID]*pqueue.Item
	deferredPQ       pqueue.PriorityQueue
	deferredMutex    sync.Mutex
	inFlightMessages map[nsq.MessageID]*pqueue.Item
	inFlightPQ       pqueue.PriorityQueue
	inFlightMutex    sync.Mutex

	// stat counters
	bufferedCount int32
}

type inFlightMessage struct {
	msg      *nsq.Message
	clientID int64
	ts       time.Time
}

// NewChannel creates a new instance of the Channel type and returns a pointer
func NewChannel(topicName string, channelName string, context *Context,
	deleteCallback func(*Channel)) *Channel {
	c := &Channel{
		topicName:       topicName,
		name:            channelName,
		incomingMsgChan: make(chan *nsq.Message, 1),
		memoryMsgChan:   make(chan *nsq.Message, context.nsqd.options.memQueueSize),
		clientMsgChan:   make(chan *nsq.Message),
		exitChan:        make(chan int),
		clients:         make(map[int64]Consumer),
		deleteCallback:  deleteCallback,
		context:         context,
	}

	c.initPQ()

	if strings.HasSuffix(channelName, "#ephemeral") {
		c.ephemeralChannel = true
		c.backend = NewDummyBackendQueue()
	} else {
		// backend names, for uniqueness, automatically include the topic... <topic>:<channel>
		backendName := topicName + ":" + channelName
		c.backend = NewDiskQueue(backendName,
			context.nsqd.options.dataPath,
			context.nsqd.options.maxBytesPerFile,
			context.nsqd.options.syncEvery,
			context.nsqd.options.syncTimeout)
	}

	go c.messagePump()

	c.waitGroup.Wrap(func() { c.router() })
	c.waitGroup.Wrap(func() { c.deferredWorker() })
	c.waitGroup.Wrap(func() { c.inFlightWorker() })

	go c.context.nsqd.Notify(c)

	return c
}

func (c *Channel) initPQ() {
	pqSize := int(math.Max(1, float64(c.context.nsqd.options.memQueueSize)/10))

	c.inFlightMessages = make(map[nsq.MessageID]*pqueue.Item)
	c.deferredMessages = make(map[nsq.MessageID]*pqueue.Item)

	c.inFlightMutex.Lock()
	c.inFlightPQ = pqueue.New(pqSize)
	c.inFlightMutex.Unlock()

	c.deferredMutex.Lock()
	c.deferredPQ = pqueue.New(pqSize)
	c.deferredMutex.Unlock()
}

// Exiting returns a boolean indicating if this channel is closed/exiting
func (c *Channel) Exiting() bool {
	return atomic.LoadInt32(&c.exitFlag) == 1
}

// Delete empties the channel and closes
func (c *Channel) Delete() error {
	return c.exit(true)
}

// Close cleanly closes the Channel
func (c *Channel) Close() error {
	return c.exit(false)
}

func (c *Channel) exit(deleted bool) error {
	if !atomic.CompareAndSwapInt32(&c.exitFlag, 0, 1) {
		return errors.New("exiting")
	}

	if deleted {
		log.Printf("CHANNEL(%s): deleting", c.name)

		// since we are explicitly deleting a channel (not just at system exit time)
		// de-register this from the lookupd
		go c.context.nsqd.Notify(c)
	} else {
		log.Printf("CHANNEL(%s): closing", c.name)
	}

	// this forceably closes client connections
	for _, client := range c.clients {
		client.Close()
	}

	close(c.exitChan)

	// handle race condition w/ things writing into incomingMsgChan
	c.Lock()
	close(c.incomingMsgChan)
	c.Unlock()

	// synchronize the close of router() and pqWorkers (2)
	c.waitGroup.Wait()

	if deleted {
		// empty the queue (deletes the backend files, too)
		c.Empty()
		return c.backend.Delete()
	}

	// write anything leftover to disk
	c.flush()
	return c.backend.Close()
}

func (c *Channel) Empty() error {
	c.Lock()
	defer c.Unlock()

	c.initPQ()
	for _, client := range c.clients {
		client.Empty()
	}

	clientMsgChan := c.clientMsgChan
	for {
		select {
		case _, ok := <-clientMsgChan:
			if !ok {
				// c.clientMsgChan may be closed while in this loop
				// so just remove it from the select so we can make progress
				clientMsgChan = nil
			}
		case <-c.memoryMsgChan:
		default:
			goto finish
		}
	}

finish:
	return c.backend.Empty()
}

// flush persists all the messages in internal memory buffers to the backend
// it does not drain inflight/deferred because it is only called in Close()
func (c *Channel) flush() error {
	var msgBuf bytes.Buffer

	// messagePump is responsible for closing the channel it writes to
	// this will read until its closed (exited)
	for msg := range c.clientMsgChan {
		log.Printf("CHANNEL(%s): recovered buffered message from clientMsgChan", c.name)
		WriteMessageToBackend(&msgBuf, msg, c.backend)
	}

	if len(c.memoryMsgChan) > 0 || len(c.inFlightMessages) > 0 || len(c.deferredMessages) > 0 {
		log.Printf("CHANNEL(%s): flushing %d memory %d in-flight %d deferred messages to backend",
			c.name, len(c.memoryMsgChan), len(c.inFlightMessages), len(c.deferredMessages))
	}

	for {
		select {
		case msg := <-c.memoryMsgChan:
			err := WriteMessageToBackend(&msgBuf, msg, c.backend)
			if err != nil {
				log.Printf("ERROR: failed to write message to backend - %s", err.Error())
			}
		default:
			goto finish
		}
	}

finish:
	for _, item := range c.inFlightMessages {
		msg := item.Value.(*inFlightMessage).msg
		err := WriteMessageToBackend(&msgBuf, msg, c.backend)
		if err != nil {
			log.Printf("ERROR: failed to write message to backend - %s", err.Error())
		}
	}

	for _, item := range c.deferredMessages {
		msg := item.Value.(*nsq.Message)
		err := WriteMessageToBackend(&msgBuf, msg, c.backend)
		if err != nil {
			log.Printf("ERROR: failed to write message to backend - %s", err.Error())
		}
	}

	return nil
}

func (c *Channel) Depth() int64 {
	return int64(len(c.memoryMsgChan)) + c.backend.Depth() + int64(atomic.LoadInt32(&c.bufferedCount))
}

func (c *Channel) Pause() error {
	atomic.StoreInt32(&c.paused, 1)

	c.RLock()
	for _, client := range c.clients {
		client.Pause()
	}
	c.RUnlock()

	c.context.nsqd.Lock()
	defer c.context.nsqd.Unlock()
	// pro-actively persist metadata so in case of process failure
	// nsqd won't suddenly unpause a channel
	return c.context.nsqd.PersistMetadata()
}

func (c *Channel) UnPause() error {
	atomic.StoreInt32(&c.paused, 0)

	c.RLock()
	for _, client := range c.clients {
		client.UnPause()
	}
	c.RUnlock()

	c.context.nsqd.Lock()
	defer c.context.nsqd.Unlock()
	// pro-actively persist metadata so in case of process failure
	// nsqd won't suddenly pause a channel
	return c.context.nsqd.PersistMetadata()
}

func (c *Channel) IsPaused() bool {
	return atomic.LoadInt32(&c.paused) == 1
}

// PutMessage writes to the appropriate incoming message channel
// (which will be routed asynchronously)
func (c *Channel) PutMessage(msg *nsq.Message) error {
	c.RLock()
	defer c.RUnlock()
	if atomic.LoadInt32(&c.exitFlag) == 1 {
		return errors.New("exiting")
	}
	c.incomingMsgChan <- msg
	atomic.AddUint64(&c.messageCount, 1)
	return nil
}

// TouchMessage resets the timeout for an in-flight message
func (c *Channel) TouchMessage(clientID int64, id nsq.MessageID) error {
	item, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}
	c.removeFromInFlightPQ(item)

	ifMsg := item.Value.(*inFlightMessage)
	currentTimeout := time.Unix(0, item.Priority)
	newTimeout := currentTimeout.Add(c.context.nsqd.options.msgTimeout)
	if newTimeout.Add(c.context.nsqd.options.msgTimeout).Sub(ifMsg.ts) >= c.context.nsqd.options.maxMsgTimeout {
		// we would have gone over, set to the max
		newTimeout = ifMsg.ts.Add(c.context.nsqd.options.maxMsgTimeout)
	}

	item.Priority = newTimeout.UnixNano()
	err = c.pushInFlightMessage(item)
	if err != nil {
		return err
	}
	c.addToInFlightPQ(item)
	return nil
}

// FinishMessage successfully discards an in-flight message
func (c *Channel) FinishMessage(clientID int64, id nsq.MessageID) error {
	item, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}
	c.removeFromInFlightPQ(item)
	return nil
}

// RequeueMessage requeues a message based on `time.Duration`, ie:
//
// `timeoutMs` == 0 - requeue a message immediately
// `timeoutMs`  > 0 - asynchronously wait for the specified timeout
//     and requeue a message (aka "deferred requeue")
//
func (c *Channel) RequeueMessage(clientID int64, id nsq.MessageID, timeout time.Duration) error {
	// remove from inflight first
	item, err := c.popInFlightMessage(clientID, id)
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
func (c *Channel) AddClient(clientID int64, client Consumer) {
	c.Lock()
	defer c.Unlock()

	_, ok := c.clients[clientID]
	if ok {
		return
	}
	c.clients[clientID] = client
}

// RemoveClient removes a client from the Channel's client list
func (c *Channel) RemoveClient(clientID int64) {
	c.Lock()
	defer c.Unlock()

	_, ok := c.clients[clientID]
	if !ok {
		return
	}
	delete(c.clients, clientID)

	if len(c.clients) == 0 && c.ephemeralChannel == true {
		go c.deleter.Do(func() { c.deleteCallback(c) })
	}
}

func (c *Channel) StartInFlightTimeout(msg *nsq.Message, clientID int64) error {
	now := time.Now()
	value := &inFlightMessage{msg, clientID, now}
	absTs := now.Add(c.context.nsqd.options.msgTimeout).UnixNano()
	item := &pqueue.Item{Value: value, Priority: absTs}
	err := c.pushInFlightMessage(item)
	if err != nil {
		return err
	}
	c.addToInFlightPQ(item)
	return nil
}

func (c *Channel) StartDeferredTimeout(msg *nsq.Message, timeout time.Duration) error {
	absTs := time.Now().Add(timeout).UnixNano()
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
	c.RLock()
	defer c.RUnlock()
	if atomic.LoadInt32(&c.exitFlag) == 1 {
		return errors.New("exiting")
	}
	c.incomingMsgChan <- msg
	atomic.AddUint64(&c.requeueCount, 1)
	return nil
}

// pushInFlightMessage atomically adds a message to the in-flight dictionary
func (c *Channel) pushInFlightMessage(item *pqueue.Item) error {
	c.Lock()
	defer c.Unlock()

	id := item.Value.(*inFlightMessage).msg.Id
	_, ok := c.inFlightMessages[id]
	if ok {
		return errors.New("ID already in flight")
	}
	c.inFlightMessages[id] = item

	return nil
}

// popInFlightMessage atomically removes a message from the in-flight dictionary
func (c *Channel) popInFlightMessage(clientID int64, id nsq.MessageID) (*pqueue.Item, error) {
	c.Lock()
	defer c.Unlock()

	item, ok := c.inFlightMessages[id]
	if !ok {
		return nil, errors.New("ID not in flight")
	}

	if item.Value.(*inFlightMessage).clientID != clientID {
		return nil, errors.New("client does not own message")
	}

	delete(c.inFlightMessages, id)

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

	// TODO: these map lookups are costly
	id := item.Value.(*nsq.Message).Id
	_, ok := c.deferredMessages[id]
	if ok {
		return errors.New("ID already deferred")
	}
	c.deferredMessages[id] = item

	return nil
}

func (c *Channel) popDeferredMessage(id nsq.MessageID) (*pqueue.Item, error) {
	c.Lock()
	defer c.Unlock()

	// TODO: these map lookups are costly
	item, ok := c.deferredMessages[id]
	if !ok {
		return nil, errors.New("ID not deferred")
	}
	delete(c.deferredMessages, id)

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
	var msgBuf bytes.Buffer
	for msg := range c.incomingMsgChan {
		select {
		case c.memoryMsgChan <- msg:
		default:
			err := WriteMessageToBackend(&msgBuf, msg, c.backend)
			if err != nil {
				log.Printf("CHANNEL(%s) ERROR: failed to write message to backend - %s", c.name, err.Error())
				// theres not really much we can do at this point, you're certainly
				// going to lose messages...
			}
		}
	}

	log.Printf("CHANNEL(%s): closing ... router", c.name)
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
		// do an extra check for closed exit before we select on all the memory/backend/exitChan
		// this solves the case where we are closed and something else is draining clientMsgChan into
		// backend. we don't want to reverse that
		if atomic.LoadInt32(&c.exitFlag) == 1 {
			goto exit
		}

		select {
		case msg = <-c.memoryMsgChan:
		case buf = <-c.backend.ReadChan():
			msg, err = nsq.DecodeMessage(buf)
			if err != nil {
				log.Printf("ERROR: failed to decode message - %s", err.Error())
				continue
			}
		case <-c.exitChan:
			goto exit
		}

		msg.Attempts++

		atomic.StoreInt32(&c.bufferedCount, 1)
		c.clientMsgChan <- msg
		atomic.StoreInt32(&c.bufferedCount, 0)
		// the client will call back to mark as in-flight w/ it's info
	}

exit:
	log.Printf("CHANNEL(%s): closing ... messagePump", c.name)
	close(c.clientMsgChan)
}

func (c *Channel) deferredWorker() {
	c.pqWorker(&c.deferredPQ, &c.deferredMutex, func(item *pqueue.Item) {
		msg := item.Value.(*nsq.Message)
		_, err := c.popDeferredMessage(msg.Id)
		if err != nil {
			return
		}
		c.doRequeue(msg)
	})
}

func (c *Channel) inFlightWorker() {
	c.pqWorker(&c.inFlightPQ, &c.inFlightMutex, func(item *pqueue.Item) {
		clientID := item.Value.(*inFlightMessage).clientID
		msg := item.Value.(*inFlightMessage).msg
		_, err := c.popInFlightMessage(clientID, msg.Id)
		if err != nil {
			return
		}
		atomic.AddUint64(&c.timeoutCount, 1)
		client, ok := c.clients[clientID]
		if ok {
			client.TimedOutMessage()
		}
		c.doRequeue(msg)
	})
}

// generic loop (executed in a goroutine) that periodically wakes up to walk
// the priority queue and call the callback
func (c *Channel) pqWorker(pq *pqueue.PriorityQueue, mutex *sync.Mutex, callback func(item *pqueue.Item)) {
	ticker := time.NewTicker(defaultWorkerWait)
	for {
		select {
		case <-ticker.C:
		case <-c.exitChan:
			goto exit
		}
		now := time.Now().UnixNano()
		for {
			mutex.Lock()
			item, _ := pq.PeekAndShift(now)
			mutex.Unlock()

			if item == nil {
				break
			}

			callback(item)
		}
	}

exit:
	log.Printf("CHANNEL(%s): closing ... pqueue worker", c.name)
	ticker.Stop()
}
