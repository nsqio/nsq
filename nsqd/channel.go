package main

import (
	"bytes"
	"container/heap"
	"errors"
	"github.com/bitly/nsq/nsq"
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
	sync.RWMutex // embed a r/w mutex

	topicName string
	name      string

	notifier Notifier
	options  *nsqdOptions

	backend BackendQueue

	incomingMsgChan chan *nsq.Message
	memoryMsgChan   chan *nsq.Message
	clientMsgChan   chan *nsq.Message
	exitChan        chan int
	waitGroup       util.WaitGroupWrapper
	exitFlag        int32

	// state tracking
	clients          []Consumer
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
	requeueCount  uint64
	messageCount  uint64
	timeoutCount  uint64
	bufferedCount int32
}

type inFlightMessage struct {
	msg    *nsq.Message
	client Consumer
	ts     time.Time
}

// NewChannel creates a new instance of the Channel type and returns a pointer
func NewChannel(topicName string, channelName string, options *nsqdOptions,
	notifier Notifier, deleteCallback func(*Channel)) *Channel {
	// backend names, for uniqueness, automatically include the topic... <topic>:<channel>
	backendName := topicName + ":" + channelName
	c := &Channel{
		topicName:       topicName,
		name:            channelName,
		incomingMsgChan: make(chan *nsq.Message, 1),
		memoryMsgChan:   make(chan *nsq.Message, options.memQueueSize),
		clientMsgChan:   make(chan *nsq.Message),
		exitChan:        make(chan int),
		clients:         make([]Consumer, 0, 5),
		deleteCallback:  deleteCallback,
		notifier:        notifier,
		options:         options,
	}

	c.initPQ()

	if strings.HasSuffix(channelName, "#ephemeral") {
		c.ephemeralChannel = true
		c.backend = NewDummyBackendQueue()
	} else {
		c.backend = NewDiskQueue(backendName, options.dataPath, options.maxBytesPerFile, options.syncEvery)
	}

	go c.messagePump()

	c.waitGroup.Wrap(func() { c.router() })
	c.waitGroup.Wrap(func() { c.deferredWorker() })
	c.waitGroup.Wrap(func() { c.inFlightWorker() })

	go notifier.Notify(c)

	return c
}

func (c *Channel) initPQ() {
	pqSize := int(math.Max(1, float64(c.options.memQueueSize)/10))

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
	err := c.exit(true)
	// since we are explicitly deleting a channel (not just at system exit time)
	// de-register this from the lookupd
	go c.notifier.Notify(c)
	return err
}

// Close cleanly closes the Channel
func (c *Channel) Close() error {
	return c.exit(false)
}

func (c *Channel) exit(deleted bool) error {
	var msgBuf bytes.Buffer

	if !atomic.CompareAndSwapInt32(&c.exitFlag, 0, 1) {
		return errors.New("exiting")
	}

	log.Printf("CHANNEL(%s): closing", c.name)

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
	} else {
		// messagePump is responsible for closing the channel it writes to
		// this will read until its closed (exited)
		for msg := range c.clientMsgChan {
			log.Printf("CHANNEL(%s): recovered buffered message from clientMsgChan", c.name)
			WriteMessageToBackend(&msgBuf, msg, c.backend)
		}

		// write anything leftover to disk
		c.flush()
	}

	return c.backend.Close()
}

func (c *Channel) Empty() error {
	c.Lock()
	defer c.Unlock()

	c.initPQ()
	for _, client := range c.clients {
		client.Empty()
	}

	for {
		select {
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

func (c *Channel) Pause() {
	atomic.StoreInt32(&c.paused, 1)
	c.RLock()
	defer c.RUnlock()
	for _, client := range c.clients {
		client.Pause()
	}
}

func (c *Channel) UnPause() {
	atomic.StoreInt32(&c.paused, 0)
	c.RLock()
	defer c.RUnlock()
	for _, client := range c.clients {
		client.UnPause()
	}
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
func (c *Channel) TouchMessage(client Consumer, id nsq.MessageID) error {
	item, err := c.popInFlightMessage(client, id)
	if err != nil {
		log.Printf("ERROR: failed to touch message(%s) - %s", id, err.Error())
		return err
	}
	c.removeFromInFlightPQ(item)

	ifMsg := item.Value.(*inFlightMessage)
	currentTimeout := time.Unix(0, item.Priority)
	newTimeout := currentTimeout.Add(c.options.msgTimeout)
	if newTimeout.Add(c.options.msgTimeout).Sub(ifMsg.ts) >= c.options.maxMsgTimeout {
		// we would have gone over, set to the max
		newTimeout = ifMsg.ts.Add(c.options.maxMsgTimeout)
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
func (c *Channel) FinishMessage(client Consumer, id nsq.MessageID) error {
	item, err := c.popInFlightMessage(client, id)
	if err != nil {
		log.Printf("ERROR: failed to finish message(%s) - %s", id, err.Error())
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
func (c *Channel) RequeueMessage(client Consumer, id nsq.MessageID, timeout time.Duration) error {
	// remove from inflight first
	item, err := c.popInFlightMessage(client, id)
	if err != nil {
		log.Printf("ERROR: failed to re-queue message(%s) - %s", id, err.Error())
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
func (c *Channel) AddClient(client Consumer) {
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
func (c *Channel) RemoveClient(client Consumer) {
	c.Lock()
	defer c.Unlock()

	if len(c.clients) != 0 {
		finalClients := make([]Consumer, 0, len(c.clients)-1)
		for _, cli := range c.clients {
			if cli != client {
				finalClients = append(finalClients, cli)
			}
		}
		c.clients = finalClients
	}

	if len(c.clients) == 0 && c.ephemeralChannel == true {
		go c.deleter.Do(func() { c.deleteCallback(c) })
	}
}

func (c *Channel) StartInFlightTimeout(msg *nsq.Message, client Consumer) error {
	now := time.Now()
	value := &inFlightMessage{msg, client, now}
	absTs := now.Add(c.options.msgTimeout).UnixNano()
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
func (c *Channel) popInFlightMessage(client Consumer, id nsq.MessageID) (*pqueue.Item, error) {
	c.Lock()
	defer c.Unlock()

	item, ok := c.inFlightMessages[id]
	if !ok {
		return nil, errors.New("ID not in flight")
	}

	if item.Value.(*inFlightMessage).client != client {
		return nil, errors.New("client does not own ID")
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
