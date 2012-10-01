package main

import (
	"../nsq"
	"../util"
	"../util/pqueue"
	"bitly/notify"
	"errors"
	"log"
	"sync"
	"sync/atomic"
)

type Topic struct {
	sync.RWMutex
	name               string
	channelMap         map[string]*Channel
	backend            BackendQueue
	incomingMsgChan    chan *nsq.Message
	memoryMsgChan      chan *nsq.Message
	messagePumpStarter sync.Once
	exitChan           chan int
	waitGroup          util.WaitGroupWrapper
	exitFlag           int32
	messageCount       uint64
	options            *nsqdOptions
}

// Topic constructor
func NewTopic(topicName string, options *nsqdOptions) *Topic {
	topic := &Topic{
		name:            topicName,
		channelMap:      make(map[string]*Channel),
		backend:         NewDiskQueue(topicName, options.dataPath, options.maxBytesPerFile, options.syncEvery),
		incomingMsgChan: make(chan *nsq.Message, 1),
		memoryMsgChan:   make(chan *nsq.Message, options.memQueueSize),
		options:         options,
		exitChan:        make(chan int),
	}

	topic.waitGroup.Wrap(func() { topic.router() })

	go notify.Post("new_topic", topic)

	return topic
}

func (t *Topic) MemoryChan() chan *nsq.Message {
	return t.memoryMsgChan
}

func (t *Topic) BackendQueue() BackendQueue {
	return t.backend
}

func (c *Topic) InFlight() map[string]*pqueue.Item {
	return nil
}

func (c *Topic) Deferred() map[string]*pqueue.Item {
	return nil
}

// GetChannel performs a thread safe operation
// to return a pointer to a Channel object (potentially new)
// for the given Topic
func (t *Topic) GetChannel(channelName string) *Channel {
	t.Lock()
	defer t.Unlock()

	channel, ok := t.channelMap[channelName]
	if !ok {
		deleteCallback := func(c *Channel) {
			t.DeleteChannel(c)
		}
		channel = NewChannel(t.name, channelName, t.options, deleteCallback)
		t.channelMap[channelName] = channel
		log.Printf("TOPIC(%s): new channel(%s)", t.name, channel.name)
	}
	t.messagePumpStarter.Do(func() { t.waitGroup.Wrap(func() { t.messagePump() }) })

	return channel
}

// HasChannel performs a thread safe operation to check for channel existance
func (t *Topic) HasChannel(channelName string) bool {
	t.RLock()
	defer t.RUnlock()
	_, ok := t.channelMap[channelName]
	return ok
}

// DeleteChannel removes a channel from the topic
// this is generally used to cleanup ephemeral channels
func (t *Topic) DeleteChannel(channel *Channel) {
	t.Lock()
	_, ok := t.channelMap[channel.name]
	if !ok {
		t.Unlock()
		return
	}
	delete(t.channelMap, channel.name)
	// not defered so that the topic can continue while the channel async closes
	t.Unlock()

	// since we are closing in this fashion it's ok to drop messages instead of persisting them
	EmptyQueue(channel)
	channel.Close()
	// since we are explicitly deleting a channel (not just at system exit time)
	// de-register this from the lookupd
	go notify.Post("channel_change", channel)
}

// PutMessage writes to the appropriate incoming
// message channel
func (t *Topic) PutMessage(msg *nsq.Message) error {
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("E_EXITING")
	}
	// TODO: theres still a race condition here when closing incomingMsgChan
	t.incomingMsgChan <- msg
	atomic.AddUint64(&t.messageCount, 1)
	return nil
}

func (t *Topic) Depth() int64 {
	return int64(len(t.memoryMsgChan)) + t.backend.Depth()
}

// messagePump selects over the in-memory and backend queue and 
// writes messages to every channel for this topic
func (t *Topic) messagePump() {
	var msg *nsq.Message
	var buf []byte
	var err error

	for {
		// do an extra check for exit before we select on all the memory/backend/exitChan
		// this solves the case where we are closed and something else is writing into
		// backend. we don't want to reverse that
		if atomic.LoadInt32(&t.exitFlag) == 1 {
			goto exit
		}

		select {
		case msg = <-t.memoryMsgChan:
		case buf = <-t.backend.ReadChan():
			msg, err = nsq.DecodeMessage(buf)
			if err != nil {
				log.Printf("ERROR: failed to decode message - %s", err.Error())
				continue
			}
		case <-t.exitChan:
			goto exit
		}

		t.RLock()
		for _, channel := range t.channelMap {
			// copy the message because each channel
			// needs a unique instance
			chanMsg := nsq.NewMessage(msg.Id, msg.Body)
			chanMsg.Timestamp = msg.Timestamp
			err := channel.PutMessage(chanMsg)
			if err != nil {
				log.Printf("TOPIC(%s) ERROR: failed to put msg(%s) to channel(%s) - %s", t.name, msg.Id, channel.name, err.Error())
			}
		}
		t.RUnlock()
	}

exit:
	log.Printf("TOPIC(%s): closing ... messagePump", t.name)
}

// router handles muxing of Topic messages including
// proxying messages to memory or backend
func (t *Topic) router() {
	for msg := range t.incomingMsgChan {
		select {
		case t.memoryMsgChan <- msg:
		default:
			err := WriteMessageToBackend(msg, t)
			if err != nil {
				log.Printf("ERROR: failed to write message to backend - %s", err.Error())
				// theres not really much we can do at this point, you're certainly
				// going to lose messages...
			}
		}
	}

	log.Printf("TOPIC(%s): closing ... router", t.name)
}

func (t *Topic) Close() error {
	var err error

	log.Printf("TOPIC(%s): closing", t.name)

	// initiate exit
	atomic.AddInt32(&t.exitFlag, 1)
	close(t.exitChan)
	close(t.incomingMsgChan)

	// synchronize the close of router() and messagePump()
	t.waitGroup.Wait()

	// close all the channels
	for _, channel := range t.channelMap {
		err = channel.Close()
		if err != nil {
			// we need to continue regardless of error to close all the channels
			log.Printf("ERROR: channel(%s) close - %s", channel.name, err.Error())
		}
	}

	// write anything leftover to disk
	if len(t.memoryMsgChan) > 0 {
		log.Printf("TOPIC(%s): flushing %d memory messages to backend", t.name, len(t.memoryMsgChan))
	}
	FlushQueue(t)
	err = t.backend.Close()
	if err != nil {
		return err
	}

	return nil
}
