package main

import (
	"../nsq"
	"../util"
	"../util/pqueue"
	"bytes"
	"errors"
	"github.com/bitly/go-notify"
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
	messagePumpStarter *sync.Once
	exitChan           chan int
	waitGroup          util.WaitGroupWrapper
	exitFlag           int32
	messageCount       uint64
	options            *nsqdOptions
}

// Topic constructor
func NewTopic(topicName string, options *nsqdOptions) *Topic {
	topic := &Topic{
		name:               topicName,
		channelMap:         make(map[string]*Channel),
		backend:            NewDiskQueue(topicName, options.dataPath, options.maxBytesPerFile, options.syncEvery),
		incomingMsgChan:    make(chan *nsq.Message, 1),
		memoryMsgChan:      make(chan *nsq.Message, options.memQueueSize),
		options:            options,
		exitChan:           make(chan int),
		messagePumpStarter: new(sync.Once),
	}

	topic.waitGroup.Wrap(func() { topic.router() })

	go notify.Post("topic_change", topic)

	return topic
}

func (t *Topic) MemoryChan() chan *nsq.Message {
	return t.memoryMsgChan
}

func (t *Topic) BackendQueue() BackendQueue {
	return t.backend
}

func (t *Topic) InFlight() map[nsq.MessageID]*pqueue.Item {
	return nil
}

func (t *Topic) Deferred() map[nsq.MessageID]*pqueue.Item {
	return nil
}

// Exiting returns a boolean indicating if this topic is closed/exiting
func (t *Topic) Exiting() bool {
	return atomic.LoadInt32(&t.exitFlag) == 1
}

// GetChannel performs a thread safe operation
// to return a pointer to a Channel object (potentially new)
// for the given Topic
func (t *Topic) GetChannel(channelName string) *Channel {
	t.Lock()
	defer t.Unlock()
	return t.getOrCreateChannel(channelName)
}

// this expects the caller to handle locking
func (t *Topic) getOrCreateChannel(channelName string) *Channel {
	channel, ok := t.channelMap[channelName]
	if !ok {
		deleteCallback := func(c *Channel) {
			t.DeleteExistingChannel(c.name)
		}
		channel = NewChannel(t.name, channelName, t.options, deleteCallback)
		t.channelMap[channelName] = channel
		log.Printf("TOPIC(%s): new channel(%s)", t.name, channel.name)
		// start the topic message pump lazily using a `once` on the first channel creation
		t.messagePumpStarter.Do(func() { t.waitGroup.Wrap(func() { t.messagePump() }) })
	}
	return channel
}

func (t *Topic) GetExistingChannel(channelName string) (*Channel, error) {
	t.RLock()
	defer t.RUnlock()
	channel, ok := t.channelMap[channelName]
	if !ok {
		return nil, errors.New("channel does not exist")
	}
	return channel, nil
}

// DeleteExistingChannel removes a channel from the topic only if it exists
func (t *Topic) DeleteExistingChannel(channelName string) error {
	t.Lock()
	channel, ok := t.channelMap[channelName]
	if !ok {
		t.Unlock()
		return errors.New("channel does not exist")
	}
	delete(t.channelMap, channelName)
	// not defered so that we can continue while the channel async closes
	t.Unlock()

	log.Printf("TOPIC(%s): deleting channel %s", t.name, channel.name)

	// delete empties the channel before closing
	// (so that we dont leave any messages around)
	channel.Delete()

	// since we are explicitly deleting a channel (not just at system exit time)
	// de-register this from the lookupd
	go notify.Post("channel_change", channel)

	return nil
}

// PutMessage writes to the appropriate incoming message channel
func (t *Topic) PutMessage(msg *nsq.Message) error {
	t.RLock()
	defer t.RUnlock()
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}
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
		// check if all the channels have been deleted
		if len(t.channelMap) == 0 {
			// put this message back on the queue
			// we need to background because we currently hold the lock
			go func() {
				t.PutMessage(msg)
			}()

			// reset the sync.Once
			t.messagePumpStarter = new(sync.Once)

			t.RUnlock()
			goto exit
		}

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
	var msgBuf bytes.Buffer
	for msg := range t.incomingMsgChan {
		select {
		case t.memoryMsgChan <- msg:
		default:
			err := WriteMessageToBackend(&msgBuf, msg, t)
			if err != nil {
				log.Printf("ERROR: failed to write message to backend - %s", err.Error())
				// theres not really much we can do at this point, you're certainly
				// going to lose messages...
			}
		}
	}

	log.Printf("TOPIC(%s): closing ... router", t.name)
}

// Delete empties the topic and all its channels and closes
func (t *Topic) Delete() error {
	EmptyQueue(t)
	t.Lock()
	for _, channel := range t.channelMap {
		delete(t.channelMap, channel.name)
		channel.Delete()
	}
	t.Unlock()
	return t.Close()
}

func (t *Topic) Close() error {
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}

	log.Printf("TOPIC(%s): closing", t.name)

	// initiate exit
	atomic.StoreInt32(&t.exitFlag, 1)

	close(t.exitChan)
	t.Lock()
	close(t.incomingMsgChan)
	t.Unlock()

	// synchronize the close of router() and messagePump()
	t.waitGroup.Wait()

	// close all the channels
	for _, channel := range t.channelMap {
		err := channel.Close()
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
	return t.backend.Close()
}
