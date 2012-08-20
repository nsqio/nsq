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
	"time"
)

type Topic struct {
	sync.RWMutex
	name               string
	channelMap         map[string]*Channel
	backend            BackendQueue
	incomingMsgChan    chan *nsq.Message
	memoryMsgChan      chan *nsq.Message
	messagePumpStarter sync.Once
	memQueueSize       int64
	dataPath           string
	maxBytesPerFile    int64
	syncEvery          int64
	msgTimeout         time.Duration
	exitChan           chan int
	waitGroup          util.WaitGroupWrapper
	exitFlag           int32
	messageCount       uint64
}

// Topic constructor
func NewTopic(topicName string, memQueueSize int64, dataPath string, maxBytesPerFile int64, syncEvery int64, msgTimeout time.Duration) *Topic {
	topic := &Topic{
		name:            topicName,
		channelMap:      make(map[string]*Channel),
		backend:         NewDiskQueue(topicName, dataPath, maxBytesPerFile, syncEvery),
		incomingMsgChan: make(chan *nsq.Message, 5),
		memoryMsgChan:   make(chan *nsq.Message, memQueueSize),
		memQueueSize:    memQueueSize,
		dataPath:        dataPath,
		maxBytesPerFile: maxBytesPerFile,
		syncEvery:       syncEvery,
		msgTimeout:      msgTimeout,
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
		channel = NewChannel(t.name, channelName, t.memQueueSize, t.dataPath, t.maxBytesPerFile, t.syncEvery, t.msgTimeout)
		t.channelMap[channelName] = channel
		log.Printf("TOPIC(%s): new channel(%s)", t.name, channel.name)
	}
	t.messagePumpStarter.Do(func() { t.waitGroup.Wrap(func() { t.messagePump() }) })

	return channel
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
				log.Printf("ERROR: failed to put msg(%s) to channel(%s) - %s", msg.Id, channel.name, err.Error())
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
