package main

import (
	"../nsq"
	"../util"
	"../util/notify"
	"log"
	"sync/atomic"
)

type Topic struct {
	name                 string
	newChannelChan       chan util.ChanReq
	channelMap           map[string]*Channel
	backend              nsq.BackendQueue
	incomingMessageChan  chan *nsq.Message
	memoryMsgChan        chan *nsq.Message
	routerSyncChan       chan int
	readSyncChan         chan int
	channelWriterStarted int32
}

var topicMap = make(map[string]*Topic)
var newTopicChan = make(chan util.ChanReq)
var topicFactoryStarted = int32(0)

// Topic constructor
func NewTopic(topicName string, inMemSize int, dataPath string) *Topic {
	topic := &Topic{name: topicName,
		newChannelChan:       make(chan util.ChanReq),
		channelMap:           make(map[string]*Channel),
		backend:              nsq.NewDiskQueue(topicName, dataPath),
		incomingMessageChan:  make(chan *nsq.Message, 5),
		memoryMsgChan:        make(chan *nsq.Message, inMemSize),
		routerSyncChan:       make(chan int, 1),
		readSyncChan:         make(chan int),
		channelWriterStarted: 0}
	go topic.Router(inMemSize, dataPath)
	notify.Post("new_topic", topic)
	return topic
}

// GetTopic performs a thread safe operation
// to return a pointer to a Topic object (potentially new)
// see: topicFactory()
func GetTopic(topicName string) *Topic {
	topicChan := make(chan interface{})
	newTopicChan <- util.ChanReq{topicName, topicChan}
	return (<-topicChan).(*Topic)
}

// topicFactory is executed in a goroutine and manages
// the creation/retrieval of Topic objects
func TopicFactory(inMemSize int, dataPath string) {
	var topic *Topic

	if !atomic.CompareAndSwapInt32(&topicFactoryStarted, 0, 1) {
		return
	}

	for {
		topicReq, ok := <-newTopicChan
		if !ok {
			break
		}
		name := topicReq.Variable.(string)
		if topic, ok = topicMap[name]; !ok {
			topic = NewTopic(name, inMemSize, dataPath)
			topicMap[name] = topic
			log.Printf("TOPIC(%s): created", topic.name)
		}
		topicReq.RetChan <- topic
	}
}

// GetChannel performs a thread safe operation
// to return a pointer to a Channel object (potentially new)
// for the given Topic
// see: Topic.Router()
func (t *Topic) GetChannel(channelName string) *Channel {
	channelChan := make(chan interface{})
	t.newChannelChan <- util.ChanReq{channelName, channelChan}
	return (<-channelChan).(*Channel)
}

// PutMessage writes to the appropriate incoming
// message channel
func (t *Topic) PutMessage(msg *nsq.Message) {
	// log.Printf("TOPIC(%s): PutMessage(%s, %s)", t.name, util.UuidToStr(msg.Uuid), string(msg.Body))
	t.incomingMessageChan <- msg
}

// MessagePump selects over the in-memory and backend queue and 
// writes messages to every channel for this topic, synchronizing
// with the channel router
func (t *Topic) MessagePump() {
	var msg *nsq.Message

	exitChan := make(chan interface{})
	notify.Observe(t.name+".topic_close", exitChan)
	for {
		select {
		case msg = <-t.memoryMsgChan:
		case <-t.backend.ReadReadyChan():
			buf, err := t.backend.Get()
			if err != nil {
				log.Printf("ERROR: t.backend.Get() - %s", err.Error())
				continue
			}
			msg, err = nsq.DecodeMessage(buf)
			if err != nil {
				log.Printf("ERROR: failed to decode message - %s", err.Error())
				continue
			}
		case <-exitChan:
			notify.Ignore(t.name+".topic_close", exitChan)
			return
		}

		t.readSyncChan <- 1
		log.Printf("TOPIC(%s): channelMap %#v", t.name, t.channelMap)
		for _, channel := range t.channelMap {
			// copy the message because each channel
			// needs a unique instance
			chanMsg := nsq.NewMessage(msg.Uuid, msg.Body)
			chanMsg.Timestamp = msg.Timestamp
			go channel.PutMessage(chanMsg)
		}
		t.routerSyncChan <- 1
	}
}

// Router handles muxing of Topic messages including
// creation of new Channel objects, proxying messages
// to memory or backend, and synchronizing reads
func (t *Topic) Router(inMemSize int, dataPath string) {
	var msg *nsq.Message

	exitChan := make(chan interface{})
	notify.Observe(t.name+".topic_close", exitChan)
	for {
		select {
		case channelReq := <-t.newChannelChan:
			name := channelReq.Variable.(string)
			channel, ok := t.channelMap[name]
			if !ok {
				channel = NewChannel(name, inMemSize, dataPath)
				t.channelMap[name] = channel
				log.Printf("TOPIC(%s): new channel(%s)", t.name, channel.name)
			}
			channelReq.RetChan <- channel
			if atomic.CompareAndSwapInt32(&t.channelWriterStarted, 0, 1) {
				go t.MessagePump()
			}
		case msg = <-t.incomingMessageChan:
			select {
			case t.memoryMsgChan <- msg:
				// log.Printf("TOPIC(%s): wrote to messageChan", t.name)
			default:
				data, err := msg.Encode()
				if err != nil {
					log.Printf("ERROR: failed to Encode() message - %s", err.Error())
					// TODO: shrug
					continue
				}
				err = t.backend.Put(data)
				if err != nil {
					log.Printf("ERROR: t.backend.Put() - %s", err.Error())
					// TODO: requeue?
				}
				// log.Printf("TOPIC(%s): wrote to backend", t.name)
			}
		case <-t.readSyncChan:
			// log.Printf("TOPIC(%s): read sync START", t.name)
			<-t.routerSyncChan
			// log.Printf("TOPIC(%s): read sync END", t.name)
		case <-exitChan:
			notify.Ignore(t.name+".topic_close", exitChan)
			return
		}
	}
}

func (t *Topic) Close() error {
	var err error

	log.Printf("TOPIC(%s): closing", t.name)

	notify.Post(t.name+".topic_close", nil)

	for _, channel := range t.channelMap {
		err = channel.Close()
		if err != nil {
			// we need to continue regardless of error to close all the channels
			log.Printf("ERROR: channel(%s) close - %s", channel.name, err.Error())
		}
	}

	err = t.backend.Close()
	if err != nil {
		return err
	}

	return nil
}
