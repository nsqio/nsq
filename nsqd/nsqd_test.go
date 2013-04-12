package main

import (
	"github.com/bitly/nsq/nsq"
	"github.com/bmizerany/assert"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestStartup(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	iterations := 300
	doneExitChan := make(chan int)

	options := NewNsqdOptions()
	options.memQueueSize = 100
	options.maxBytesPerFile = 10240
	mustStartNSQd(options)

	topicName := "nsqd_test" + strconv.Itoa(int(time.Now().Unix()))

	exitChan := make(chan int)
	go func() {
		<-exitChan
		nsqd.Exit()
		doneExitChan <- 1
	}()

	body := make([]byte, 256)
	topic := nsqd.GetTopic(topicName)
	for i := 0; i < iterations; i++ {
		msg := nsq.NewMessage(<-nsqd.idChan, body)
		topic.PutMessage(msg)
	}

	log.Printf("pulling from channel")
	channel1 := topic.GetChannel("ch1")

	log.Printf("read %d msgs", iterations/2)
	for i := 0; i < iterations/2; i++ {
		msg := <-channel1.clientMsgChan
		log.Printf("read message %d", i+1)
		assert.Equal(t, msg.Body, body)
	}

	for {
		if channel1.Depth() == int64(iterations/2) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	exitChan <- 1
	<-doneExitChan

	// start up a new nsqd w/ the same folder

	options = NewNsqdOptions()
	options.memQueueSize = 100
	options.maxBytesPerFile = 10240
	mustStartNSQd(options)

	go func() {
		<-exitChan
		nsqd.Exit()
		doneExitChan <- 1
	}()

	topic = nsqd.GetTopic(topicName)
	// should be empty; channel should have drained everything
	count := topic.Depth()
	assert.Equal(t, count, int64(0))

	channel1 = topic.GetChannel("ch1")

	chan_count := channel1.Depth()
	assert.Equal(t, chan_count, int64(iterations/2))

	// read the other half of the messages
	for i := 0; i < iterations/2; i++ {
		msg := <-channel1.clientMsgChan
		log.Printf("read message %d", i+1)
		assert.Equal(t, msg.Body, body)
	}

	// verify we drained things
	assert.Equal(t, len(topic.memoryMsgChan), 0)
	assert.Equal(t, topic.backend.Depth(), int64(0))

	exitChan <- 1
	<-doneExitChan
}

func TestEphemeralChannel(t *testing.T) {
	// a normal channel sticks around after clients disconnect; an ephemeral channel is
	// lazily removed after the last client disconnects
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	options := NewNsqdOptions()
	options.memQueueSize = 100
	mustStartNSQd(options)

	topicName := "ephemeral_test" + strconv.Itoa(int(time.Now().Unix()))
	doneExitChan := make(chan int)

	exitChan := make(chan int)
	go func() {
		<-exitChan
		nsqd.Exit()
		doneExitChan <- 1
	}()

	body := []byte("an_ephemeral_message")
	topic := nsqd.GetTopic(topicName)
	ephemeralChannel := topic.GetChannel("ch1#ephemeral")

	msg := nsq.NewMessage(<-nsqd.idChan, body)
	topic.PutMessage(msg)
	msg = <-ephemeralChannel.clientMsgChan
	assert.Equal(t, msg.Body, body)

	log.Printf("pulling from channel")
	ephemeralChannel.RemoveClient(nil)

	time.Sleep(50 * time.Millisecond)

	assert.Equal(t, len(topic.channelMap), 0)
	exitChan <- 1
	<-doneExitChan
}
