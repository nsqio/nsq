package main

import (
	"../nsq"
	"github.com/bmizerany/assert"
	"io/ioutil"
	"log"
	"net"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"
)

func TestStartup(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	iterations := 300
	doneExitChan := make(chan int)

	tcpAddr, _ := net.ResolveTCPAddr("tcp", "0.0.0.0:4150")
	httpAddr, _ := net.ResolveTCPAddr("tcp", "0.0.0.0:4151")

	nsqd := NewNSQd(1)
	nsqd.tcpAddr = tcpAddr
	nsqd.httpAddr = httpAddr
	nsqd.memQueueSize = 100
	nsqd.maxBytesPerFile = 10240

	topicName := "nsqd_test" + strconv.Itoa(int(time.Now().Unix()))

	exitChan := make(chan int)
	go func() {
		nsqd.Main()
		<-exitChan
		nsqd.Exit()
		doneExitChan <- 1
	}()

	body := []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccaaaaaaaaaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccaaaaaaaaaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccaaaaaaaaaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
	topic := nsqd.GetTopic(topicName)
	for i := 0; i < iterations; i++ {
		msg := nsq.NewMessage(<-nsqd.idChan, body)
		topic.PutMessage(msg)
	}

	log.Printf("checking that PUTs are finished")
	for {
		count := topic.Depth()
		log.Printf("there are %d and %d", len(topic.memoryMsgChan), topic.backend.Depth())
		if count == int64(iterations-1) { // this will always be one short because the channel buffer is primed
			break
		}
		runtime.Gosched()
	}

	log.Printf("pulling from channel")
	channel1 := topic.GetChannel("ch1")

	// channel should drain topic
	// topic -> channel is buffered, so this may take a few cycles
	for {
		topic_count := topic.Depth()
		chan_count := channel1.Depth()

		log.Printf("%d %d; waiting for channel to drain topic; there are %d and %d in topic and %d and %d in channel",
			topic_count, chan_count,
			len(topic.memoryMsgChan), topic.backend.Depth(), len(channel1.memoryMsgChan), channel1.backend.Depth())
		if topic_count == 0 && chan_count == int64(iterations) {
			break
		}
		runtime.Gosched()
	}

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
		runtime.Gosched()
	}

	exitChan <- 1
	<-doneExitChan

	// start up a new nsqd w/ the same folder

	nsqdd := NewNSQd(1)
	nsqdd.tcpAddr = tcpAddr
	nsqdd.httpAddr = httpAddr
	nsqdd.memQueueSize = 100
	nsqdd.maxBytesPerFile = 10240
	nsqdd.dataPath = nsqd.dataPath

	go func() {
		nsqdd.Main()
		<-exitChan
		nsqdd.Exit()
		doneExitChan <- 1
	}()

	topic = nsqdd.GetTopic(topicName)
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
