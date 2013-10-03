package main

import (
	"fmt"
	"github.com/bitly/go-nsq"
	"github.com/bitly/go-simplejson"
	"github.com/bmizerany/assert"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strconv"
	"testing"
	"time"
)

func getMetadata(n *NSQd) (*simplejson.Json, error) {
	fn := fmt.Sprintf(path.Join(n.options.dataPath, "nsqd.%d.dat"), n.workerId)
	data, err := ioutil.ReadFile(fn)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("ERROR: failed to read channel metadata from %s - %s", fn, err.Error())
		}
		return nil, err
	}

	js, err := simplejson.NewJson(data)
	if err != nil {
		log.Printf("ERROR: failed to parse metadata - %s", err.Error())
		return nil, err
	}
	return js, nil
}

func TestStartup(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	iterations := 300
	doneExitChan := make(chan int)

	options := NewNsqdOptions()
	options.memQueueSize = 100
	options.maxBytesPerFile = 10240
	_, _, nsqd := mustStartNSQd(options)

	topicName := "nsqd_test" + strconv.Itoa(int(time.Now().Unix()))

	exitChan := make(chan int)
	go func() {
		<-exitChan
		nsqd.Exit()
		doneExitChan <- 1
	}()

	// verify nsqd metadata shows no topics
	err := nsqd.PersistMetadata()
	assert.Equal(t, err, nil)
	metaData, err := getMetadata(nsqd)
	assert.Equal(t, err, nil)
	topics, err := metaData.Get("topics").Array()
	assert.Equal(t, err, nil)
	assert.Equal(t, len(topics), 0)

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

	// make sure metadata shows the topic
	metaData, err = getMetadata(nsqd)
	assert.Equal(t, err, nil)
	topics, err = metaData.Get("topics").Array()
	assert.Equal(t, err, nil)
	assert.Equal(t, len(topics), 1)
	observedTopicName, err := metaData.Get("topics").GetIndex(0).Get("name").String()
	assert.Equal(t, observedTopicName, topicName)
	assert.Equal(t, err, nil)

	exitChan <- 1
	<-doneExitChan

	// start up a new nsqd w/ the same folder

	options = NewNsqdOptions()
	options.memQueueSize = 100
	options.maxBytesPerFile = 10240
	_, _, nsqd = mustStartNSQd(options)

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

	for {
		if channel1.Depth() == int64(iterations/2) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

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
	_, _, nsqd := mustStartNSQd(options)

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
	client := NewClientV2(0, nil, &Context{nsqd})
	ephemeralChannel.AddClient(client.ID, client)

	msg := nsq.NewMessage(<-nsqd.idChan, body)
	topic.PutMessage(msg)
	msg = <-ephemeralChannel.clientMsgChan
	assert.Equal(t, msg.Body, body)

	log.Printf("pulling from channel")
	ephemeralChannel.RemoveClient(client.ID)

	time.Sleep(50 * time.Millisecond)

	assert.Equal(t, len(topic.channelMap), 0)
	exitChan <- 1
	<-doneExitChan
}

func metadataForChannel(n *NSQd, topicIndex int, channelIndex int) *simplejson.Json {
	metadata, _ := getMetadata(n)
	mChannels := metadata.Get("topics").GetIndex(topicIndex).Get("channels")
	return mChannels.GetIndex(channelIndex)
}

func TestPauseMetadata(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	options := NewNsqdOptions()
	_, _, nsqd := mustStartNSQd(options)

	topicName := "pause_metadata" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("ch")

	b, _ := metadataForChannel(nsqd, 0, 0).Get("paused").Bool()
	assert.Equal(t, b, false)

	channel.Pause()

	b, _ = metadataForChannel(nsqd, 0, 0).Get("paused").Bool()
	assert.Equal(t, b, true)

	channel.UnPause()

	b, _ = metadataForChannel(nsqd, 0, 0).Get("paused").Bool()
	assert.Equal(t, b, false)
}
