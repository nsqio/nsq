package main

import (
	"github.com/bmizerany/assert"
	"bytes"
	"io/ioutil"
	"log"
	"testing"
	"strconv"
)

func TestGetTopic(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	go topicFactory(*memQueueSize)
	
	topic1 := GetTopic("test")
	assert.NotEqual(t, nil, topic1)
	assert.Equal(t, "test", topic1.name)
	
	topic2 := GetTopic("test")
	assert.Equal(t, topic1, topic2)
	
	topic3 := GetTopic("test2")
	assert.Equal(t, "test2", topic3.name)
	assert.NotEqual(t, topic2, topic3)
}

func TestGetChannel(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	go topicFactory(*memQueueSize)
	
	topic := GetTopic("test")
	channel1 := topic.GetChannel("ch1")
	assert.NotEqual(t, nil, channel1)
	assert.Equal(t, "ch1", channel1.name)
	
	channel2 := topic.GetChannel("ch2")
	
	assert.Equal(t, channel1, topic.channelMap["ch1"])
	assert.Equal(t, channel2, topic.channelMap["ch2"])
}

func BenchmarkPut(b *testing.B) {
	b.StopTimer()
	log.SetOutput(ioutil.Discard)
	go topicFactory(b.N)
	go uuidFactory()
	topicName := "testbench" + strconv.Itoa(b.N)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		buf := bytes.NewBuffer(<-uuidChan)
		buf.Write([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
		topic := GetTopic(topicName)
		topic.PutMessage(NewMessage(buf.Bytes()))
	}
}
