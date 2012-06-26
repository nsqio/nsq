package main

import (
	"../nsq"
	"github.com/bmizerany/assert"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"testing"
)

func TestGetTopic(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	nsqd = NewNSQd(1, nil, nil, nil, 10, os.TempDir(), 1024)

	topic1 := nsqd.GetTopic("test")
	assert.NotEqual(t, nil, topic1)
	assert.Equal(t, "test", topic1.name)

	topic2 := nsqd.GetTopic("test")
	assert.Equal(t, topic1, topic2)

	topic3 := nsqd.GetTopic("test2")
	assert.Equal(t, "test2", topic3.name)
	assert.NotEqual(t, topic2, topic3)
}

func TestGetChannel(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	topic := NewTopic("test", 10, os.TempDir(), 1024)
	channel1 := topic.GetChannel("ch1")
	assert.NotEqual(t, nil, channel1)
	assert.Equal(t, "ch1", channel1.name)

	channel2 := topic.GetChannel("ch2")

	assert.Equal(t, channel1, topic.channelMap["ch1"])
	assert.Equal(t, channel2, topic.channelMap["ch2"])
}

func BenchmarkTopicPut(b *testing.B) {
	b.StopTimer()
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)
	topicName := "testbench" + strconv.Itoa(b.N)
	nsqd = NewNSQd(1, nil, nil, nil, int64(b.N), os.TempDir(), 1024)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		topic := nsqd.GetTopic(topicName)
		msg := nsq.NewMessage(<-nsqd.idChan, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa"))
		topic.PutMessage(msg)
	}
}
