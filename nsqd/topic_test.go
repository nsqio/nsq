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

	topic1 := GetTopic("test", 10, os.TempDir())
	assert.NotEqual(t, nil, topic1)
	assert.Equal(t, "test", topic1.name)

	topic2 := GetTopic("test", 10, os.TempDir())
	assert.Equal(t, topic1, topic2)

	topic3 := GetTopic("test2", 10, os.TempDir())
	assert.Equal(t, "test2", topic3.name)
	assert.NotEqual(t, topic2, topic3)
}

func TestGetChannel(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	topic := GetTopic("test", 10, os.TempDir())
	channel1 := topic.GetChannel("ch1", 10, os.TempDir())
	assert.NotEqual(t, nil, channel1)
	assert.Equal(t, "ch1", channel1.name)

	channel2 := topic.GetChannel("ch2", 10, os.TempDir())

	assert.Equal(t, channel1, topic.channelMap["ch1"])
	assert.Equal(t, channel2, topic.channelMap["ch2"])
}

func BenchmarkPut(b *testing.B) {
	b.StopTimer()
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)
	topicName := "testbench" + strconv.Itoa(b.N)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		topic := GetTopic(topicName, b.N, os.TempDir())
		msg := nsq.NewMessage(<-idChan, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa"))
		topic.PutMessage(msg)
	}
}
