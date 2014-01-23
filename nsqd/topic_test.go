package main

import (
	"github.com/bitly/go-nsq"
	"github.com/bmizerany/assert"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"
)

func TestGetTopic(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	nsqd := NewNSQD(NewNSQDOptions())
	defer nsqd.Exit()

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

	nsqd := NewNSQD(NewNSQDOptions())
	defer nsqd.Exit()

	topic := nsqd.GetTopic("test")

	channel1 := topic.GetChannel("ch1")
	assert.NotEqual(t, nil, channel1)
	assert.Equal(t, "ch1", channel1.name)

	channel2 := topic.GetChannel("ch2")

	assert.Equal(t, channel1, topic.channelMap["ch1"])
	assert.Equal(t, channel2, topic.channelMap["ch2"])
}

func TestDeletes(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	nsqd := NewNSQD(NewNSQDOptions())
	defer nsqd.Exit()

	topic := nsqd.GetTopic("test")

	channel1 := topic.GetChannel("ch1")
	assert.NotEqual(t, nil, channel1)

	err := topic.DeleteExistingChannel("ch1")
	assert.Equal(t, nil, err)
	assert.Equal(t, 0, len(topic.channelMap))

	channel2 := topic.GetChannel("ch2")
	assert.NotEqual(t, nil, channel2)

	err = nsqd.DeleteExistingTopic("test")
	assert.Equal(t, nil, err)
	assert.Equal(t, 0, len(topic.channelMap))
	assert.Equal(t, 0, len(nsqd.topicMap))
}

func TestDeleteLast(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	nsqd := NewNSQD(NewNSQDOptions())
	defer nsqd.Exit()

	topic := nsqd.GetTopic("test")

	channel1 := topic.GetChannel("ch1")
	assert.NotEqual(t, nil, channel1)

	err := topic.DeleteExistingChannel("ch1")
	assert.Equal(t, nil, err)
	assert.Equal(t, 0, len(topic.channelMap))

	msg := nsq.NewMessage(<-nsqd.idChan, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	err = topic.PutMessage(msg)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, nil, err)
	assert.Equal(t, topic.Depth(), int64(1))
}

func TestPause(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	nsqd := NewNSQD(NewNSQDOptions())
	defer nsqd.Exit()

	topicName := "test_topic_pause" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	err := topic.Pause()
	assert.Equal(t, err, nil)

	channel := topic.GetChannel("ch1")
	assert.NotEqual(t, channel, nil)

	msg := nsq.NewMessage(<-nsqd.idChan, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	err = topic.PutMessage(msg)
	assert.Equal(t, err, nil)

	time.Sleep(15 * time.Millisecond)

	assert.Equal(t, topic.Depth(), int64(1))
	assert.Equal(t, channel.Depth(), int64(0))

	err = topic.UnPause()
	assert.Equal(t, err, nil)

	time.Sleep(15 * time.Millisecond)

	assert.Equal(t, topic.Depth(), int64(0))
	assert.Equal(t, channel.Depth(), int64(1))
}

func BenchmarkTopicPut(b *testing.B) {
	b.StopTimer()
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)
	topicName := "bench_topic_put" + strconv.Itoa(b.N)
	options := NewNSQDOptions()
	options.MemQueueSize = int64(b.N)
	nsqd := NewNSQD(options)
	defer nsqd.Exit()
	b.StartTimer()

	for i := 0; i <= b.N; i++ {
		topic := nsqd.GetTopic(topicName)
		msg := nsq.NewMessage(<-nsqd.idChan, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa"))
		topic.PutMessage(msg)
	}
}

func BenchmarkTopicToChannelPut(b *testing.B) {
	b.StopTimer()
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)
	topicName := "bench_topic_to_channel_put" + strconv.Itoa(b.N)
	channelName := "bench"
	options := NewNSQDOptions()
	options.MemQueueSize = int64(b.N)
	nsqd := NewNSQD(options)
	defer nsqd.Exit()
	channel := nsqd.GetTopic(topicName).GetChannel(channelName)
	b.StartTimer()

	for i := 0; i <= b.N; i++ {
		topic := nsqd.GetTopic(topicName)
		msg := nsq.NewMessage(<-nsqd.idChan, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa"))
		topic.PutMessage(msg)
	}

	for {
		if len(channel.memoryMsgChan) == b.N {
			break
		}
		runtime.Gosched()
	}
}
