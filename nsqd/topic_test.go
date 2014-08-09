package nsqd

import (
	"errors"
	"runtime"
	"strconv"
	"testing"
	"time"
)

func TestGetTopic(t *testing.T) {
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	topic1 := nsqd.GetTopic("test")
	nequal(t, nil, topic1)
	equal(t, "test", topic1.name)

	topic2 := nsqd.GetTopic("test")
	equal(t, topic1, topic2)

	topic3 := nsqd.GetTopic("test2")
	equal(t, "test2", topic3.name)
	nequal(t, topic2, topic3)
}

func TestGetChannel(t *testing.T) {
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	topic := nsqd.GetTopic("test")

	channel1 := topic.GetChannel("ch1")
	nequal(t, nil, channel1)
	equal(t, "ch1", channel1.name)

	channel2 := topic.GetChannel("ch2")

	equal(t, channel1, topic.channelMap["ch1"])
	equal(t, channel2, topic.channelMap["ch2"])
}

func TestHealth(t *testing.T) {
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	topic := nsqd.GetTopic("test")

	msg := NewMessage(<-nsqd.idChan, make([]byte, 100))
	err := topic.PutMessage(msg)
	equal(t, err, nil)

	msg = NewMessage(<-nsqd.idChan, make([]byte, 100))
	err = topic.PutMessages([]*Message{msg})
	equal(t, err, nil)

	nsqd.SetHealth(errors.New("broken"))

	msg = NewMessage(<-nsqd.idChan, make([]byte, 100))
	err = topic.PutMessage(msg)
	nequal(t, err, nil)

	msg = NewMessage(<-nsqd.idChan, make([]byte, 100))
	err = topic.PutMessages([]*Message{msg})
	nequal(t, err, nil)
}

func TestDeletes(t *testing.T) {
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	topic := nsqd.GetTopic("test")

	channel1 := topic.GetChannel("ch1")
	nequal(t, nil, channel1)

	err := topic.DeleteExistingChannel("ch1")
	equal(t, nil, err)
	equal(t, 0, len(topic.channelMap))

	channel2 := topic.GetChannel("ch2")
	nequal(t, nil, channel2)

	err = nsqd.DeleteExistingTopic("test")
	equal(t, nil, err)
	equal(t, 0, len(topic.channelMap))
	equal(t, 0, len(nsqd.topicMap))
}

func TestDeleteLast(t *testing.T) {
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	topic := nsqd.GetTopic("test")

	channel1 := topic.GetChannel("ch1")
	nequal(t, nil, channel1)

	err := topic.DeleteExistingChannel("ch1")
	equal(t, nil, err)
	equal(t, 0, len(topic.channelMap))

	msg := NewMessage(<-nsqd.idChan, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	err = topic.PutMessage(msg)
	time.Sleep(100 * time.Millisecond)
	equal(t, nil, err)
	equal(t, topic.Depth(), int64(1))
}

func TestPause(t *testing.T) {
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	topicName := "test_topic_pause" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	err := topic.Pause()
	equal(t, err, nil)

	channel := topic.GetChannel("ch1")
	nequal(t, channel, nil)

	msg := NewMessage(<-nsqd.idChan, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	err = topic.PutMessage(msg)
	equal(t, err, nil)

	time.Sleep(15 * time.Millisecond)

	equal(t, topic.Depth(), int64(1))
	equal(t, channel.Depth(), int64(0))

	err = topic.UnPause()
	equal(t, err, nil)

	time.Sleep(15 * time.Millisecond)

	equal(t, topic.Depth(), int64(0))
	equal(t, channel.Depth(), int64(1))
}

func BenchmarkTopicPut(b *testing.B) {
	b.StopTimer()
	topicName := "bench_topic_put" + strconv.Itoa(b.N)
	opts := NewNSQDOptions()
	opts.Logger = nil
	opts.MemQueueSize = int64(b.N)
	_, _, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()
	b.StartTimer()

	for i := 0; i <= b.N; i++ {
		topic := nsqd.GetTopic(topicName)
		msg := NewMessage(<-nsqd.idChan, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa"))
		topic.PutMessage(msg)
	}
}

func BenchmarkTopicToChannelPut(b *testing.B) {
	b.StopTimer()
	topicName := "bench_topic_to_channel_put" + strconv.Itoa(b.N)
	channelName := "bench"
	opts := NewNSQDOptions()
	opts.Logger = nil
	opts.MemQueueSize = int64(b.N)
	_, _, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()
	channel := nsqd.GetTopic(topicName).GetChannel(channelName)
	b.StartTimer()

	for i := 0; i <= b.N; i++ {
		topic := nsqd.GetTopic(topicName)
		msg := NewMessage(<-nsqd.idChan, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa"))
		topic.PutMessage(msg)
	}

	for {
		if len(channel.memoryMsgChan) == b.N {
			break
		}
		runtime.Gosched()
	}
}
