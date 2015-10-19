package nsqd

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"
)

func TestGetTopic(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
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
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic := nsqd.GetTopic("test")

	channel1 := topic.GetChannel("ch1")
	nequal(t, nil, channel1)
	equal(t, "ch1", channel1.name)

	channel2 := topic.GetChannel("ch2")

	equal(t, channel1, topic.channelMap["ch1"])
	equal(t, channel2, topic.channelMap["ch2"])
}

type errorBackendQueue struct{}

func (d *errorBackendQueue) Put([]byte) error      { return errors.New("never gonna happen") }
func (d *errorBackendQueue) ReadChan() chan []byte { return nil }
func (d *errorBackendQueue) Close() error          { return nil }
func (d *errorBackendQueue) Delete() error         { return nil }
func (d *errorBackendQueue) Depth() int64          { return 0 }
func (d *errorBackendQueue) Empty() error          { return nil }

type errorRecoveredBackendQueue struct{ errorBackendQueue }

func (d *errorRecoveredBackendQueue) Put([]byte) error { return nil }

func TestHealth(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts.MemQueueSize = 2
	_, httpAddr, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic := nsqd.GetTopic("test")
	topic.backend = &errorBackendQueue{}

	msg := NewMessage(<-nsqd.idChan, make([]byte, 100))
	err := topic.PutMessage(msg)
	equal(t, err, nil)

	msg = NewMessage(<-nsqd.idChan, make([]byte, 100))
	err = topic.PutMessages([]*Message{msg})
	equal(t, err, nil)

	msg = NewMessage(<-nsqd.idChan, make([]byte, 100))
	err = topic.PutMessage(msg)
	nequal(t, err, nil)

	msg = NewMessage(<-nsqd.idChan, make([]byte, 100))
	err = topic.PutMessages([]*Message{msg})
	nequal(t, err, nil)

	url := fmt.Sprintf("http://%s/ping", httpAddr)
	resp, err := http.Get(url)
	equal(t, err, nil)
	equal(t, resp.StatusCode, 500)
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	equal(t, string(body), "NOK - never gonna happen")

	topic.backend = &errorRecoveredBackendQueue{}

	msg = NewMessage(<-nsqd.idChan, make([]byte, 100))
	err = topic.PutMessages([]*Message{msg})
	equal(t, err, nil)

	resp, err = http.Get(url)
	equal(t, err, nil)
	equal(t, resp.StatusCode, 200)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	equal(t, string(body), "OK")
}

func TestDeletes(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
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
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
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
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
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

func TestTopicBackendMaxMsgSize(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_topic_backend_maxmsgsize" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)

	equal(t, topic.backend.(*diskQueue).maxMsgSize, int32(opts.MaxMsgSize+minValidMsgLength))
}

func BenchmarkTopicPut(b *testing.B) {
	b.StopTimer()
	topicName := "bench_topic_put" + strconv.Itoa(b.N)
	opts := NewOptions()
	opts.Logger = newTestLogger(b)
	opts.MemQueueSize = int64(b.N)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
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
	opts := NewOptions()
	opts.Logger = newTestLogger(b)
	opts.MemQueueSize = int64(b.N)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
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
