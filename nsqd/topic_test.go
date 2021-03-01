package nsqd

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/nsqio/nsq/internal/test"
)

func TestGetTopic(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic1, _ := nsqd.GetOrCreateTopic("test")
	test.NotNil(t, topic1)
	test.Equal(t, "test", topic1.name)

	topic2, _ := nsqd.GetOrCreateTopic("test")
	test.Equal(t, topic1, topic2)

	topic3, _ := nsqd.GetOrCreateTopic("test2")
	test.Equal(t, "test2", topic3.name)
	test.NotEqual(t, topic2, topic3)
}

func TestGetChannel(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic, _ := nsqd.GetOrCreateTopic("test")

	channel1, _ := topic.GetOrCreateChannel("ch1")
	test.NotNil(t, channel1)
	test.Equal(t, "ch1", channel1.name)

	channel2, _ := topic.GetOrCreateChannel("ch2")

	test.Equal(t, channel1, topic.channelMap["ch1"])
	test.Equal(t, channel2, topic.channelMap["ch2"])
}

type errorBackendQueue struct{}

func (d *errorBackendQueue) Put([]byte) error        { return errors.New("never gonna happen") }
func (d *errorBackendQueue) ReadChan() <-chan []byte { return nil }
func (d *errorBackendQueue) Close() error            { return nil }
func (d *errorBackendQueue) Delete() error           { return nil }
func (d *errorBackendQueue) Depth() int64            { return 0 }
func (d *errorBackendQueue) Empty() error            { return nil }

type errorRecoveredBackendQueue struct{ errorBackendQueue }

func (d *errorRecoveredBackendQueue) Put([]byte) error { return nil }

func TestHealth(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.MemQueueSize = 2
	_, httpAddr, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic, _ := nsqd.GetOrCreateTopic("test")
	topic.backend = &errorBackendQueue{}

	msg := NewMessage(topic.GenerateID(), make([]byte, 100))
	err := topic.PutMessage(msg)
	test.Nil(t, err)

	msg = NewMessage(topic.GenerateID(), make([]byte, 100))
	err = topic.PutMessages([]*Message{msg})
	test.Nil(t, err)

	msg = NewMessage(topic.GenerateID(), make([]byte, 100))
	err = topic.PutMessage(msg)
	test.NotNil(t, err)

	msg = NewMessage(topic.GenerateID(), make([]byte, 100))
	err = topic.PutMessages([]*Message{msg})
	test.NotNil(t, err)

	url := fmt.Sprintf("http://%s/ping", httpAddr)
	resp, err := http.Get(url)
	test.Nil(t, err)
	test.Equal(t, 500, resp.StatusCode)
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	test.Equal(t, "NOK - never gonna happen", string(body))

	topic.backend = &errorRecoveredBackendQueue{}

	msg = NewMessage(topic.GenerateID(), make([]byte, 100))
	err = topic.PutMessages([]*Message{msg})
	test.Nil(t, err)

	resp, err = http.Get(url)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	test.Equal(t, "OK", string(body))
}

func TestDeletes(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic, _ := nsqd.GetOrCreateTopic("test")

	channel1, _ := topic.GetOrCreateChannel("ch1")
	test.NotNil(t, channel1)

	err := topic.DeleteExistingChannel("ch1")
	test.Nil(t, err)
	test.Equal(t, 0, len(topic.channelMap))

	channel2, _ := topic.GetOrCreateChannel("ch2")
	test.NotNil(t, channel2)

	err = nsqd.DeleteExistingTopic("test")
	test.Nil(t, err)
	test.Equal(t, 0, len(topic.channelMap))
	test.Equal(t, 0, len(nsqd.topicMap))
}

func TestDeleteLast(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic, _ := nsqd.GetOrCreateTopic("test")

	channel1, _ := topic.GetOrCreateChannel("ch1")
	test.NotNil(t, channel1)

	err := topic.DeleteExistingChannel("ch1")
	test.Nil(t, err)
	test.Equal(t, 0, len(topic.channelMap))

	msg := NewMessage(topic.GenerateID(), []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	err = topic.PutMessage(msg)
	time.Sleep(100 * time.Millisecond)
	test.Nil(t, err)
	test.Equal(t, int64(1), topic.Depth())
}

func TestPause(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_topic_pause" + strconv.Itoa(int(time.Now().Unix()))
	topic, _ := nsqd.GetOrCreateTopic(topicName)
	err := topic.Pause()
	test.Nil(t, err)

	channel, _ := topic.GetOrCreateChannel("ch1")
	test.NotNil(t, channel)

	msg := NewMessage(topic.GenerateID(), []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	err = topic.PutMessage(msg)
	test.Nil(t, err)

	time.Sleep(15 * time.Millisecond)

	test.Equal(t, int64(1), topic.Depth())
	test.Equal(t, int64(0), channel.Depth())

	err = topic.UnPause()
	test.Nil(t, err)

	time.Sleep(15 * time.Millisecond)

	test.Equal(t, int64(0), topic.Depth())
	test.Equal(t, int64(1), channel.Depth())
}

func TestDrainEmpty(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic, _ := nsqd.GetOrCreateTopic("drain_topic_empty")

	channel1, _ := topic.GetOrCreateChannel("ch1")
	test.NotNil(t, channel1)
	channel2, _ := topic.GetOrCreateChannel("ch2")
	test.NotNil(t, channel2)
	test.Equal(t, 2, len(topic.channelMap))

	topic.StartDraining()
	time.Sleep(time.Millisecond)

	// topic exiting is slow
	for i := 0; i < 10; i++ {
		t, _ := nsqd.GetExistingTopic("drain_topic_empty")
		if t == nil {
			break
		}
		time.Sleep(time.Millisecond * 50)
	}
	test.Equal(t, 0, len(topic.channelMap))

	topic, _ = nsqd.GetExistingTopic("drain_topic_empty")
	test.Nil(t, topic)
}

func BenchmarkTopicPut(b *testing.B) {
	b.StopTimer()
	topicName := "bench_topic_put" + strconv.Itoa(b.N)
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(b)
	opts.LogLevel = LOG_WARN
	opts.MemQueueSize = int64(b.N)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()
	gf := &test.GUIDFactory{}
	b.StartTimer()

	for i := 0; i <= b.N; i++ {
		topic, _ := nsqd.GetOrCreateTopic(topicName)
		msg := NewMessage(guid(gf.NextMessageID()).Hex(), []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa"))
		topic.PutMessage(msg)
	}
}

func BenchmarkTopicToChannelPut(b *testing.B) {
	b.StopTimer()
	topicName := "bench_topic_to_channel_put" + strconv.Itoa(b.N)
	channelName := "bench"
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(b)
	opts.LogLevel = LOG_WARN
	opts.MemQueueSize = int64(b.N)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()
	topic, _ := nsqd.GetOrCreateTopic(topicName)
	channel, _ := topic.GetOrCreateChannel(channelName)
	gf := &test.GUIDFactory{}
	b.StartTimer()

	for i := 0; i <= b.N; i++ {
		msg := NewMessage(guid(gf.NextMessageID()).Hex(), []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa"))
		topic.PutMessage(msg)
	}

	for {
		if len(channel.memoryMsgChan) == b.N {
			break
		}
		runtime.Gosched()
	}
}

func BenchmarkTopicMessagePump(b *testing.B) {
	b.StopTimer()
	topicName := "bench_topic_put_throughput" + strconv.Itoa(b.N)
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(b)
	opts.LogLevel = LOG_WARN
	opts.MemQueueSize = int64(b.N)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic, _ := nsqd.GetOrCreateTopic(topicName)
	ch, _ := topic.GetOrCreateChannel("ch")
	ctx, cancel := context.WithCancel(context.Background())
	gf := &test.GUIDFactory{}

	var wg sync.WaitGroup
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ch.memoryMsgChan:
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	b.StartTimer()
	for i := 0; i <= b.N; i++ {
		msg := NewMessage(guid(gf.NextMessageID()).Hex(), []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa"))
		topic.PutMessage(msg)
	}
	cancel()
	wg.Wait()
}
