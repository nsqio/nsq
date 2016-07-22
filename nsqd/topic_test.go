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

	"github.com/mreiferson/wal"
	"github.com/nsqio/nsq/internal/test"
)

func TestGetTopic(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic1 := nsqd.GetTopic("test")
	test.NotNil(t, topic1)
	test.Equal(t, "test", topic1.name)

	topic2 := nsqd.GetTopic("test")
	test.Equal(t, topic1, topic2)

	topic3 := nsqd.GetTopic("test2")
	test.Equal(t, "test2", topic3.name)
	test.NotEqual(t, topic2, topic3)
}

func TestGetChannel(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic := nsqd.GetTopic("test")

	channel1 := topic.GetChannel("ch1")
	test.NotNil(t, channel1)
	test.Equal(t, "ch1", channel1.name)

	channel2 := topic.GetChannel("ch2")

	test.Equal(t, channel1, topic.channelMap["ch1"])
	test.Equal(t, channel2, topic.channelMap["ch2"])
}

type errorWAL struct{}

func (d *errorWAL) Append([][]byte, []uint32) (uint64, uint64, error) {
	return 0, 0, errors.New("never gonna happen")
}
func (d *errorWAL) AppendFast([]wal.WriteEntry) (uint64, uint64, error) {
	return 0, 0, errors.New("never gonna happen")
}
func (d *errorWAL) Close() error                             { return nil }
func (d *errorWAL) Delete() error                            { return nil }
func (d *errorWAL) Empty() error                             { return nil }
func (d *errorWAL) Depth() uint64                            { return 0 }
func (d *errorWAL) GetCursor(idx uint64) (wal.Cursor, error) { return nil, nil }

func TestTopicHealth(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.MemQueueSize = 2
	_, httpAddr, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic := nsqd.GetTopic("test")
	topic.wal = &errorWAL{}

	body := make([]byte, 100)

	err := topic.Pub([]wal.WriteEntry{NewEntry(body, 0)})
	test.NotNil(t, err)

	url := fmt.Sprintf("http://%s/ping", httpAddr)
	resp, err := http.Get(url)
	test.Nil(t, err)
	test.Equal(t, 500, resp.StatusCode)
	rbody, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	test.Equal(t, "NOK - never gonna happen", string(rbody))

	topic.wal = wal.NewEphemeral()

	err = topic.Pub([]wal.WriteEntry{NewEntry(body, 0)})
	equal(t, err, nil)

	resp, err = http.Get(url)
	equal(t, err, nil)
	equal(t, resp.StatusCode, 200)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	equal(t, string(body), "OK")
}

func TestTopicDeletes(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic := nsqd.GetTopic("test")

	channel1 := topic.GetChannel("ch1")
	test.NotNil(t, channel1)

	err := topic.DeleteExistingChannel("ch1")
	test.Nil(t, err)
	test.Equal(t, 0, len(topic.channelMap))

	channel2 := topic.GetChannel("ch2")
	test.NotNil(t, channel2)

	err = nsqd.DeleteExistingTopic("test")
	test.Nil(t, err)
	test.Equal(t, 0, len(topic.channelMap))
	test.Equal(t, 0, len(nsqd.topicMap))
}

func TestTopicDeleteLast(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic := nsqd.GetTopic("test")

	channel1 := topic.GetChannel("ch1")
	test.NotNil(t, channel1)

	err := topic.DeleteExistingChannel("ch1")
	test.Nil(t, err)
	test.Equal(t, 0, len(topic.channelMap))

	body := []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa")
	err = topic.Pub([]wal.WriteEntry{NewEntry(body, 0)})
	time.Sleep(100 * time.Millisecond)
	test.Nil(t, err)
	test.Equal(t, uint64(1), topic.Depth())
}

func TestTopicPause(t *testing.T) {
	t.Skipf("TODO: topic pausing")

	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_topic_pause" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	err := topic.Pause()
	test.Nil(t, err)

	channel := topic.GetChannel("ch1")
	test.NotNil(t, channel)

	body := []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa")
	err = topic.Pub([]wal.WriteEntry{NewEntry(body, 0)})
	test.Nil(t, err)

	time.Sleep(15 * time.Millisecond)

	test.Equal(t, uint64(1), topic.Depth())
	test.Equal(t, uint64(0), channel.Depth())

	err = topic.UnPause()
	test.Nil(t, err)

	time.Sleep(15 * time.Millisecond)

	test.Equal(t, uint64(0), topic.Depth())
	test.Equal(t, uint64(1), channel.Depth())
}

// TODO: (WAL) fixme
// func TestTopicBackendMaxMsgSize(t *testing.T) {
// 	opts := NewOptions()
// 	opts.Logger = newTestLogger(t)
// 	_, _, nsqd := mustStartNSQD(opts)
// 	defer os.RemoveAll(opts.DataPath)
// 	defer nsqd.Exit()
//
// 	topicName := "test_topic_backend_maxmsgsize" + strconv.Itoa(int(time.Now().Unix()))
// 	topic := nsqd.GetTopic(topicName)
//
// 	test.Equal(t, int32(opts.MaxMsgSize+minValidMsgLength), topic.backend.(*diskQueue).maxMsgSize)
// }

func BenchmarkTopicPut(b *testing.B) {
	b.StopTimer()
	topicName := "bench_topic_put" + strconv.Itoa(b.N)
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(b)
	opts.MemQueueSize = int64(b.N)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()
	b.StartTimer()

	for i := 0; i <= b.N; i++ {
		topic := nsqd.GetTopic(topicName)
		body := []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa")
		topic.Pub([]wal.WriteEntry{NewEntry(body, 0)})
	}
}

func BenchmarkTopicToChannelPut(b *testing.B) {
	b.StopTimer()
	topicName := "bench_topic_to_channel_put" + strconv.Itoa(b.N)
	channelName := "bench"
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(b)
	opts.MemQueueSize = int64(b.N)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()
	channel := nsqd.GetTopic(topicName).GetChannel(channelName)
	b.StartTimer()

	for i := 0; i <= b.N; i++ {
		topic := nsqd.GetTopic(topicName)
		body := []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa")
		topic.Pub([]wal.WriteEntry{NewEntry(body, 0)})
	}

	for {
		if len(channel.memoryMsgChan) == b.N {
			break
		}
		runtime.Gosched()
	}
}
