package nsqd

import (
	"errors"
	"os"
	//"runtime"
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

	topic1 := nsqd.GetTopic("test", 0)
	nequal(t, nil, topic1)
	equal(t, "test", topic1.GetTopicName())

	topic2 := nsqd.GetTopic("test", 0)
	equal(t, topic1, topic2)

	topic3 := nsqd.GetTopic("test2", 1)
	equal(t, "test2", topic3.GetTopicName())
	nequal(t, topic2, topic3)

	topic1_1 := nsqd.GetTopicIgnPart("test")
	equal(t, "test", topic1_1.GetTopicName())
	equal(t, 0, topic1_1.GetTopicPart())
	topic3_1 := nsqd.GetTopicIgnPart("test2")
	equal(t, "test2", topic3_1.GetTopicName())
	equal(t, 1, topic3_1.GetTopicPart())

}

func TestGetChannel(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic := nsqd.GetTopic("test", 0)

	channel1 := topic.GetChannel("ch1")
	nequal(t, nil, channel1)
	equal(t, "ch1", channel1.name)

	channel2 := topic.GetChannel("ch2")

	equal(t, channel1, topic.channelMap["ch1"])
	equal(t, channel2, topic.channelMap["ch2"])
}

type errorBackendQueue struct{}

func (d *errorBackendQueue) Put([]byte) (BackendOffset, int32, int64, error) {
	return 0, 0, 0, errors.New("never gonna happen")
}
func (d *errorBackendQueue) ReadChan() chan []byte                     { return nil }
func (d *errorBackendQueue) Close() error                              { return nil }
func (d *errorBackendQueue) Delete() error                             { return nil }
func (d *errorBackendQueue) Depth() int64                              { return 0 }
func (d *errorBackendQueue) Empty() error                              { return nil }
func (d *errorBackendQueue) Flush() error                              { return nil }
func (d *errorBackendQueue) GetQueueReadEnd() BackendQueueEnd          { return nil }
func (d *errorBackendQueue) GetQueueWriteEnd() BackendQueueEnd         { return nil }
func (d *errorBackendQueue) ResetWriteEnd(BackendOffset, int64) error  { return nil }
func (d *errorBackendQueue) RollbackWrite(BackendOffset, uint64) error { return nil }

type errorRecoveredBackendQueue struct{ errorBackendQueue }

func (d *errorRecoveredBackendQueue) Put([]byte) (BackendOffset, int32, int64, error) {
	return 0, 0, 0, nil
}

func TestHealth(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts.LogLevel = 2
	opts.MemQueueSize = 2
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic := nsqd.GetTopicIgnPart("test")
	topic.backend = &errorBackendQueue{}

	msg := NewMessage(0, make([]byte, 100))
	_, _, _, _, err := topic.PutMessage(msg)
	nequal(t, err, nil)

	// TODO: health should be topic scoped.
	//equal(t, nsqd.IsHealthy(), false)
	topic.backend = &errorRecoveredBackendQueue{}

	msg = NewMessage(0, make([]byte, 100))
	_, _, _, _, err = topic.PutMessages([]*Message{msg})
	equal(t, err, nil)

	equal(t, nsqd.IsHealthy(), true)
}

func TestDeletes(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic := nsqd.GetTopicIgnPart("test")

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

	topic := nsqd.GetTopic("test", 0)

	channel1 := topic.GetChannel("ch1")
	nequal(t, nil, channel1)

	err := topic.DeleteExistingChannel("ch1")
	equal(t, nil, err)
	equal(t, 0, len(topic.channelMap))

	msg := NewMessage(0, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	_, _, _, _, err = topic.PutMessage(msg)
	time.Sleep(100 * time.Millisecond)
	equal(t, nil, err)
}

func TestTopicBackendMaxMsgSize(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_topic_backend_maxmsgsize" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName, 0)

	equal(t, topic.backend.(*diskQueueWriter).maxMsgSize, int32(opts.MaxMsgSize+minValidMsgLength))
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
		topic := nsqd.GetTopic(topicName, 0)
		msg := NewMessage(0, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa"))
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
	nsqd.GetTopic(topicName, 0).GetChannel(channelName)
	b.StartTimer()

	for i := 0; i <= b.N; i++ {
		topic := nsqd.GetTopic(topicName, 0)
		msg := NewMessage(0, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa"))
		topic.PutMessage(msg)
	}
}
