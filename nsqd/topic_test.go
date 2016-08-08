package nsqd

import (
	"errors"
	"os"
	//"runtime"
	"path"
	"path/filepath"
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
func (d *errorBackendQueue) GetQueueReadEnd() BackendQueueEnd          { return &diskQueueEndInfo{} }
func (d *errorBackendQueue) GetQueueWriteEnd() BackendQueueEnd         { return &diskQueueEndInfo{} }
func (d *errorBackendQueue) ResetWriteEnd(BackendOffset, int64) error  { return nil }
func (d *errorBackendQueue) RollbackWrite(BackendOffset, uint64) error { return nil }

type errorRecoveredBackendQueue struct{ errorBackendQueue }

func (d *errorRecoveredBackendQueue) Put([]byte) (BackendOffset, int32, int64, error) {
	return 0, 0, 0, nil
}

func TestTopicMarkRemoved(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic := nsqd.GetTopic("test", 0)
	origPath := topic.dataPath

	channel1 := topic.GetChannel("ch1")
	nequal(t, nil, channel1)
	msg := NewMessage(0, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	for i := 0; i <= 1000; i++ {
		msg.ID = 0
		topic.PutMessage(msg)
	}
	topic1 := nsqd.GetTopic("test", 1)
	channel1 = topic1.GetChannel("ch1")
	nequal(t, nil, channel1)
	for i := 0; i <= 1000; i++ {
		msg.ID = 0
		topic1.PutMessage(msg)
	}
	topic.ForceFlush()
	topic1.ForceFlush()
	oldName := topic.backend.fileName(0)
	oldMetaName := topic.backend.metaDataFileName()
	oldName1 := topic1.backend.fileName(0)
	oldMetaName1 := topic1.backend.metaDataFileName()

	curTime1 := strconv.Itoa(int(time.Now().Unix()))
	err := topic.MarkAsRemoved()
	equal(t, nil, err)
	equal(t, 0, len(topic.channelMap))
	curTime2 := strconv.Itoa(int(time.Now().Unix()))
	// mark as removed should keep the topic base directory
	_, err = os.Stat(origPath)
	equal(t, nil, err)
	// partition data should be removed
	newPath1 := origPath + "-removed-" + curTime1
	newPath2 := origPath + "-removed-" + curTime2
	newPath := newPath1
	if _, err := os.Stat(newPath1); err != nil {
		newPath = newPath2
	}

	_, err = os.Stat(newPath)
	defer os.RemoveAll(newPath)
	equal(t, nil, err)
	newName := path.Join(newPath, filepath.Base(oldName))
	newMetaName := path.Join(newPath, filepath.Base(oldMetaName))
	// should keep other topic partition
	_, err = os.Stat(oldName1)
	equal(t, nil, err)
	_, err = os.Stat(oldMetaName1)
	equal(t, nil, err)
	_, err = os.Stat(oldName)
	nequal(t, nil, err)
	_, err = os.Stat(oldMetaName)
	nequal(t, nil, err)
	_, err = os.Stat(newName)
	equal(t, nil, err)
	_, err = os.Stat(newMetaName)
	equal(t, nil, err)
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

	err = nsqd.DeleteExistingTopic("test", topic.GetTopicPart())
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

	equal(t, topic.backend.maxMsgSize, int32(opts.MaxMsgSize+minValidMsgLength))
}

func TestTopicResetWriteEnd(t *testing.T) {
	// TODO
}

func benchmarkTopicPut(b *testing.B, size int) {
	b.StopTimer()
	topicName := "bench_topic_put" + strconv.Itoa(b.N)
	opts := NewOptions()
	opts.Logger = newTestLogger(b)
	opts.MemQueueSize = int64(b.N)
	_, _, nsqd := mustStartNSQD(opts)
	msg := NewMessage(0, make([]byte, size))
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()
	b.StartTimer()

	for i := 0; i <= b.N; i++ {
		topic := nsqd.GetTopic(topicName, 0)
		msg.ID = 0
		topic.PutMessage(msg)
	}
}

func BenchmarkTopicPut16(b *testing.B) {
	benchmarkTopicPut(b, 16)
}

func BenchmarkTopicPut128(b *testing.B) {
	benchmarkTopicPut(b, 128)
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
	msg := NewMessage(0, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa"))

	for i := 0; i <= b.N; i++ {
		topic := nsqd.GetTopic(topicName, 0)
		msg.ID = 0
		topic.PutMessage(msg)
	}
}
