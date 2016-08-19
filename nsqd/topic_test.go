package nsqd

import (
	"errors"
	"os"
	//"runtime"
	"github.com/absolute8511/nsq/internal/test"
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
	test.NotNil(t, topic1)
	test.Equal(t, "test", topic1.GetTopicName())

	topic2 := nsqd.GetTopic("test", 0)
	test.Equal(t, topic1, topic2)

	topic3 := nsqd.GetTopic("test2", 1)
	test.Equal(t, "test2", topic3.GetTopicName())
	test.NotEqual(t, topic2, topic3)

	topic1_1 := nsqd.GetTopicIgnPart("test")
	test.Equal(t, "test", topic1_1.GetTopicName())
	test.Equal(t, 0, topic1_1.GetTopicPart())
	topic3_1 := nsqd.GetTopicIgnPart("test2")
	test.Equal(t, "test2", topic3_1.GetTopicName())
	test.Equal(t, 1, topic3_1.GetTopicPart())

}

func TestGetChannel(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic := nsqd.GetTopic("test", 0)

	channel1 := topic.GetChannel("ch1")
	test.NotNil(t, channel1)
	test.Equal(t, "ch1", channel1.name)

	channel2 := topic.GetChannel("ch2")

	test.Equal(t, channel1, topic.channelMap["ch1"])
	test.Equal(t, channel2, topic.channelMap["ch2"])
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
	test.NotNil(t, channel1)
	msg := NewMessage(0, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	for i := 0; i <= 1000; i++ {
		msg.ID = 0
		topic.PutMessage(msg)
	}
	topic1 := nsqd.GetTopic("test", 1)
	err := topic.SetMagicCode(time.Now().UnixNano())
	err = topic1.SetMagicCode(time.Now().UnixNano())
	test.Equal(t, nil, err)
	channel1 = topic1.GetChannel("ch1")
	test.NotNil(t, channel1)
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
	oldMagicFile := path.Join(topic.dataPath, "magic"+strconv.Itoa(topic.partition))
	oldMagicFile1 := path.Join(topic1.dataPath, "magic"+strconv.Itoa(topic1.partition))
	_, err = os.Stat(oldMagicFile)
	test.Equal(t, nil, err)
	_, err = os.Stat(oldMagicFile1)
	test.Equal(t, nil, err)

	removedPath, err := topic.MarkAsRemoved()
	test.Equal(t, nil, err)
	test.Equal(t, 0, len(topic.channelMap))
	// mark as removed should keep the topic base directory
	_, err = os.Stat(origPath)
	test.Equal(t, nil, err)
	// partition data should be removed
	newPath := removedPath
	_, err = os.Stat(newPath)
	defer os.RemoveAll(newPath)
	test.Equal(t, nil, err)
	newName := path.Join(newPath, filepath.Base(oldName))
	newMetaName := path.Join(newPath, filepath.Base(oldMetaName))
	newMagicFile := path.Join(newPath, filepath.Base(oldMagicFile))
	// should keep other topic partition
	_, err = os.Stat(oldName1)
	test.Equal(t, nil, err)
	_, err = os.Stat(oldMetaName1)
	test.Equal(t, nil, err)
	_, err = os.Stat(oldMagicFile1)
	test.Equal(t, nil, err)
	_, err = os.Stat(oldName)
	test.NotNil(t, err)
	_, err = os.Stat(oldMetaName)
	test.NotNil(t, err)
	_, err = os.Stat(oldMagicFile)
	test.NotNil(t, err)
	_, err = os.Stat(newName)
	test.Equal(t, nil, err)
	_, err = os.Stat(newMetaName)
	test.Equal(t, nil, err)
	_, err = os.Stat(newMagicFile)
	test.Equal(t, nil, err)
}

func TestDeletes(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic := nsqd.GetTopicIgnPart("test")
	oldMagicFile := path.Join(topic.dataPath, "magic"+strconv.Itoa(topic.partition))

	channel1 := topic.GetChannel("ch1")
	test.NotNil(t, channel1)

	err := topic.SetMagicCode(time.Now().UnixNano())
	_, err = os.Stat(oldMagicFile)
	test.Equal(t, nil, err)
	err = topic.DeleteExistingChannel("ch1")
	test.Equal(t, nil, err)
	test.Equal(t, 0, len(topic.channelMap))

	channel2 := topic.GetChannel("ch2")
	test.NotNil(t, channel2)

	err = nsqd.DeleteExistingTopic("test", topic.GetTopicPart())
	test.Equal(t, nil, err)
	test.Equal(t, 0, len(topic.channelMap))
	test.Equal(t, 0, len(nsqd.topicMap))
	_, err = os.Stat(oldMagicFile)
	test.NotNil(t, err)
}

func TestDeleteLast(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic := nsqd.GetTopic("test", 0)

	channel1 := topic.GetChannel("ch1")
	test.NotNil(t, channel1)

	err := topic.DeleteExistingChannel("ch1")
	test.Nil(t, err)
	test.Equal(t, 0, len(topic.channelMap))

	msg := NewMessage(0, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	_, _, _, _, err = topic.PutMessage(msg)
	time.Sleep(100 * time.Millisecond)
	test.Nil(t, err)
}

func TestTopicBackendMaxMsgSize(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_topic_backend_maxmsgsize" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName, 0)

	test.Equal(t, topic.backend.maxMsgSize, int32(opts.MaxMsgSize+minValidMsgLength))
}

func TestTopicPutChannelWait(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic := nsqd.GetTopic("test", 0)
	topic.autoCommit = 1
	topic.syncEvery = 10

	channel := topic.GetChannel("ch")
	test.NotNil(t, channel)
	msg := NewMessage(0, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	for i := 0; i <= 10; i++ {
		msg.ID = 0
		topic.PutMessage(msg)
	}
	topic.ForceFlush()
	test.Equal(t, topic.backend.GetQueueReadEnd(), topic.backend.GetQueueWriteEnd())
	test.Equal(t, topic.backend.GetQueueReadEnd(), channel.GetChannelEnd())
	for i := 0; i <= 10; i++ {
		select {
		case outMsg := <-channel.clientMsgChan:
			test.Equal(t, msg.Body, outMsg.Body)
			channel.ConfirmBackendQueue(outMsg)
		case <-time.After(time.Second):
			t.Fatalf("should read message in channel")
		}
	}
	test.Equal(t, true, channel.IsWaitingMoreData())
	test.Equal(t, topic.backend.GetQueueReadEnd(), channel.GetChannelEnd())
	msg.ID = 0
	topic.PutMessage(msg)
	test.Equal(t, false, channel.IsWaitingMoreData())
	test.Equal(t, topic.backend.GetQueueReadEnd(), topic.backend.GetQueueWriteEnd())
	test.Equal(t, topic.backend.GetQueueReadEnd(), channel.GetChannelEnd())
	select {
	case outMsg := <-channel.clientMsgChan:
		test.Equal(t, msg.Body, outMsg.Body)
		channel.ConfirmBackendQueue(outMsg)
	case <-time.After(time.Second):
		t.Fatalf("should read the message in channel")
	}
	test.Equal(t, true, channel.IsWaitingMoreData())
	msg.ID = 0
	topic.PutMessage(msg)
	test.Equal(t, false, channel.IsWaitingMoreData())
	test.Equal(t, topic.backend.GetQueueReadEnd(), topic.backend.GetQueueWriteEnd())
	test.Equal(t, topic.backend.GetQueueReadEnd(), channel.GetChannelEnd())
	msg.ID = 0
	topic.PutMessage(msg)
	test.NotEqual(t, topic.backend.GetQueueReadEnd(), topic.backend.GetQueueWriteEnd())
	test.Equal(t, topic.backend.GetQueueReadEnd(), channel.GetChannelEnd())
}

func TestTopicCleanOldData(t *testing.T) {
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
