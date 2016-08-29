package nsqd

import (
	"errors"
	"os"
	//"runtime"
	"github.com/absolute8511/glog"
	"github.com/absolute8511/nsq/internal/levellogger"
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
	topic.dynamicConf.AutoCommit = 1
	topic.dynamicConf.SyncEvery = 10

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

func TestTopicCleanOldDataByRetentionSize(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts.MaxBytesPerFile = 1024 * 1024
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic := nsqd.GetTopic("test", 0)
	topic.dynamicConf.AutoCommit = 1
	topic.dynamicConf.SyncEvery = 10
	topic.dynamicConf.RetentionDay = 1

	msgNum := 5000
	channel := topic.GetChannel("ch")
	test.NotNil(t, channel)
	msg := NewMessage(0, make([]byte, 1000))
	msgSize := int32(0)
	for i := 0; i <= msgNum; i++ {
		msg.ID = 0
		_, _, msgSize, _, _ = topic.PutMessage(msg)
	}
	topic.ForceFlush()

	fileNum := topic.backend.diskWriteEnd.EndOffset.FileNum
	test.Equal(t, int64(0), topic.backend.GetQueueReadStart().(*diskQueueEndInfo).EndOffset.FileNum)

	test.Equal(t, true, fileNum >= 4)
	for i := 0; i < 100; i++ {
		msg := <-channel.clientMsgChan
		channel.ConfirmBackendQueue(msg)
	}

	topic.TryCleanOldData(1, false, nil)
	// should not clean not consumed data
	test.Equal(t, int64(0), topic.backend.GetQueueReadStart().(*diskQueueEndInfo).EndOffset.FileNum)
	startFileName := topic.backend.fileName(0)
	fStat, err := os.Stat(startFileName)
	test.Nil(t, err)
	fileSize := fStat.Size()
	fileCnt := fileSize / int64(msgSize)

	for i := 0; i < msgNum-100; i++ {
		msg := <-channel.clientMsgChan
		channel.ConfirmBackendQueue(msg)
	}
	topic.TryCleanOldData(1024*1024*2, false, nil)
	test.Equal(t, int64(2), topic.backend.GetQueueReadStart().(*diskQueueEndInfo).EndOffset.FileNum)
	startFileName = topic.backend.fileName(0)
	_, err = os.Stat(startFileName)
	test.NotNil(t, err)
	test.Equal(t, true, os.IsNotExist(err))
	startFileName = topic.backend.fileName(1)
	_, err = os.Stat(startFileName)
	test.NotNil(t, err)
	test.Equal(t, true, os.IsNotExist(err))
	test.Equal(t, BackendOffset(2*fileSize), topic.backend.GetQueueReadStart().Offset())
	test.Equal(t, 2*fileCnt, topic.backend.GetQueueReadStart().TotalMsgCnt())

	topic.TryCleanOldData(1, false, nil)

	// should keep at least 2 files
	test.Equal(t, fileNum-1, topic.backend.GetQueueReadStart().(*diskQueueEndInfo).EndOffset.FileNum)
	test.Equal(t, BackendOffset((fileNum-1)*fileSize), topic.backend.GetQueueReadStart().Offset())
	test.Equal(t, (fileNum-1)*fileCnt, topic.backend.GetQueueReadStart().TotalMsgCnt())
	for i := 0; i < int(fileNum)-1; i++ {
		startFileName = topic.backend.fileName(int64(i))
		_, err = os.Stat(startFileName)
		test.NotNil(t, err)
		test.Equal(t, true, os.IsNotExist(err))
	}
}

func TestTopicCleanOldDataByRetentionDay(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts.MaxBytesPerFile = 1024 * 1024
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic := nsqd.GetTopic("test", 0)
	topic.dynamicConf.AutoCommit = 1
	topic.dynamicConf.SyncEvery = 10

	msgNum := 5000
	channel := topic.GetChannel("ch")
	test.NotNil(t, channel)
	msg := NewMessage(0, make([]byte, 1000))
	msg.Timestamp = time.Now().Add(-1 * time.Hour * time.Duration(24*4)).UnixNano()
	msgSize := int32(0)
	var dend BackendQueueEnd
	for i := 0; i <= msgNum; i++ {
		msg.ID = 0
		_, _, msgSize, dend, _ = topic.PutMessage(msg)
		msg.Timestamp = time.Now().Add(-1 * time.Hour * 24 * time.Duration(4-dend.(*diskQueueEndInfo).EndOffset.FileNum)).UnixNano()
	}
	topic.ForceFlush()

	fileNum := topic.backend.diskWriteEnd.EndOffset.FileNum
	test.Equal(t, int64(0), topic.backend.GetQueueReadStart().(*diskQueueEndInfo).EndOffset.FileNum)

	test.Equal(t, true, fileNum >= 4)
	for i := 0; i < 100; i++ {
		msg := <-channel.clientMsgChan
		channel.ConfirmBackendQueue(msg)
	}

	topic.dynamicConf.RetentionDay = 1
	topic.TryCleanOldData(0, false, nil)
	// should not clean not consumed data
	test.Equal(t, int64(0), topic.backend.GetQueueReadStart().(*diskQueueEndInfo).EndOffset.FileNum)
	startFileName := topic.backend.fileName(0)
	fStat, err := os.Stat(startFileName)
	test.Nil(t, err)
	fileSize := fStat.Size()
	fileCnt := fileSize / int64(msgSize)

	for i := 0; i < msgNum-100; i++ {
		msg := <-channel.clientMsgChan
		channel.ConfirmBackendQueue(msg)
	}
	topic.dynamicConf.RetentionDay = 2
	topic.TryCleanOldData(0, false, nil)
	test.Equal(t, int64(2), topic.backend.GetQueueReadStart().(*diskQueueEndInfo).EndOffset.FileNum)
	startFileName = topic.backend.fileName(0)
	_, err = os.Stat(startFileName)
	test.NotNil(t, err)
	test.Equal(t, true, os.IsNotExist(err))
	startFileName = topic.backend.fileName(1)
	_, err = os.Stat(startFileName)
	test.NotNil(t, err)
	test.Equal(t, true, os.IsNotExist(err))
	test.Equal(t, BackendOffset(2*fileSize), topic.backend.GetQueueReadStart().Offset())
	test.Equal(t, 2*fileCnt, topic.backend.GetQueueReadStart().TotalMsgCnt())

	topic.dynamicConf.RetentionDay = 1
	topic.TryCleanOldData(0, false, nil)

	// should keep at least 2 files
	test.Equal(t, fileNum-1, topic.backend.GetQueueReadStart().(*diskQueueEndInfo).EndOffset.FileNum)
	test.Equal(t, BackendOffset((fileNum-1)*fileSize), topic.backend.GetQueueReadStart().Offset())
	test.Equal(t, (fileNum-1)*fileCnt, topic.backend.GetQueueReadStart().TotalMsgCnt())
	for i := 0; i < int(fileNum)-1; i++ {
		startFileName = topic.backend.fileName(int64(i))
		_, err = os.Stat(startFileName)
		test.NotNil(t, err)
		test.Equal(t, true, os.IsNotExist(err))
	}
}

func TestTopicResetWithQueueStart(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	if testing.Verbose() {
		opts.Logger = &levellogger.GLogger{}
		opts.LogLevel = 3
		glog.SetFlags(0, "", "", true, true, 1)
		glog.StartWorker(time.Second)
	}
	opts.MaxBytesPerFile = 1024 * 1024
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic := nsqd.GetTopic("test", 0)
	topic.dynamicConf.AutoCommit = 1
	topic.dynamicConf.SyncEvery = 10

	msgNum := 5000
	channel := topic.GetChannel("ch")
	test.NotNil(t, channel)
	msg := NewMessage(0, make([]byte, 1000))
	msg.Timestamp = time.Now().Add(-1 * time.Hour * time.Duration(24*4)).UnixNano()
	msgSize := int32(0)
	var dend BackendQueueEnd
	for i := 0; i <= msgNum; i++ {
		msg.ID = 0
		_, _, msgSize, dend, _ = topic.PutMessage(msg)
		msg.Timestamp = time.Now().Add(-1 * time.Hour * 24 * time.Duration(4-dend.(*diskQueueEndInfo).EndOffset.FileNum)).UnixNano()
	}
	topic.ForceFlush()

	fileNum := topic.backend.diskWriteEnd.EndOffset.FileNum
	test.Equal(t, int64(0), topic.backend.GetQueueReadStart().(*diskQueueEndInfo).EndOffset.FileNum)

	test.Equal(t, true, fileNum >= 4)
	nsqLog.Warningf("reading the topic %v backend ", topic.GetFullName())
	for i := 0; i < 100; i++ {
		msg := <-channel.clientMsgChan
		channel.ConfirmBackendQueue(msg)
	}

	topic.dynamicConf.RetentionDay = 2

	oldEnd := topic.backend.GetQueueWriteEnd().(*diskQueueEndInfo)
	// reset with new start
	resetStart := &diskQueueEndInfo{}
	resetStart.virtualEnd = topic.backend.GetQueueWriteEnd().Offset() + BackendOffset(msgSize*10)
	resetStart.totalMsgCnt = topic.backend.GetQueueWriteEnd().TotalMsgCnt() + 10
	err := topic.ResetBackendWithQueueStartNoLock(int64(resetStart.Offset()), resetStart.TotalMsgCnt())
	test.NotNil(t, err)
	topic.DisableForSlave()
	err = topic.ResetBackendWithQueueStartNoLock(int64(resetStart.Offset()), resetStart.TotalMsgCnt())
	test.Nil(t, err)
	topic.EnableForMaster()

	nsqLog.Warningf("reset the topic %v backend with queue start: %v", topic.GetFullName(), resetStart)
	test.Equal(t, resetStart.Offset(), BackendOffset(topic.GetQueueReadStart()))
	newEnd := topic.backend.GetQueueWriteEnd().(*diskQueueEndInfo)
	test.Equal(t, resetStart.Offset(), newEnd.Offset())
	test.Equal(t, resetStart.TotalMsgCnt(), newEnd.TotalMsgCnt())
	test.Equal(t, true, newEnd.EndOffset.GreatThan(&oldEnd.EndOffset))
	test.Equal(t, int64(0), newEnd.EndOffset.Pos)
	test.Equal(t, resetStart.Offset(), channel.GetConfirmed().Offset())
	test.Equal(t, resetStart.TotalMsgCnt(), channel.GetChannelEnd().TotalMsgCnt())

	for i := 0; i < msgNum; i++ {
		msg.ID = 0
		_, _, msgSize, _, _ = topic.PutMessage(msg)
	}
	topic.ForceFlush()
	newEnd = topic.backend.GetQueueWriteEnd().(*diskQueueEndInfo)
	test.Equal(t, resetStart.TotalMsgCnt()+int64(msgNum), newEnd.TotalMsgCnt())
	for i := 0; i < 100; i++ {
		msg := <-channel.clientMsgChan
		channel.ConfirmBackendQueue(msg)
		test.Equal(t, msg.offset+msg.rawMoveSize, channel.GetConfirmed().Offset())
	}

	// reset with old start
	topic.DisableForSlave()
	err = topic.ResetBackendWithQueueStartNoLock(int64(resetStart.Offset()), resetStart.TotalMsgCnt())
	test.Nil(t, err)

	topic.EnableForMaster()
	test.Equal(t, resetStart.Offset(), BackendOffset(topic.GetQueueReadStart()))
	newEnd = topic.backend.GetQueueWriteEnd().(*diskQueueEndInfo)
	test.Equal(t, resetStart.Offset(), newEnd.Offset())
	test.Equal(t, resetStart.TotalMsgCnt(), newEnd.TotalMsgCnt())
	test.Equal(t, true, newEnd.EndOffset.GreatThan(&oldEnd.EndOffset))
	test.Equal(t, int64(0), newEnd.EndOffset.Pos)
	test.Equal(t, resetStart.Offset(), channel.GetConfirmed().Offset())
	test.Equal(t, resetStart.TotalMsgCnt(), channel.GetChannelEnd().TotalMsgCnt())
	for i := 0; i < msgNum; i++ {
		msg.ID = 0
		_, _, msgSize, dend, _ = topic.PutMessage(msg)
		msg.Timestamp = time.Now().Add(-1 * time.Hour * 24 * time.Duration(4-dend.(*diskQueueEndInfo).EndOffset.FileNum)).UnixNano()
	}
	topic.ForceFlush()
	newEnd = topic.backend.GetQueueWriteEnd().(*diskQueueEndInfo)
	test.Equal(t, resetStart.TotalMsgCnt()+int64(msgNum), newEnd.TotalMsgCnt())
	for i := 0; i < 100; i++ {
		msg := <-channel.clientMsgChan
		channel.ConfirmBackendQueue(msg)
		test.Equal(t, msg.offset+msg.rawMoveSize, channel.GetConfirmed().Offset())
	}
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
