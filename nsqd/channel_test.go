package nsqd

import (
	//"github.com/absolute8511/nsq/internal/levellogger"
	"github.com/Workiva/go-datastructures/augmentedtree"
	"os"
	"strconv"
	"testing"
	"time"
)

// ensure that we can push a message through a topic and get it out of a channel
func TestPutMessage(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts.LogLevel = 3
	opts.SyncEvery = 1
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_put_message" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	channel1 := topic.GetChannel("ch")

	var id MessageID
	msg := NewMessage(id, []byte("test"))
	topic.PutMessage(msg)
	topic.flush(true)

	outputMsg := <-channel1.clientMsgChan
	equal(t, msg.ID, outputMsg.ID)
	equal(t, msg.Body, outputMsg.Body)
}

// ensure that both channels get the same message
func TestPutMessage2Chan(t *testing.T) {
	opts := NewOptions()
	opts.SyncEvery = 1
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_put_message_2chan" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	channel1 := topic.GetChannel("ch1")
	channel2 := topic.GetChannel("ch2")

	var id MessageID
	msg := NewMessage(id, []byte("test"))
	topic.PutMessage(msg)
	topic.flush(true)

	outputMsg1 := <-channel1.clientMsgChan
	equal(t, msg.ID, outputMsg1.ID)
	equal(t, msg.Body, outputMsg1.Body)

	outputMsg2 := <-channel2.clientMsgChan
	equal(t, msg.ID, outputMsg2.ID)
	equal(t, msg.Body, outputMsg2.Body)
}

func TestChannelBackendMaxMsgSize(t *testing.T) {
	opts := NewOptions()
	opts.SyncEvery = 1
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_channel_backend_maxmsgsize" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)

	equal(t, topic.backend.maxMsgSize, int32(opts.MaxMsgSize+minValidMsgLength))
}

func TestInFlightWorker(t *testing.T) {
	count := 250

	opts := NewOptions()
	opts.SyncEvery = 1
	opts.Logger = newTestLogger(t)
	opts.MsgTimeout = 100 * time.Millisecond
	opts.QueueScanRefreshInterval = 100 * time.Millisecond
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_in_flight_worker" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	channel := topic.GetChannel("channel")

	for i := 0; i < count; i++ {
		msg := NewMessage(topic.nextMsgID(), []byte("test"))
		channel.StartInFlightTimeout(msg, 0, "", opts.MsgTimeout)
	}

	channel.Lock()
	inFlightMsgs := len(channel.inFlightMessages)
	channel.Unlock()
	equal(t, inFlightMsgs, count)

	channel.inFlightMutex.Lock()
	inFlightPQMsgs := len(channel.inFlightPQ)
	channel.inFlightMutex.Unlock()
	equal(t, inFlightPQMsgs, count)

	// the in flight worker has a resolution of 100ms so we need to wait
	// at least that much longer than our msgTimeout (in worst case)
	time.Sleep(4*opts.MsgTimeout + opts.QueueScanInterval)

	channel.Lock()
	inFlightMsgs = len(channel.inFlightMessages)
	channel.Unlock()
	equal(t, inFlightMsgs, 0)

	channel.inFlightMutex.Lock()
	inFlightPQMsgs = len(channel.inFlightPQ)
	channel.inFlightMutex.Unlock()
	equal(t, inFlightPQMsgs, 0)
}

func TestChannelEmpty(t *testing.T) {
	opts := NewOptions()
	opts.SyncEvery = 1
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_channel_empty" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	channel := topic.GetChannel("channel")

	msgs := make([]*Message, 0, 25)
	for i := 0; i < 25; i++ {
		msg := NewMessage(topic.nextMsgID(), []byte("test"))
		channel.StartInFlightTimeout(msg, 0, "", opts.MsgTimeout)
		msgs = append(msgs, msg)
	}

	channel.RequeueMessage(0, "", msgs[len(msgs)-1].ID, 0, true)
	equal(t, len(channel.inFlightMessages), 24)
	equal(t, len(channel.inFlightPQ), 24)

	channel.skipChannelToEnd()

	equal(t, len(channel.inFlightMessages), 0)
	equal(t, len(channel.inFlightPQ), 0)
	equal(t, channel.Depth(), int64(0))
}

func TestChannelHealth(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts.MemQueueSize = 2

	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic := nsqd.GetTopicIgnPart("test")

	channel := topic.GetChannel("channel")
	// cause channel.messagePump to exit so we can set channel.backend without
	// a data race. side effect is it closes clientMsgChan, and messagePump is
	// never restarted. note this isn't the intended usage of exitChan but gets
	// around the data race without more invasive changes to how channel.backend
	// is set/loaded.
	channel.exitChan <- 1
}

func TestChannelSkip(t *testing.T) {
	opts := NewOptions()
	opts.SyncEvery = 1
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_channel_skip" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	channel := topic.GetChannel("channel")

	msgs := make([]*Message, 0, 10)
	for i := 0; i < 10; i++ {
		var msgId MessageID
		msgBytes := []byte(strconv.Itoa(i))
		msg := NewMessage(msgId, msgBytes)
		msgs = append(msgs, msg)
	}
	topic.PutMessages(msgs)

	var msgId MessageID
	msgBytes := []byte(strconv.Itoa(10))
	msg := NewMessage(msgId, msgBytes)
	_, backendOffsetMid, _, _, _ := topic.PutMessage(msg)
	topic.flush(true)
	equal(t, channel.Depth(), int64(11))

	msgs = make([]*Message, 0, 9)
	//put another 10 messages
	for i := 0; i < 9; i++ {
		var msgId MessageID
		msgBytes := []byte(strconv.Itoa(i + 11))
		msg := NewMessage(msgId, msgBytes)
		msgs = append(msgs, msg)
	}
	topic.PutMessages(msgs)
	topic.flush(true)
	equal(t, channel.Depth(), int64(20))

	//skip forward to message 10
	t.Logf("backendOffsetMid: %d", backendOffsetMid)
	channel.SetConsumeOffset(backendOffsetMid, 10, true)
	time.Sleep(time.Second)
	for i := 0; i < 10; i++ {
		outputMsg := <-channel.clientMsgChan
		equal(t, string(outputMsg.Body[:]), strconv.Itoa(i+10))
	}
}

func TestChannelResetReadEnd(t *testing.T) {
	opts := NewOptions()
	opts.SyncEvery = 1
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_channel_skip" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	channel := topic.GetChannel("channel")

	msgs := make([]*Message, 0, 10)
	for i := 0; i < 10; i++ {
		var msgId MessageID
		msgBytes := []byte(strconv.Itoa(i))
		msg := NewMessage(msgId, msgBytes)
		msgs = append(msgs, msg)
	}
	topic.PutMessages(msgs)

	var msgId MessageID
	msgBytes := []byte(strconv.Itoa(10))
	msg := NewMessage(msgId, msgBytes)
	_, backendOffsetMid, _, _, _ := topic.PutMessage(msg)
	topic.flush(true)
	equal(t, channel.Depth(), int64(11))

	msgs = make([]*Message, 0, 9)
	//put another 10 messages
	for i := 0; i < 9; i++ {
		var msgId MessageID
		msgBytes := []byte(strconv.Itoa(i + 11))
		msg := NewMessage(msgId, msgBytes)
		msgs = append(msgs, msg)
	}
	topic.PutMessages(msgs)
	topic.flush(true)
	equal(t, channel.Depth(), int64(20))

	//skip forward to message 10
	t.Logf("backendOffsetMid: %d", backendOffsetMid)
	channel.SetConsumeOffset(backendOffsetMid, 10, true)
	for i := 0; i < 10; i++ {
		outputMsg := <-channel.clientMsgChan
		equal(t, string(outputMsg.Body[:]), strconv.Itoa(i+10))
	}
	equal(t, channel.Depth(), int64(10))

	channel.SetConsumeOffset(0, 0, true)
	//equal(t, channel.Depth(), int64(20))
	for i := 0; i < 20; i++ {
		outputMsg := <-channel.clientMsgChan
		t.Logf("Msg: %s", outputMsg.Body)
		equal(t, string(outputMsg.Body[:]), strconv.Itoa(i))
	}
}

// depth timestamp is the next msg time need to be consumed
func TestChannelDepthTimestamp(t *testing.T) {
	// handle read no data, reset, etc
	opts := NewOptions()
	opts.SyncEvery = 1
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_channel_depthts" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	channel := topic.GetChannel("channel")

	msgs := make([]*Message, 0, 9)
	//put another 10 messages
	for i := 0; i < 10; i++ {
		var msgId MessageID
		msgBytes := []byte(strconv.Itoa(i + 11))
		msg := NewMessage(msgId, msgBytes)
		time.Sleep(time.Millisecond * 10)
		msgs = append(msgs, msg)
	}
	topic.PutMessages(msgs)
	topic.flush(true)

	lastDepthTs := int64(0)
	for i := 0; i < 9; i++ {
		msgOutput := <-channel.clientMsgChan
		time.Sleep(time.Millisecond * 10)
		if lastDepthTs != 0 {
			// next msg timestamp == last depth ts
			equal(t, msgOutput.Timestamp, lastDepthTs)
		}
		lastDepthTs = channel.DepthTimestamp()
	}
	channel.resetReaderToConfirmed()
	equal(t, channel.DepthTimestamp(), int64(0))
}

func TestRangeTree(t *testing.T) {
	atr := augmentedtree.New(1)
	atr.Add(&queueInterval{100, 110, 1, 2})
	atr.Add(&queueInterval{110, 120, 2, 3})
	atr.Add(&queueInterval{120, 130, 3, 4})
	atr.Add(&queueInterval{130, 140, 4, 5})
	aret := atr.Query(&queueInterval{111, 140, 2, 5})
	equal(t, len(aret), 3)
	aret = atr.Query(&queueInterval{99, 100, 0, 1})
	equal(t, len(aret), 1)
	atr.Traverse(func(inter augmentedtree.Interval) {
		t.Logf("inter: %v\n", inter)
	})
	atr.Delete(&queueInterval{110, 120, 2, 5})
	t.Log("after delete \n")
	atr.Traverse(func(inter augmentedtree.Interval) {
		t.Logf("inter: %v\n", inter)
	})
	equal(t, atr.Len(), uint64(3))

	tr := NewIntervalTree()
	m1 := &queueInterval{0, 10, 1, 2}
	m2 := &queueInterval{10, 20, 2, 3}
	m3 := &queueInterval{20, 30, 3, 4}
	m4 := &queueInterval{30, 40, 4, 5}
	m5 := &queueInterval{40, 50, 5, 6}
	m6 := &queueInterval{50, 60, 6, 7}
	ret := tr.Query(m1, false)
	equal(t, len(ret), 0)
	tr.AddOrMerge(m1)
	lowest := tr.IsLowestAt(m1.Start())
	equal(t, lowest, m1)
	lowest = tr.IsLowestAt(m1.End())
	equal(t, lowest, nil)
	deleted := tr.DeleteLower(m1.Start() + (m1.End()-m1.Start())/2)
	equal(t, deleted, 0)
	ret = tr.Query(m3, false)
	equal(t, len(ret), 0)
	lowest = tr.IsLowestAt(m1.Start())
	equal(t, lowest, m1)
	tr.AddOrMerge(m3)
	ret = tr.Query(m5, false)
	equal(t, len(ret), 0)
	tr.AddOrMerge(m5)
	ret = tr.Query(m2, false)
	equal(t, len(ret), 2)
	lowest = tr.IsLowestAt(m1.Start())
	equal(t, lowest, m1)
	lowest = tr.IsLowestAt(m3.Start())
	equal(t, lowest, nil)

	deleted = tr.DeleteLower(m1.Start() + (m1.End()-m1.Start())/2)
	equal(t, deleted, 0)

	merged := tr.AddOrMerge(m2)
	equal(t, merged.Start(), m1.Start())
	equal(t, merged.StartCnt(), m1.StartCnt())
	equal(t, merged.End(), m3.End())
	equal(t, merged.EndCnt(), m3.EndCnt())

	ret = tr.Query(m6, false)
	equal(t, len(ret), 1)

	merged = tr.AddOrMerge(m6)
	equal(t, merged.Start(), m5.Start())
	equal(t, merged.StartCnt(), m5.StartCnt())
	equal(t, merged.End(), m6.End())
	equal(t, merged.EndCnt(), m6.EndCnt())

	ret = tr.Query(m4, false)
	equal(t, len(ret), 2)

	merged = tr.AddOrMerge(m4)

	equal(t, tr.Len(), int(1))
	equal(t, merged.Start(), int64(0))
	equal(t, merged.End(), int64(60))
	equal(t, merged.StartCnt(), uint64(1))
	equal(t, merged.EndCnt(), uint64(7))

	deleted = tr.DeleteLower(m1.Start() + (m1.End()-m1.Start())/2)
	equal(t, deleted, 0)
	deleted = tr.DeleteLower(int64(60))
	equal(t, deleted, 1)
	equal(t, tr.Len(), int(0))
}
