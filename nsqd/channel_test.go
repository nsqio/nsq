package nsqd

import (
	//"github.com/absolute8511/nsq/internal/levellogger"
	"os"
	"strconv"
	"testing"
	"time"
)

// ensure that we can push a message through a topic and get it out of a channel
func TestPutMessage(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	//opts.Logger = &levellogger.GLogger{}
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
	// TODO: backward and forward
}

func TestChannelResetReadEnd(t *testing.T) {
}

func TestChannelDepthTimestamp(t *testing.T) {
	// handle read no data, reset, etc
}
