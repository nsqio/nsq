package nsqd

import (
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/mreiferson/wal"
	"github.com/nsqio/nsq/internal/test"
)

func channelReceiveHelper(c *Channel) *Message {
	var msg *Message
	select {
	case msg = <-c.memoryMsgChan:
	case ev := <-c.cursor.ReadCh():
		entry, _ := DecodeWireEntry(ev.Body)
		msg = NewMessage(guid(ev.ID).Hex(), time.Now().UnixNano(), entry.Body)
	}
	c.StartInFlightTimeout(msg, 0, time.Second*60)
	return msg
}

// ensure that we can push a message through a topic and get it out of a channel
func TestPutMessage(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_put_message" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel1 := topic.GetChannel("ch")

	body := []byte("test")
	topic.Pub([]wal.EntryWriterTo{NewEntry(body, time.Now().UnixNano(), 0)})

	outputMsg := channelReceiveHelper(channel1)
	// test.Equal(t, msg.ID, outputMsg.ID)
	test.Equal(t, body, outputMsg.Body)
}

// ensure that both channels get the same message
func TestPutMessage2Chan(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_put_message_2chan" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel1 := topic.GetChannel("ch1")
	channel2 := topic.GetChannel("ch2")

	body := []byte("test")
	topic.Pub([]wal.EntryWriterTo{NewEntry(body, time.Now().UnixNano(), 0)})

	outputMsg1 := channelReceiveHelper(channel1)
	// test.Equal(t, msg.ID, outputMsg1.ID)
	test.Equal(t, body, outputMsg1.Body)

	outputMsg2 := channelReceiveHelper(channel2)
	// test.Equal(t, msg.ID, outputMsg2.ID)
	test.Equal(t, body, outputMsg2.Body)
}

// TODO: (WAL) fixme
// func TestChannelBackendMaxMsgSize(t *testing.T) {
// 	opts := NewOptions()
// 	opts.Logger = newTestLogger(t)
// 	_, _, nsqd := mustStartNSQD(opts)
// 	defer os.RemoveAll(opts.DataPath)
// 	defer nsqd.Exit()
//
// 	topicName := "test_channel_backend_maxmsgsize" + strconv.Itoa(int(time.Now().Unix()))
// 	topic := nsqd.GetTopic(topicName)
// 	ch := topic.GetChannel("ch")
//
// 	test.Equal(t, int32(opts.MaxMsgSize+minValidMsgLength), ch.backend.(*diskQueue).maxMsgSize)
// }

func TestInFlightWorker(t *testing.T) {
	count := 250

	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.MsgTimeout = 100 * time.Millisecond
	opts.QueueScanRefreshInterval = 100 * time.Millisecond
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_in_flight_worker" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("channel")

	for i := 0; i < count; i++ {
		msg := NewMessage(guid(i).Hex(), time.Now().UnixNano(), []byte("test"))
		channel.StartInFlightTimeout(msg, 0, opts.MsgTimeout)
	}

	channel.Lock()
	inFlightMsgs := len(channel.inFlightMessages)
	channel.Unlock()
	test.Equal(t, count, inFlightMsgs)

	channel.inFlightMutex.Lock()
	inFlightPQMsgs := len(channel.inFlightPQ)
	channel.inFlightMutex.Unlock()
	test.Equal(t, count, inFlightPQMsgs)

	// the in flight worker has a resolution of 100ms so we need to wait
	// at least that much longer than our msgTimeout (in worst case)
	time.Sleep(4 * opts.MsgTimeout)

	channel.Lock()
	inFlightMsgs = len(channel.inFlightMessages)
	channel.Unlock()
	test.Equal(t, 0, inFlightMsgs)

	channel.inFlightMutex.Lock()
	inFlightPQMsgs = len(channel.inFlightPQ)
	channel.inFlightMutex.Unlock()
	test.Equal(t, 0, inFlightPQMsgs)
}

func TestChannelEmpty(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_channel_empty" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("channel")

	body := []byte("test")
	for i := 0; i < 25; i++ {
		topic.Pub([]wal.EntryWriterTo{NewEntry(body, time.Now().UnixNano(), 0)})
	}

	channelReceiveHelper(channel)
	msg := channelReceiveHelper(channel)
	channel.RequeueMessage(0, msg.ID, 100*time.Millisecond)
	test.Equal(t, 1, len(channel.inFlightMessages))
	test.Equal(t, 1, len(channel.inFlightPQ))
	test.Equal(t, 1, len(channel.deferredMessages))
	test.Equal(t, 1, len(channel.deferredPQ))
	test.Equal(t, uint64(25), channel.Depth())

	channel.Empty()

	test.Equal(t, 0, len(channel.inFlightMessages))
	test.Equal(t, 0, len(channel.inFlightPQ))
	test.Equal(t, 0, len(channel.deferredMessages))
	test.Equal(t, 0, len(channel.deferredPQ))
	test.Equal(t, uint64(0), channel.Depth())
}

func TestChannelEmptyConsumer(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, _ := mustConnectNSQD(tcpAddr)
	defer conn.Close()

	topicName := "test_channel_empty" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("channel")
	client := newClientV2(0, conn, &context{nsqd})
	client.SetReadyCount(25)
	channel.AddClient(client.ID, client)

	for i := 0; i < 25; i++ {
		msg := NewMessage(guid(0).Hex(), time.Now().UnixNano(), []byte("test"))
		channel.StartInFlightTimeout(msg, 0, opts.MsgTimeout)
		client.SendingMessage()
	}

	for _, cl := range channel.clients {
		stats := cl.Stats()
		test.Equal(t, int64(25), stats.InFlightCount)
	}

	channel.Empty()

	for _, cl := range channel.clients {
		stats := cl.Stats()
		test.Equal(t, int64(0), stats.InFlightCount)
	}
}
