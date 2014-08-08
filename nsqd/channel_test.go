package nsqd

import (
	"strconv"
	"testing"
	"time"
)

// ensure that we can push a message through a topic and get it out of a channel
func TestPutMessage(t *testing.T) {
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	topicName := "test_put_message" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel1 := topic.GetChannel("ch")

	var id MessageID
	msg := NewMessage(id, []byte("test"))
	topic.PutMessage(msg)

	outputMsg := <-channel1.clientMsgChan
	equal(t, msg.ID, outputMsg.ID)
	equal(t, msg.Body, outputMsg.Body)
}

// ensure that both channels get the same message
func TestPutMessage2Chan(t *testing.T) {
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	topicName := "test_put_message_2chan" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel1 := topic.GetChannel("ch1")
	channel2 := topic.GetChannel("ch2")

	var id MessageID
	msg := NewMessage(id, []byte("test"))
	topic.PutMessage(msg)

	outputMsg1 := <-channel1.clientMsgChan
	equal(t, msg.ID, outputMsg1.ID)
	equal(t, msg.Body, outputMsg1.Body)

	outputMsg2 := <-channel2.clientMsgChan
	equal(t, msg.ID, outputMsg2.ID)
	equal(t, msg.Body, outputMsg2.Body)
}

func TestInFlightWorker(t *testing.T) {
	count := 250

	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	opts.MsgTimeout = 100 * time.Millisecond
	_, _, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	topicName := "test_in_flight_worker" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("channel")

	for i := 0; i < count; i++ {
		msg := NewMessage(<-nsqd.idChan, []byte("test"))
		channel.StartInFlightTimeout(msg, 0, opts.MsgTimeout)
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
	time.Sleep(4 * opts.MsgTimeout)

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
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	topicName := "test_channel_empty" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("channel")

	msgs := make([]*Message, 0, 25)
	for i := 0; i < 25; i++ {
		msg := NewMessage(<-nsqd.idChan, []byte("test"))
		channel.StartInFlightTimeout(msg, 0, opts.MsgTimeout)
		msgs = append(msgs, msg)
	}

	channel.RequeueMessage(0, msgs[len(msgs)-1].ID, 100*time.Millisecond)
	equal(t, len(channel.inFlightMessages), 24)
	equal(t, len(channel.inFlightPQ), 24)
	equal(t, len(channel.deferredMessages), 1)
	equal(t, len(channel.deferredPQ), 1)

	channel.Empty()

	equal(t, len(channel.inFlightMessages), 0)
	equal(t, len(channel.inFlightPQ), 0)
	equal(t, len(channel.deferredMessages), 0)
	equal(t, len(channel.deferredPQ), 0)
	equal(t, channel.Depth(), int64(0))
}

func TestChannelEmptyConsumer(t *testing.T) {
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	tcpAddr, _, nsqd := mustStartNSQD(opts)
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
		msg := NewMessage(<-nsqd.idChan, []byte("test"))
		channel.StartInFlightTimeout(msg, 0, opts.MsgTimeout)
		client.SendingMessage()
	}

	for _, cl := range channel.clients {
		stats := cl.Stats()
		equal(t, stats.InFlightCount, int64(25))
	}

	channel.Empty()

	for _, cl := range channel.clients {
		stats := cl.Stats()
		equal(t, stats.InFlightCount, int64(0))
	}

}
