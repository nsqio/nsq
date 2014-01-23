package main

import (
	"github.com/bitly/go-nsq"
	"github.com/bmizerany/assert"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"testing"
	"time"
)

// ensure that we can push a message through a topic and get it out of a channel
func TestPutMessage(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	nsqd := NewNSQD(NewNSQDOptions())
	defer nsqd.Exit()

	topicName := "test_put_message" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel1 := topic.GetChannel("ch")

	var id nsq.MessageID
	msg := nsq.NewMessage(id, []byte("test"))
	topic.PutMessage(msg)

	outputMsg := <-channel1.clientMsgChan
	assert.Equal(t, msg.Id, outputMsg.Id)
	assert.Equal(t, msg.Body, outputMsg.Body)
}

// ensure that both channels get the same message
func TestPutMessage2Chan(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	nsqd := NewNSQD(NewNSQDOptions())
	defer nsqd.Exit()

	topicName := "test_put_message_2chan" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel1 := topic.GetChannel("ch1")
	channel2 := topic.GetChannel("ch2")

	var id nsq.MessageID
	msg := nsq.NewMessage(id, []byte("test"))
	topic.PutMessage(msg)

	outputMsg1 := <-channel1.clientMsgChan
	assert.Equal(t, msg.Id, outputMsg1.Id)
	assert.Equal(t, msg.Body, outputMsg1.Body)

	outputMsg2 := <-channel2.clientMsgChan
	assert.Equal(t, msg.Id, outputMsg2.Id)
	assert.Equal(t, msg.Body, outputMsg2.Body)
}

func TestInFlightWorker(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	options := NewNSQDOptions()
	options.MsgTimeout = 200 * time.Millisecond
	nsqd := NewNSQD(options)
	defer nsqd.Exit()

	topicName := "test_in_flight_worker" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("channel")

	for i := 0; i < 1000; i++ {
		msg := nsq.NewMessage(<-nsqd.idChan, []byte("test"))
		channel.StartInFlightTimeout(msg, 0)
	}

	assert.Equal(t, len(channel.inFlightMessages), 1000)
	assert.Equal(t, len(channel.inFlightPQ), 1000)

	// the in flight worker has a resolution of 100ms so we need to wait
	// at least that much longer than our msgTimeout (in worst case)
	time.Sleep(options.MsgTimeout + 100*time.Millisecond)

	assert.Equal(t, len(channel.inFlightMessages), 0)
	assert.Equal(t, len(channel.inFlightPQ), 0)
}

func TestChannelEmpty(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	nsqd := NewNSQD(NewNSQDOptions())
	defer nsqd.Exit()

	topicName := "test_channel_empty" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("channel")

	msgs := make([]*nsq.Message, 0, 25)
	for i := 0; i < 25; i++ {
		msg := nsq.NewMessage(<-nsqd.idChan, []byte("test"))
		channel.StartInFlightTimeout(msg, 0)
		msgs = append(msgs, msg)
	}

	channel.RequeueMessage(0, msgs[len(msgs)-1].Id, 100*time.Millisecond)
	assert.Equal(t, len(channel.inFlightMessages), 24)
	assert.Equal(t, len(channel.inFlightPQ), 24)
	assert.Equal(t, len(channel.deferredMessages), 1)
	assert.Equal(t, len(channel.deferredPQ), 1)

	channel.Empty()

	assert.Equal(t, len(channel.inFlightMessages), 0)
	assert.Equal(t, len(channel.inFlightPQ), 0)
	assert.Equal(t, len(channel.deferredMessages), 0)
	assert.Equal(t, len(channel.deferredPQ), 0)
	assert.Equal(t, channel.Depth(), int64(0))
}

func TestChannelEmptyConsumer(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	tcpAddr, _, nsqd := mustStartNSQD(NewNSQDOptions())
	defer nsqd.Exit()
	conn, _ := mustConnectNSQD(tcpAddr)

	topicName := "test_channel_empty" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("channel")
	client := NewClientV2(0, conn, &Context{nsqd})
	client.SetReadyCount(25)
	channel.AddClient(client.ID, client)

	for i := 0; i < 25; i++ {
		msg := nsq.NewMessage(<-nsqd.idChan, []byte("test"))
		channel.StartInFlightTimeout(msg, 0)
		client.SendingMessage()
	}

	for _, cl := range channel.clients {
		stats := cl.Stats()
		assert.Equal(t, stats.InFlightCount, int64(25))
	}

	channel.Empty()

	for _, cl := range channel.clients {
		stats := cl.Stats()
		assert.Equal(t, stats.InFlightCount, int64(0))
	}

}
