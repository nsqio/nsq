package main

import (
	"../nsq"
	"github.com/bmizerany/assert"
	"io/ioutil"
	"log"
	"os"
	"testing"
)

// ensure that we can push a message through a topic and get it out of a channel
func TestPutMessage(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	topic := NewTopic("test_put_message", 10, os.TempDir(), 1024)
	channel1 := topic.GetChannel("ch")

	msg := nsq.NewMessage([]byte("abcdefghijklmnop"), []byte("test"))
	topic.PutMessage(msg)

	outputMsg := <-channel1.ClientMessageChan
	assert.Equal(t, msg.Id, outputMsg.Id)
	assert.Equal(t, msg.Body, outputMsg.Body)
}

// ensure that both channels get the same message
func TestPutMessage2Chan(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	topic := NewTopic("test_put_message_2chan", 10, os.TempDir(), 1024)
	channel1 := topic.GetChannel("ch1")
	channel2 := topic.GetChannel("ch2")

	msg := nsq.NewMessage([]byte("abcdefghijklmnop"), []byte("test"))
	topic.PutMessage(msg)

	outputMsg1 := <-channel1.ClientMessageChan
	assert.Equal(t, msg.Id, outputMsg1.Id)
	assert.Equal(t, msg.Body, outputMsg1.Body)

	outputMsg2 := <-channel2.ClientMessageChan
	assert.Equal(t, msg.Id, outputMsg2.Id)
	assert.Equal(t, msg.Body, outputMsg2.Body)
}
