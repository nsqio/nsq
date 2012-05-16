package message

import (
	"../util"
	"bytes"
	"github.com/bmizerany/assert"
	"io/ioutil"
	"log"
	"testing"
)

// ensure that we can push a message through a topic and get it out of a channel
func TestPutMessage(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	go TopicFactory(10)

	topic := GetTopic("test")
	channel1 := topic.GetChannel("ch1")

	var buf bytes.Buffer
	uuid := util.Uuid()
	buf.Write(uuid)
	body := []byte("test")
	buf.Write(body)
	inputMsg := NewMessage(buf.Bytes())
	topic.PutMessage(inputMsg)

	outputMsg := channel1.GetMessage()
	assert.Equal(t, uuid, outputMsg.Uuid())
	assert.Equal(t, body, outputMsg.Body())
}

// ensure that both channels get the same message
func TestPutMessage2Chan(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	go TopicFactory(10)

	topic := GetTopic("test")
	channel1 := topic.GetChannel("ch1")
	channel2 := topic.GetChannel("ch2")

	var buf bytes.Buffer
	uuid := util.Uuid()
	buf.Write(uuid)
	body := []byte("test")
	buf.Write(body)
	inputMsg := NewMessage(buf.Bytes())
	topic.PutMessage(inputMsg)

	outputMsg1 := channel1.GetMessage()
	assert.Equal(t, uuid, outputMsg1.Uuid())
	assert.Equal(t, body, outputMsg1.Body())

	outputMsg2 := channel2.GetMessage()
	assert.Equal(t, uuid, outputMsg2.Uuid())
	assert.Equal(t, body, outputMsg2.Body())
}
