package main

import (
	"../nsq"
	"../util"
	"github.com/bmizerany/assert"
	"io/ioutil"
	"log"
	"net"
	"testing"
)

// exercise the basic operations of the V2 protocol
func TestV2(t *testing.T) {
	log.SetOutput(ioutil.Discard)

	tcpAddr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:5170")

	go TopicFactory(10, ".")
	go TcpServer(tcpAddr)

	msg := nsq.NewMessage(util.Uuid(), []byte("test body"))
	topic := GetTopic("testv2")
	topic.PutMessage(msg)

	consumer := nsq.NewConsumer(tcpAddr)
	err = consumer.Connect()
	assert.Equal(t, err, nil)

	err = consumer.Version(nsq.ProtocolV2Magic)
	assert.Equal(t, err, nil)

	err = consumer.WriteCommand(consumer.Subscribe("testv2", "ch"))
	assert.Equal(t, err, nil)

	err = consumer.WriteCommand(consumer.Ready(1))
	assert.Equal(t, err, nil)

	resp, err := consumer.ReadResponse()
	assert.Equal(t, err, nil)
	frameType, msgInterface, err := consumer.UnpackResponse(resp)
	msgOut := msgInterface.(*nsq.Message)
	assert.Equal(t, frameType, nsq.FrameTypeMessage)
	assert.Equal(t, msgOut.Uuid, msg.Uuid)
	assert.Equal(t, msgOut.Body, msg.Body)
	assert.Equal(t, msgOut.Retries, uint16(1))
}
