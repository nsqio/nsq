package main

import (
	"../nsq"
	"github.com/bmizerany/assert"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"testing"
	"time"
)

func mustStartNSQd(t *testing.T) (*net.TCPAddr, *net.TCPAddr) {
	tcpAddr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	nsqd = NewNSQd(1, tcpAddr, tcpAddr, nil, 10, os.TempDir(), 1024)
	nsqd.Main()
	return nsqd.tcpListener.Addr().(*net.TCPAddr), nsqd.httpListener.Addr().(*net.TCPAddr)
}

// exercise the basic operations of the V2 protocol
func TestBasicV2(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	tcpAddr, _ := mustStartNSQd(t)
	defer nsqd.Exit()

	topicName := "test_v2" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	msg := nsq.NewMessage(<-nsqd.idChan, []byte("test body"))
	topic.PutMessage(msg)

	consumer := nsq.NewConsumer(tcpAddr)

	err := consumer.Connect()
	assert.Equal(t, err, nil)

	err = consumer.Version(nsq.ProtocolV2Magic)
	assert.Equal(t, err, nil)

	err = consumer.WriteCommand(consumer.Subscribe(topicName, "ch"))
	assert.Equal(t, err, nil)

	err = consumer.WriteCommand(consumer.Ready(1))
	assert.Equal(t, err, nil)

	resp, err := consumer.ReadResponse()
	assert.Equal(t, err, nil)
	frameType, msgInterface, err := consumer.UnpackResponse(resp)
	msgOut := msgInterface.(*nsq.Message)
	assert.Equal(t, frameType, nsq.FrameTypeMessage)
	assert.Equal(t, msgOut.Id, msg.Id)
	assert.Equal(t, msgOut.Body, msg.Body)
	assert.Equal(t, msgOut.Retries, uint16(1))
}

func TestMultipleConsumerV2(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	msgChan := make(chan *nsq.Message)

	tcpAddr, _ := mustStartNSQd(t)
	defer nsqd.Exit()

	topicName := "test_multiple_v2" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	msg := nsq.NewMessage(<-nsqd.idChan, []byte("test body"))
	topic.GetChannel("ch1")
	topic.GetChannel("ch2")
	topic.PutMessage(msg)

	for _, i := range []string{"1", "2"} {
		consumer := nsq.NewConsumer(tcpAddr)
		err := consumer.Connect()
		assert.Equal(t, err, nil)

		err = consumer.Version(nsq.ProtocolV2Magic)
		assert.Equal(t, err, nil)

		err = consumer.WriteCommand(consumer.Subscribe(topicName, "ch"+i))
		assert.Equal(t, err, nil)

		err = consumer.WriteCommand(consumer.Ready(1))
		assert.Equal(t, err, nil)

		go func(c *nsq.Consumer) {
			resp, _ := c.ReadResponse()
			_, msgInterface, _ := c.UnpackResponse(resp)
			msgChan <- msgInterface.(*nsq.Message)
		}(consumer)
	}

	msgOut := <-msgChan
	assert.Equal(t, msgOut.Id, msg.Id)
	assert.Equal(t, msgOut.Body, msg.Body)
	assert.Equal(t, msgOut.Retries, uint16(1))
	msgOut = <-msgChan
	assert.Equal(t, msgOut.Id, msg.Id)
	assert.Equal(t, msgOut.Body, msg.Body)
	assert.Equal(t, msgOut.Retries, uint16(1))
}
