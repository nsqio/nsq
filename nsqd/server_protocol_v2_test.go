package main

import (
	"../nsq"
	"../util"
	"github.com/bmizerany/assert"
	// "io/ioutil"
	// "log"
	"net"
	"testing"
	"time"
)

// exercise the basic operations of the V2 protocol
func TestBasicV2(t *testing.T) {
	// log.SetOutput(ioutil.Discard)
	// defer log.SetOutput(os.Stdout)

	tcpAddr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:5170")
	tcpListener, err := net.Listen("tcp", tcpAddr.String())
	defer tcpListener.Close()

	go TopicFactory(10, ".")
	go util.TcpServer(tcpListener, tcpClientHandler)

	msg := nsq.NewMessage(util.Uuid(), []byte("test body"))
	topic := GetTopic("test_v2")
	topic.PutMessage(msg)

	consumer := nsq.NewConsumer(tcpAddr)

	err = consumer.Connect()
	assert.Equal(t, err, nil)

	err = consumer.Version(nsq.ProtocolV2Magic)
	assert.Equal(t, err, nil)

	err = consumer.WriteCommand(consumer.Subscribe("test_v2", "ch"))
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

func TestMultipleConsumerV2(t *testing.T) {
	// log.SetOutput(ioutil.Discard)

	readyChan := make(chan int)
	msgChan := make(chan *nsq.Message)

	tcpAddr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:5170")
	tcpListener, err := net.Listen("tcp", tcpAddr.String())
	defer tcpListener.Close()

	go TopicFactory(10, ".")
	go util.TcpServer(tcpListener, tcpClientHandler)

	msg := nsq.NewMessage(util.Uuid(), []byte("test body"))
	topic := GetTopic("test_multiple_v2")

	for _, i := range []string{"1", "2"} {
		consumer := nsq.NewConsumer(tcpAddr)
		err = consumer.Connect()
		assert.Equal(t, err, nil)

		err = consumer.Version(nsq.ProtocolV2Magic)
		assert.Equal(t, err, nil)

		err = consumer.WriteCommand(consumer.Subscribe("test_multiple_v2", "ch"+i))
		assert.Equal(t, err, nil)

		err = consumer.WriteCommand(consumer.Ready(1))
		assert.Equal(t, err, nil)

		go func() {
			<-time.After(time.Duration(time.Second))
			readyChan <- 1
		}()

		go func(c *nsq.Consumer) {
			resp, _ := c.ReadResponse()
			_, msgInterface, _ := c.UnpackResponse(resp)
			msgChan <- msgInterface.(*nsq.Message)
		}(consumer)
	}

	<-readyChan
	<-readyChan

	topic.PutMessage(msg)

	msgOut := <-msgChan
	assert.Equal(t, msgOut.Uuid, msg.Uuid)
	assert.Equal(t, msgOut.Body, msg.Body)
	assert.Equal(t, msgOut.Retries, uint16(1))
	msgOut = <-msgChan
	assert.Equal(t, msgOut.Uuid, msg.Uuid)
	assert.Equal(t, msgOut.Body, msg.Body)
	assert.Equal(t, msgOut.Retries, uint16(1))
}
