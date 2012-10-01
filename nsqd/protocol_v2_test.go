package main

import (
	"../nsq"
	"../util"
	"bufio"
	"bytes"
	"github.com/bmizerany/assert"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"testing"
	"time"
)

func mustStartNSQd(timeout time.Duration) (*net.TCPAddr, *net.TCPAddr) {
	tcpAddr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	options := NewNsqdOptions()
	options.clientTimeout = timeout
	nsqd = NewNSQd(1, options)
	nsqd.tcpAddr = tcpAddr
	nsqd.httpAddr = tcpAddr
	nsqd.Main()
	return nsqd.tcpListener.Addr().(*net.TCPAddr), nsqd.httpListener.Addr().(*net.TCPAddr)
}

func mustConnectNSQd(t *testing.T, tcpAddr *net.TCPAddr) net.Conn {
	conn, err := net.DialTimeout("tcp", tcpAddr.String(), time.Second)
	if err != nil {
		t.Fatal("failed to connect to nsqd")
	}
	conn.Write(nsq.MagicV2)
	return conn
}

// test channel/topic names
func TestChannelTopicNames(t *testing.T) {
	assert.Equal(t, nsq.IsValidChannelName("test"), true)
	assert.Equal(t, nsq.IsValidChannelName("test#ephemeral"), true)
	assert.Equal(t, nsq.IsValidTopicName("test"), true)
	assert.Equal(t, nsq.IsValidTopicName("test#ephemeral"), false)
	assert.Equal(t, nsq.IsValidTopicName("test:ephemeral"), false)
}

// exercise the basic operations of the V2 protocol
func TestBasicV2(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	tcpAddr, _ := mustStartNSQd(60 * time.Second)
	defer nsqd.Exit()

	topicName := "test_v2" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	msg := nsq.NewMessage(<-nsqd.idChan, []byte("test body"))
	topic.PutMessage(msg)

	conn := mustConnectNSQd(t, tcpAddr)

	err := nsq.SendCommand(conn, nsq.Subscribe(topicName, "ch", "TestBasicV2", "TestBasicV2"))
	assert.Equal(t, err, nil)

	err = nsq.SendCommand(conn, nsq.Ready(1))
	assert.Equal(t, err, nil)

	resp, err := nsq.ReadResponse(conn)
	assert.Equal(t, err, nil)
	frameType, data, err := nsq.UnpackResponse(resp)
	msgOut, _ := nsq.DecodeMessage(data)
	assert.Equal(t, frameType, nsq.FrameTypeMessage)
	assert.Equal(t, msgOut.Id, msg.Id)
	assert.Equal(t, msgOut.Body, msg.Body)
	assert.Equal(t, msgOut.Attempts, uint16(1))
}

func TestMultipleConsumerV2(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	msgChan := make(chan *nsq.Message)

	tcpAddr, _ := mustStartNSQd(60 * time.Second)
	defer nsqd.Exit()

	topicName := "test_multiple_v2" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	msg := nsq.NewMessage(<-nsqd.idChan, []byte("test body"))
	topic.GetChannel("ch1")
	topic.GetChannel("ch2")
	topic.PutMessage(msg)

	for _, i := range []string{"1", "2"} {
		conn := mustConnectNSQd(t, tcpAddr)

		err := nsq.SendCommand(conn, nsq.Subscribe(topicName, "ch"+i, "TestMultipleConsumerV2", "TestMultipleConsumerV2"))
		assert.Equal(t, err, nil)

		err = nsq.SendCommand(conn, nsq.Ready(1))
		assert.Equal(t, err, nil)

		go func(c net.Conn) {
			resp, _ := nsq.ReadResponse(c)
			_, data, _ := nsq.UnpackResponse(resp)
			msg, _ := nsq.DecodeMessage(data)
			msgChan <- msg
		}(conn)
	}

	msgOut := <-msgChan
	assert.Equal(t, msgOut.Id, msg.Id)
	assert.Equal(t, msgOut.Body, msg.Body)
	assert.Equal(t, msgOut.Attempts, uint16(1))
	msgOut = <-msgChan
	assert.Equal(t, msgOut.Id, msg.Id)
	assert.Equal(t, msgOut.Body, msg.Body)
	assert.Equal(t, msgOut.Attempts, uint16(1))
}

func TestClientTimeout(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	topicName := "test_client_timeout_v2" + strconv.Itoa(int(time.Now().Unix()))

	tcpAddr, _ := mustStartNSQd(50 * time.Millisecond)
	defer nsqd.Exit()

	conn := mustConnectNSQd(t, tcpAddr)

	err := nsq.SendCommand(conn, nsq.Subscribe(topicName, "ch", "TestClientTimeoutV2", "TestClientTimeoutV2"))
	assert.Equal(t, err, nil)

	time.Sleep(50 * time.Millisecond)

	// depending on timing there may be 1 or 2 hearbeats sent
	// just read until we get an error
	timer := time.After(100 * time.Millisecond)
	for {
		select {
		case <-timer:
			t.Fatalf("test timed out")
		default:
			_, err := nsq.ReadResponse(conn)
			if err != nil {
				goto done
			}
		}
	}
done:
}

func TestClientHeartbeat(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	topicName := "test_hb_v2" + strconv.Itoa(int(time.Now().Unix()))

	tcpAddr, _ := mustStartNSQd(100 * time.Millisecond)
	defer nsqd.Exit()

	conn := mustConnectNSQd(t, tcpAddr)

	err := nsq.SendCommand(conn, nsq.Subscribe(topicName, "ch", "TestClientHeartbeatV2", "TestClientHeartbeatV2"))
	assert.Equal(t, err, nil)

	err = nsq.SendCommand(conn, nsq.Ready(1))
	assert.Equal(t, err, nil)

	resp, _ := nsq.ReadResponse(conn)
	_, data, _ := nsq.UnpackResponse(resp)
	assert.Equal(t, data, []byte("_heartbeat_"))

	time.Sleep(10 * time.Millisecond)

	err = nsq.SendCommand(conn, nsq.Nop())
	assert.Equal(t, err, nil)

	// wait long enough that would have timed out (had we not sent the above cmd)
	time.Sleep(50 * time.Millisecond)

	err = nsq.SendCommand(conn, nsq.Nop())
	assert.Equal(t, err, nil)
}

func BenchmarkProtocolV2Command(b *testing.B) {
	b.StopTimer()
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)
	p := &ProtocolV2{}
	c := NewClientV2(nil)
	params := [][]byte{[]byte("SUB"), []byte("test"), []byte("ch")}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		p.Exec(c, params)
	}
}

func BenchmarkProtocolV2Data(b *testing.B) {
	b.StopTimer()
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)
	p := &ProtocolV2{}
	var cb bytes.Buffer
	rw := bufio.NewReadWriter(bufio.NewReader(&cb), bufio.NewWriter(ioutil.Discard))
	conn := util.MockConn{rw}
	c := NewClientV2(conn)
	var buf bytes.Buffer
	msg := nsq.NewMessage([]byte("0123456789abcdef"), []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	b.StartTimer()

	for i := 0; i < b.N; i += 1 {
		buf.Reset()
		msg.Encode(&buf)
		p.Send(c, nsq.FrameTypeMessage, buf.Bytes())
	}
}
