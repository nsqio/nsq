package main

import (
	"bufio"
	"bytes"
	"compress/flate"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/bitly/go-nsq"
	"github.com/bmizerany/assert"
	"github.com/mreiferson/go-snappystream"
	"io"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
)

func mustStartNSQD(options *nsqdOptions) (*net.TCPAddr, *net.TCPAddr, *NSQD) {
	tcpAddr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	httpAddr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	nsqd := NewNSQD(options)
	nsqd.tcpAddr = tcpAddr
	nsqd.httpAddr = httpAddr
	nsqd.Main()
	return nsqd.tcpListener.Addr().(*net.TCPAddr), nsqd.httpListener.Addr().(*net.TCPAddr), nsqd
}

func mustConnectNSQD(tcpAddr *net.TCPAddr) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", tcpAddr.String(), time.Second)
	if err != nil {
		return nil, err
	}
	conn.Write(nsq.MagicV2)
	return conn, nil
}

func identify(t *testing.T, conn io.ReadWriter) {
	ci := make(map[string]interface{})
	ci["short_id"] = "test"
	ci["long_id"] = "test"
	cmd, _ := nsq.Identify(ci)
	err := cmd.Write(conn)
	assert.Equal(t, err, nil)
	readValidate(t, conn, nsq.FrameTypeResponse, "OK")
}

func identifyHeartbeatInterval(t *testing.T, conn io.ReadWriter, interval int, f int32, d string) {
	ci := make(map[string]interface{})
	ci["short_id"] = "test"
	ci["long_id"] = "test"
	ci["heartbeat_interval"] = interval
	cmd, _ := nsq.Identify(ci)
	err := cmd.Write(conn)
	assert.Equal(t, err, nil)
	readValidate(t, conn, f, d)
}

func identifyFeatureNegotiation(t *testing.T, conn io.ReadWriter, extra map[string]interface{}) []byte {
	ci := make(map[string]interface{})
	ci["short_id"] = "test"
	ci["long_id"] = "test"
	ci["feature_negotiation"] = true
	if extra != nil {
		for k, v := range extra {
			ci[k] = v
		}
	}
	cmd, _ := nsq.Identify(ci)
	err := cmd.Write(conn)
	assert.Equal(t, err, nil)
	resp, err := nsq.ReadResponse(conn)
	assert.Equal(t, err, nil)
	frameType, data, err := nsq.UnpackResponse(resp)
	assert.Equal(t, err, nil)
	assert.Equal(t, frameType, nsq.FrameTypeResponse)
	return data
}

func identifyOutputBuffering(t *testing.T, conn io.ReadWriter, size int, timeout int, f int32, d string) {
	ci := make(map[string]interface{})
	ci["short_id"] = "test"
	ci["long_id"] = "test"
	ci["output_buffer_size"] = size
	ci["output_buffer_timeout"] = timeout
	cmd, _ := nsq.Identify(ci)
	err := cmd.Write(conn)
	assert.Equal(t, err, nil)
	readValidate(t, conn, f, d)
}

func sub(t *testing.T, conn io.ReadWriter, topicName string, channelName string) {
	err := nsq.Subscribe(topicName, channelName).Write(conn)
	assert.Equal(t, err, nil)
	readValidate(t, conn, nsq.FrameTypeResponse, "OK")
}

func subFail(t *testing.T, conn io.ReadWriter, topicName string, channelName string) {
	err := nsq.Subscribe(topicName, channelName).Write(conn)
	assert.Equal(t, err, nil)
	resp, err := nsq.ReadResponse(conn)
	frameType, _, err := nsq.UnpackResponse(resp)
	assert.Equal(t, frameType, nsq.FrameTypeError)
}

func readValidate(t *testing.T, conn io.Reader, f int32, d string) {
	resp, err := nsq.ReadResponse(conn)
	assert.Equal(t, err, nil)
	frameType, data, err := nsq.UnpackResponse(resp)
	assert.Equal(t, err, nil)
	assert.Equal(t, frameType, f)
	assert.Equal(t, string(data), d)
}

// test channel/topic names
func TestChannelTopicNames(t *testing.T) {
	assert.Equal(t, nsq.IsValidChannelName("test"), true)
	assert.Equal(t, nsq.IsValidChannelName("test-with_period."), true)
	assert.Equal(t, nsq.IsValidChannelName("test#ephemeral"), true)
	assert.Equal(t, nsq.IsValidTopicName("test"), true)
	assert.Equal(t, nsq.IsValidTopicName("test-with_period."), true)
	assert.Equal(t, nsq.IsValidTopicName("test#ephemeral"), false)
	assert.Equal(t, nsq.IsValidTopicName("test:ephemeral"), false)
}

// exercise the basic operations of the V2 protocol
func TestBasicV2(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	options := NewNSQDOptions()
	options.ClientTimeout = 60 * time.Second
	tcpAddr, _, nsqd := mustStartNSQD(options)
	defer nsqd.Exit()

	topicName := "test_v2" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	msg := nsq.NewMessage(<-nsqd.idChan, []byte("test body"))
	topic.PutMessage(msg)

	conn, err := mustConnectNSQD(tcpAddr)
	assert.Equal(t, err, nil)

	identify(t, conn)
	sub(t, conn, topicName, "ch")

	err = nsq.Ready(1).Write(conn)
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

	options := NewNSQDOptions()
	options.ClientTimeout = 60 * time.Second
	tcpAddr, _, nsqd := mustStartNSQD(options)
	defer nsqd.Exit()

	topicName := "test_multiple_v2" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	msg := nsq.NewMessage(<-nsqd.idChan, []byte("test body"))
	topic.GetChannel("ch1")
	topic.GetChannel("ch2")
	topic.PutMessage(msg)

	for _, i := range []string{"1", "2"} {
		conn, err := mustConnectNSQD(tcpAddr)
		assert.Equal(t, err, nil)

		identify(t, conn)
		sub(t, conn, topicName, "ch"+i)

		err = nsq.Ready(1).Write(conn)
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

	options := NewNSQDOptions()
	options.ClientTimeout = 50 * time.Millisecond
	tcpAddr, _, nsqd := mustStartNSQD(options)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	assert.Equal(t, err, nil)

	identify(t, conn)
	sub(t, conn, topicName, "ch")

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

	options := NewNSQDOptions()
	options.ClientTimeout = 200 * time.Millisecond
	tcpAddr, _, nsqd := mustStartNSQD(options)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	assert.Equal(t, err, nil)

	identify(t, conn)
	sub(t, conn, topicName, "ch")

	err = nsq.Ready(1).Write(conn)
	assert.Equal(t, err, nil)

	resp, _ := nsq.ReadResponse(conn)
	_, data, _ := nsq.UnpackResponse(resp)
	assert.Equal(t, data, []byte("_heartbeat_"))

	time.Sleep(20 * time.Millisecond)

	err = nsq.Nop().Write(conn)
	assert.Equal(t, err, nil)

	// wait long enough that would have timed out (had we not sent the above cmd)
	time.Sleep(100 * time.Millisecond)

	err = nsq.Nop().Write(conn)
	assert.Equal(t, err, nil)
}

func TestClientHeartbeatDisableSUB(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	topicName := "test_hb_v2" + strconv.Itoa(int(time.Now().Unix()))

	options := NewNSQDOptions()
	options.ClientTimeout = 200 * time.Millisecond
	tcpAddr, _, nsqd := mustStartNSQD(options)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	assert.Equal(t, err, nil)

	identifyHeartbeatInterval(t, conn, -1, nsq.FrameTypeResponse, "OK")
	subFail(t, conn, topicName, "ch")
}

func TestClientHeartbeatDisable(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	options := NewNSQDOptions()
	options.ClientTimeout = 100 * time.Millisecond
	tcpAddr, _, nsqd := mustStartNSQD(options)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	assert.Equal(t, err, nil)

	identifyHeartbeatInterval(t, conn, -1, nsq.FrameTypeResponse, "OK")

	time.Sleep(150 * time.Millisecond)

	err = nsq.Nop().Write(conn)
	assert.Equal(t, err, nil)
}

func TestMaxHeartbeatIntervalValid(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	options := NewNSQDOptions()
	options.MaxHeartbeatInterval = 300 * time.Second
	tcpAddr, _, nsqd := mustStartNSQD(options)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	assert.Equal(t, err, nil)

	hbi := int(options.MaxHeartbeatInterval / time.Millisecond)
	identifyHeartbeatInterval(t, conn, hbi, nsq.FrameTypeResponse, "OK")
}

func TestMaxHeartbeatIntervalInvalid(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	options := NewNSQDOptions()
	options.MaxHeartbeatInterval = 300 * time.Second
	tcpAddr, _, nsqd := mustStartNSQD(options)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	assert.Equal(t, err, nil)

	hbi := int(options.MaxHeartbeatInterval/time.Millisecond + 1)
	identifyHeartbeatInterval(t, conn, hbi, nsq.FrameTypeError, "E_BAD_BODY IDENTIFY heartbeat interval (300001) is invalid")
}

func TestPausing(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	topicName := "test_pause_v2" + strconv.Itoa(int(time.Now().Unix()))

	tcpAddr, _, nsqd := mustStartNSQD(NewNSQDOptions())
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	assert.Equal(t, err, nil)

	identify(t, conn)
	sub(t, conn, topicName, "ch")

	err = nsq.Ready(1).Write(conn)
	assert.Equal(t, err, nil)

	topic := nsqd.GetTopic(topicName)
	msg := nsq.NewMessage(<-nsqd.idChan, []byte("test body"))
	channel := topic.GetChannel("ch")
	topic.PutMessage(msg)

	// receive the first message via the client, finish it, and send new RDY
	resp, _ := nsq.ReadResponse(conn)
	_, data, _ := nsq.UnpackResponse(resp)
	msg, err = nsq.DecodeMessage(data)
	assert.Equal(t, msg.Body, []byte("test body"))

	err = nsq.Finish(msg.Id).Write(conn)
	assert.Equal(t, err, nil)

	err = nsq.Ready(1).Write(conn)
	assert.Equal(t, err, nil)

	// sleep to allow the RDY state to take effect
	time.Sleep(50 * time.Millisecond)

	// pause the channel... the client shouldn't receive any more messages
	channel.Pause()

	// sleep to allow the paused state to take effect
	time.Sleep(50 * time.Millisecond)

	msg = nsq.NewMessage(<-nsqd.idChan, []byte("test body2"))
	topic.PutMessage(msg)

	// allow the client to possibly get a message, the test would hang indefinitely
	// if pausing was not working on the internal clientMsgChan read
	time.Sleep(50 * time.Millisecond)
	msg = <-channel.clientMsgChan
	assert.Equal(t, msg.Body, []byte("test body2"))

	// unpause the channel... the client should now be pushed a message
	channel.UnPause()

	msg = nsq.NewMessage(<-nsqd.idChan, []byte("test body3"))
	topic.PutMessage(msg)

	resp, _ = nsq.ReadResponse(conn)
	_, data, _ = nsq.UnpackResponse(resp)
	msg, err = nsq.DecodeMessage(data)
	assert.Equal(t, msg.Body, []byte("test body3"))
}

func TestEmptyCommand(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	tcpAddr, _, nsqd := mustStartNSQD(NewNSQDOptions())
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	assert.Equal(t, err, nil)

	_, err = conn.Write([]byte("\n\n"))
	assert.Equal(t, err, nil)

	// if we didn't panic here we're good, see issue #120
}

func TestSizeLimits(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	options := NewNSQDOptions()
	*verbose = true
	options.MaxMessageSize = 100
	options.MaxBodySize = 1000
	tcpAddr, _, nsqd := mustStartNSQD(options)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	assert.Equal(t, err, nil)

	topicName := "test_limits_v2" + strconv.Itoa(int(time.Now().Unix()))

	identify(t, conn)
	sub(t, conn, topicName, "ch")

	// PUB that's valid
	nsq.Publish(topicName, make([]byte, 95)).Write(conn)
	resp, _ := nsq.ReadResponse(conn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	log.Printf("frameType: %d, data: %s", frameType, data)
	assert.Equal(t, frameType, nsq.FrameTypeResponse)
	assert.Equal(t, data, []byte("OK"))

	// PUB that's invalid (too big)
	nsq.Publish(topicName, make([]byte, 105)).Write(conn)
	resp, _ = nsq.ReadResponse(conn)
	frameType, data, _ = nsq.UnpackResponse(resp)
	log.Printf("frameType: %d, data: %s", frameType, data)
	assert.Equal(t, frameType, nsq.FrameTypeError)
	assert.Equal(t, string(data), fmt.Sprintf("E_BAD_MESSAGE PUB message too big 105 > 100"))

	// need to reconnect
	conn, err = mustConnectNSQD(tcpAddr)
	assert.Equal(t, err, nil)

	// PUB thats empty
	nsq.Publish(topicName, make([]byte, 0)).Write(conn)
	resp, _ = nsq.ReadResponse(conn)
	frameType, data, _ = nsq.UnpackResponse(resp)
	log.Printf("frameType: %d, data: %s", frameType, data)
	assert.Equal(t, frameType, nsq.FrameTypeError)
	assert.Equal(t, string(data), fmt.Sprintf("E_BAD_MESSAGE PUB invalid message body size 0"))

	// need to reconnect
	conn, err = mustConnectNSQD(tcpAddr)
	assert.Equal(t, err, nil)

	// MPUB body that's valid
	mpub := make([][]byte, 0)
	for i := 0; i < 5; i++ {
		mpub = append(mpub, make([]byte, 100))
	}
	cmd, _ := nsq.MultiPublish(topicName, mpub)
	cmd.Write(conn)
	resp, _ = nsq.ReadResponse(conn)
	frameType, data, _ = nsq.UnpackResponse(resp)
	log.Printf("frameType: %d, data: %s", frameType, data)
	assert.Equal(t, frameType, nsq.FrameTypeResponse)
	assert.Equal(t, data, []byte("OK"))

	// MPUB body that's invalid (body too big)
	mpub = make([][]byte, 0)
	for i := 0; i < 11; i++ {
		mpub = append(mpub, make([]byte, 100))
	}
	cmd, _ = nsq.MultiPublish(topicName, mpub)
	cmd.Write(conn)
	resp, _ = nsq.ReadResponse(conn)
	frameType, data, _ = nsq.UnpackResponse(resp)
	log.Printf("frameType: %d, data: %s", frameType, data)
	assert.Equal(t, frameType, nsq.FrameTypeError)
	assert.Equal(t, string(data), fmt.Sprintf("E_BAD_BODY MPUB body too big 1148 > 1000"))

	// need to reconnect
	conn, err = mustConnectNSQD(tcpAddr)
	assert.Equal(t, err, nil)

	// MPUB that's invalid (one message empty)
	mpub = make([][]byte, 0)
	for i := 0; i < 5; i++ {
		mpub = append(mpub, make([]byte, 100))
	}
	mpub = append(mpub, make([]byte, 0))
	cmd, _ = nsq.MultiPublish(topicName, mpub)
	cmd.Write(conn)
	resp, _ = nsq.ReadResponse(conn)
	frameType, data, _ = nsq.UnpackResponse(resp)
	log.Printf("frameType: %d, data: %s", frameType, data)
	assert.Equal(t, frameType, nsq.FrameTypeError)
	assert.Equal(t, string(data), fmt.Sprintf("E_BAD_MESSAGE MPUB invalid message(5) body size 0"))

	// need to reconnect
	conn, err = mustConnectNSQD(tcpAddr)
	assert.Equal(t, err, nil)

	// MPUB body that's invalid (one of the messages is too big)
	mpub = make([][]byte, 0)
	for i := 0; i < 5; i++ {
		mpub = append(mpub, make([]byte, 101))
	}
	cmd, _ = nsq.MultiPublish(topicName, mpub)
	cmd.Write(conn)
	resp, _ = nsq.ReadResponse(conn)
	frameType, data, _ = nsq.UnpackResponse(resp)
	log.Printf("frameType: %d, data: %s", frameType, data)
	assert.Equal(t, frameType, nsq.FrameTypeError)
	assert.Equal(t, string(data), fmt.Sprintf("E_BAD_MESSAGE MPUB message too big 101 > 100"))
}

func TestTouch(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	*verbose = true
	options := NewNSQDOptions()
	options.MsgTimeout = 50 * time.Millisecond
	tcpAddr, _, nsqd := mustStartNSQD(options)
	defer nsqd.Exit()

	topicName := "test_touch" + strconv.Itoa(int(time.Now().Unix()))

	conn, err := mustConnectNSQD(tcpAddr)
	assert.Equal(t, err, nil)

	identify(t, conn)
	sub(t, conn, topicName, "ch")

	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("ch")
	msg := nsq.NewMessage(<-nsqd.idChan, []byte("test body"))
	topic.PutMessage(msg)

	err = nsq.Ready(1).Write(conn)
	assert.Equal(t, err, nil)

	resp, err := nsq.ReadResponse(conn)
	assert.Equal(t, err, nil)
	frameType, data, err := nsq.UnpackResponse(resp)
	msgOut, _ := nsq.DecodeMessage(data)
	assert.Equal(t, frameType, nsq.FrameTypeMessage)
	assert.Equal(t, msgOut.Id, msg.Id)

	time.Sleep(25 * time.Millisecond)

	err = nsq.Touch(msg.Id).Write(conn)
	assert.Equal(t, err, nil)

	time.Sleep(30 * time.Millisecond)

	err = nsq.Finish(msg.Id).Write(conn)
	assert.Equal(t, err, nil)

	assert.Equal(t, channel.timeoutCount, uint64(0))
}

func TestMaxRdyCount(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	*verbose = true
	options := NewNSQDOptions()
	options.MaxRdyCount = 50
	tcpAddr, _, nsqd := mustStartNSQD(options)
	defer nsqd.Exit()

	topicName := "test_max_rdy_count" + strconv.Itoa(int(time.Now().Unix()))

	conn, err := mustConnectNSQD(tcpAddr)
	assert.Equal(t, err, nil)

	topic := nsqd.GetTopic(topicName)
	msg := nsq.NewMessage(<-nsqd.idChan, []byte("test body"))
	topic.PutMessage(msg)

	data := identifyFeatureNegotiation(t, conn, nil)
	r := struct {
		MaxRdyCount int64 `json:"max_rdy_count"`
	}{}
	err = json.Unmarshal(data, &r)
	assert.Equal(t, err, nil)
	assert.Equal(t, r.MaxRdyCount, int64(50))
	sub(t, conn, topicName, "ch")

	err = nsq.Ready(int(options.MaxRdyCount)).Write(conn)
	assert.Equal(t, err, nil)

	resp, err := nsq.ReadResponse(conn)
	assert.Equal(t, err, nil)
	frameType, data, err := nsq.UnpackResponse(resp)
	msgOut, _ := nsq.DecodeMessage(data)
	assert.Equal(t, frameType, nsq.FrameTypeMessage)
	assert.Equal(t, msgOut.Id, msg.Id)

	err = nsq.Ready(int(options.MaxRdyCount) + 1).Write(conn)
	assert.Equal(t, err, nil)

	resp, err = nsq.ReadResponse(conn)
	assert.Equal(t, err, nil)
	frameType, data, err = nsq.UnpackResponse(resp)
	assert.Equal(t, frameType, int32(1))
	assert.Equal(t, string(data), "E_INVALID RDY count 51 out of range 0-50")
}

func TestFatalError(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	tcpAddr, _, nsqd := mustStartNSQD(NewNSQDOptions())
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	assert.Equal(t, err, nil)

	_, err = conn.Write([]byte("ASDF\n"))
	assert.Equal(t, err, nil)

	resp, err := nsq.ReadResponse(conn)
	assert.Equal(t, err, nil)
	frameType, data, err := nsq.UnpackResponse(resp)
	assert.Equal(t, frameType, int32(1))
	assert.Equal(t, string(data), "E_INVALID invalid command ASDF")

	_, err = nsq.ReadResponse(conn)
	assert.NotEqual(t, err, nil)
}

func TestOutputBuffering(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	*verbose = true
	options := NewNSQDOptions()
	options.MaxOutputBufferSize = 512 * 1024
	options.MaxOutputBufferTimeout = time.Second
	tcpAddr, _, nsqd := mustStartNSQD(options)
	defer nsqd.Exit()

	topicName := "test_output_buffering" + strconv.Itoa(int(time.Now().Unix()))

	conn, err := mustConnectNSQD(tcpAddr)
	assert.Equal(t, err, nil)

	outputBufferSize := 256 * 1024
	outputBufferTimeout := 500

	topic := nsqd.GetTopic(topicName)
	msg := nsq.NewMessage(<-nsqd.idChan, make([]byte, outputBufferSize-1024))
	topic.PutMessage(msg)

	start := time.Now()
	identifyOutputBuffering(t, conn, outputBufferSize, outputBufferTimeout, nsq.FrameTypeResponse, "OK")
	sub(t, conn, topicName, "ch")

	err = nsq.Ready(10).Write(conn)
	assert.Equal(t, err, nil)

	resp, err := nsq.ReadResponse(conn)
	assert.Equal(t, err, nil)
	end := time.Now()

	assert.Equal(t, int(end.Sub(start)/time.Millisecond) >= outputBufferTimeout, true)

	frameType, data, err := nsq.UnpackResponse(resp)
	msgOut, _ := nsq.DecodeMessage(data)
	assert.Equal(t, frameType, nsq.FrameTypeMessage)
	assert.Equal(t, msgOut.Id, msg.Id)
}

func TestOutputBufferingValidity(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	*verbose = true
	options := NewNSQDOptions()
	options.MaxOutputBufferSize = 512 * 1024
	options.MaxOutputBufferTimeout = time.Second
	tcpAddr, _, nsqd := mustStartNSQD(options)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	assert.Equal(t, err, nil)

	identifyOutputBuffering(t, conn, 512*1024, 1000, nsq.FrameTypeResponse, "OK")
	identifyOutputBuffering(t, conn, -1, -1, nsq.FrameTypeResponse, "OK")
	identifyOutputBuffering(t, conn, 0, 0, nsq.FrameTypeResponse, "OK")
	identifyOutputBuffering(t, conn, 512*1024+1, 0, nsq.FrameTypeError, fmt.Sprintf("E_BAD_BODY IDENTIFY output buffer size (%d) is invalid", 512*1024+1))

	conn, err = mustConnectNSQD(tcpAddr)
	assert.Equal(t, err, nil)

	identifyOutputBuffering(t, conn, 0, 1001, nsq.FrameTypeError, "E_BAD_BODY IDENTIFY output buffer timeout (1001) is invalid")
}

func TestTLS(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	*verbose = true
	options := NewNSQDOptions()
	options.TLSCert = "./test/cert.pem"
	options.TLSKey = "./test/key.pem"
	tcpAddr, _, nsqd := mustStartNSQD(options)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	assert.Equal(t, err, nil)

	data := identifyFeatureNegotiation(t, conn, map[string]interface{}{"tls_v1": true})
	r := struct {
		TLSv1 bool `json:"tls_v1"`
	}{}
	err = json.Unmarshal(data, &r)
	assert.Equal(t, err, nil)
	assert.Equal(t, r.TLSv1, true)

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}
	tlsConn := tls.Client(conn, tlsConfig)

	err = tlsConn.Handshake()
	assert.Equal(t, err, nil)

	resp, _ := nsq.ReadResponse(tlsConn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	log.Printf("frameType: %d, data: %s", frameType, data)
	assert.Equal(t, frameType, nsq.FrameTypeResponse)
	assert.Equal(t, data, []byte("OK"))
}

func TestDeflate(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	*verbose = true
	options := NewNSQDOptions()
	options.DeflateEnabled = true
	tcpAddr, _, nsqd := mustStartNSQD(options)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	assert.Equal(t, err, nil)

	data := identifyFeatureNegotiation(t, conn, map[string]interface{}{"deflate": true})
	r := struct {
		Deflate bool `json:"deflate"`
	}{}
	err = json.Unmarshal(data, &r)
	assert.Equal(t, err, nil)
	assert.Equal(t, r.Deflate, true)

	compressConn := flate.NewReader(conn)
	resp, _ := nsq.ReadResponse(compressConn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	log.Printf("frameType: %d, data: %s", frameType, data)
	assert.Equal(t, frameType, nsq.FrameTypeResponse)
	assert.Equal(t, data, []byte("OK"))
}

type readWriter struct {
	io.Reader
	io.Writer
}

func TestSnappy(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	*verbose = true
	options := NewNSQDOptions()
	options.SnappyEnabled = true
	tcpAddr, _, nsqd := mustStartNSQD(options)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	assert.Equal(t, err, nil)

	data := identifyFeatureNegotiation(t, conn, map[string]interface{}{"snappy": true})
	r := struct {
		Snappy bool `json:"snappy"`
	}{}
	err = json.Unmarshal(data, &r)
	assert.Equal(t, err, nil)
	assert.Equal(t, r.Snappy, true)

	compressConn := snappystream.NewReader(conn, snappystream.SkipVerifyChecksum)
	resp, _ := nsq.ReadResponse(compressConn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	log.Printf("frameType: %d, data: %s", frameType, data)
	assert.Equal(t, frameType, nsq.FrameTypeResponse)
	assert.Equal(t, data, []byte("OK"))

	msgBody := make([]byte, 128000)
	w := snappystream.NewWriter(conn)

	rw := readWriter{compressConn, w}

	topicName := "test_snappy" + strconv.Itoa(int(time.Now().Unix()))
	sub(t, rw, topicName, "ch")

	err = nsq.Ready(1).Write(rw)
	assert.Equal(t, err, nil)

	topic := nsqd.GetTopic(topicName)
	msg := nsq.NewMessage(<-nsqd.idChan, msgBody)
	topic.PutMessage(msg)

	resp, _ = nsq.ReadResponse(compressConn)
	frameType, data, _ = nsq.UnpackResponse(resp)
	msgOut, _ := nsq.DecodeMessage(data)
	assert.Equal(t, frameType, nsq.FrameTypeMessage)
	assert.Equal(t, msgOut.Id, msg.Id)
	assert.Equal(t, msgOut.Body, msg.Body)
}

func TestTLSDeflate(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	*verbose = true
	options := NewNSQDOptions()
	options.DeflateEnabled = true
	options.TLSCert = "./test/cert.pem"
	options.TLSKey = "./test/key.pem"
	tcpAddr, _, nsqd := mustStartNSQD(options)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	assert.Equal(t, err, nil)

	data := identifyFeatureNegotiation(t, conn, map[string]interface{}{"tls_v1": true, "deflate": true})
	r := struct {
		TLSv1   bool `json:"tls_v1"`
		Deflate bool `json:"deflate"`
	}{}
	err = json.Unmarshal(data, &r)
	assert.Equal(t, err, nil)
	assert.Equal(t, r.TLSv1, true)
	assert.Equal(t, r.Deflate, true)

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}
	tlsConn := tls.Client(conn, tlsConfig)

	err = tlsConn.Handshake()
	assert.Equal(t, err, nil)

	resp, _ := nsq.ReadResponse(tlsConn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	log.Printf("frameType: %d, data: %s", frameType, data)
	assert.Equal(t, frameType, nsq.FrameTypeResponse)
	assert.Equal(t, data, []byte("OK"))

	compressConn := flate.NewReader(tlsConn)

	resp, _ = nsq.ReadResponse(compressConn)
	frameType, data, _ = nsq.UnpackResponse(resp)
	log.Printf("frameType: %d, data: %s", frameType, data)
	assert.Equal(t, frameType, nsq.FrameTypeResponse)
	assert.Equal(t, data, []byte("OK"))
}

func TestSampling(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	rand.Seed(time.Now().UTC().UnixNano())

	num := 3000

	*verbose = true
	options := NewNSQDOptions()
	options.MaxRdyCount = int64(num)
	tcpAddr, _, nsqd := mustStartNSQD(options)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	assert.Equal(t, err, nil)

	data := identifyFeatureNegotiation(t, conn, map[string]interface{}{"sample_rate": int32(42)})
	r := struct {
		SampleRate int32 `json:"sample_rate"`
	}{}
	err = json.Unmarshal(data, &r)
	assert.Equal(t, err, nil)
	assert.Equal(t, r.SampleRate, int32(42))

	topicName := "test_sampling" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	for i := 0; i < num; i++ {
		msg := nsq.NewMessage(<-nsqd.idChan, []byte("test body"))
		topic.PutMessage(msg)
	}
	channel := topic.GetChannel("ch")

	// let the topic drain into the channel
	time.Sleep(50 * time.Millisecond)

	sub(t, conn, topicName, "ch")
	err = nsq.Ready(num).Write(conn)
	assert.Equal(t, err, nil)

	doneChan := make(chan int)
	go func() {
		for {
			if channel.Depth() == 0 {
				close(doneChan)
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	<-doneChan
	// within 3%
	assert.Equal(t, len(channel.inFlightMessages) <= int(float64(num)*0.45), true)
	assert.Equal(t, len(channel.inFlightMessages) >= int(float64(num)*0.39), true)
}

func TestTLSSnappy(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	*verbose = true
	options := NewNSQDOptions()
	options.SnappyEnabled = true
	options.TLSCert = "./test/cert.pem"
	options.TLSKey = "./test/key.pem"
	tcpAddr, _, nsqd := mustStartNSQD(options)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	assert.Equal(t, err, nil)

	data := identifyFeatureNegotiation(t, conn, map[string]interface{}{"tls_v1": true, "snappy": true})
	r := struct {
		TLSv1  bool `json:"tls_v1"`
		Snappy bool `json:"snappy"`
	}{}
	err = json.Unmarshal(data, &r)
	assert.Equal(t, err, nil)
	assert.Equal(t, r.TLSv1, true)
	assert.Equal(t, r.Snappy, true)

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}
	tlsConn := tls.Client(conn, tlsConfig)

	err = tlsConn.Handshake()
	assert.Equal(t, err, nil)

	resp, _ := nsq.ReadResponse(tlsConn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	log.Printf("frameType: %d, data: %s", frameType, data)
	assert.Equal(t, frameType, nsq.FrameTypeResponse)
	assert.Equal(t, data, []byte("OK"))

	compressConn := snappystream.NewReader(tlsConn, snappystream.SkipVerifyChecksum)

	resp, _ = nsq.ReadResponse(compressConn)
	frameType, data, _ = nsq.UnpackResponse(resp)
	log.Printf("frameType: %d, data: %s", frameType, data)
	assert.Equal(t, frameType, nsq.FrameTypeResponse)
	assert.Equal(t, data, []byte("OK"))
}

func BenchmarkProtocolV2Exec(b *testing.B) {
	b.StopTimer()
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)
	p := &ProtocolV2{}
	c := NewClientV2(0, nil, nil)
	params := [][]byte{[]byte("NOP")}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		p.Exec(c, params)
	}
}

func benchmarkProtocolV2Pub(b *testing.B, size int) {
	var wg sync.WaitGroup
	b.StopTimer()
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)
	options := NewNSQDOptions()
	options.MemQueueSize = int64(b.N)
	tcpAddr, _, nsqd := mustStartNSQD(options)
	msg := make([]byte, size)
	batchSize := 200
	batch := make([][]byte, 0)
	for i := 0; i < batchSize; i++ {
		batch = append(batch, msg)
	}
	topicName := "bench_v2_pub" + strconv.Itoa(int(time.Now().Unix()))
	b.SetBytes(int64(len(msg)))
	b.StartTimer()

	for j := 0; j < runtime.GOMAXPROCS(0); j++ {
		wg.Add(1)
		go func() {
			conn, err := mustConnectNSQD(tcpAddr)
			if err != nil {
				panic(err.Error())
			}
			rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

			num := b.N / runtime.GOMAXPROCS(0) / batchSize
			for i := 0; i < num; i += 1 {
				cmd, _ := nsq.MultiPublish(topicName, batch)
				err := cmd.Write(rw)
				if err != nil {
					panic(err.Error())
				}
				err = rw.Flush()
				if err != nil {
					panic(err.Error())
				}
				resp, err := nsq.ReadResponse(rw)
				if err != nil {
					panic(err.Error())
				}
				_, data, _ := nsq.UnpackResponse(resp)
				if !bytes.Equal(data, []byte("OK")) {
					panic("invalid response")
				}
			}
			wg.Done()
		}()
	}

	wg.Wait()

	b.StopTimer()
	nsqd.Exit()
}

func BenchmarkProtocolV2Pub256(b *testing.B)  { benchmarkProtocolV2Pub(b, 256) }
func BenchmarkProtocolV2Pub512(b *testing.B)  { benchmarkProtocolV2Pub(b, 512) }
func BenchmarkProtocolV2Pub1k(b *testing.B)   { benchmarkProtocolV2Pub(b, 1024) }
func BenchmarkProtocolV2Pub2k(b *testing.B)   { benchmarkProtocolV2Pub(b, 2*1024) }
func BenchmarkProtocolV2Pub4k(b *testing.B)   { benchmarkProtocolV2Pub(b, 4*1024) }
func BenchmarkProtocolV2Pub8k(b *testing.B)   { benchmarkProtocolV2Pub(b, 8*1024) }
func BenchmarkProtocolV2Pub16k(b *testing.B)  { benchmarkProtocolV2Pub(b, 16*1024) }
func BenchmarkProtocolV2Pub32k(b *testing.B)  { benchmarkProtocolV2Pub(b, 32*1024) }
func BenchmarkProtocolV2Pub64k(b *testing.B)  { benchmarkProtocolV2Pub(b, 64*1024) }
func BenchmarkProtocolV2Pub128k(b *testing.B) { benchmarkProtocolV2Pub(b, 128*1024) }
func BenchmarkProtocolV2Pub256k(b *testing.B) { benchmarkProtocolV2Pub(b, 256*1024) }
func BenchmarkProtocolV2Pub512k(b *testing.B) { benchmarkProtocolV2Pub(b, 512*1024) }
func BenchmarkProtocolV2Pub1m(b *testing.B)   { benchmarkProtocolV2Pub(b, 1024*1024) }

func benchmarkProtocolV2Sub(b *testing.B, size int) {
	var wg sync.WaitGroup
	b.StopTimer()
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)
	options := NewNSQDOptions()
	options.MemQueueSize = int64(b.N)
	tcpAddr, _, nsqd := mustStartNSQD(options)
	msg := make([]byte, size)
	topicName := "bench_v2_sub" + strconv.Itoa(b.N) + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	for i := 0; i < b.N; i++ {
		msg := nsq.NewMessage(<-nsqd.idChan, msg)
		topic.PutMessage(msg)
	}
	topic.GetChannel("ch")
	b.SetBytes(int64(len(msg)))
	goChan := make(chan int)
	rdyChan := make(chan int)
	workers := runtime.GOMAXPROCS(0)
	for j := 0; j < workers; j++ {
		wg.Add(1)
		go func() {
			subWorker(b.N, workers, tcpAddr, topicName, rdyChan, goChan)
			wg.Done()
		}()
		<-rdyChan
	}
	b.StartTimer()

	close(goChan)
	wg.Wait()

	b.StopTimer()
	nsqd.Exit()
}

func subWorker(n int, workers int, tcpAddr *net.TCPAddr, topicName string, rdyChan chan int, goChan chan int) {
	conn, err := mustConnectNSQD(tcpAddr)
	if err != nil {
		panic(err.Error())
	}
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	identify(nil, conn)
	sub(nil, conn, topicName, "ch")

	rdyCount := int(math.Min(math.Max(float64(n/workers), 1), 2500))
	rdyChan <- 1
	<-goChan
	nsq.Ready(rdyCount).Write(rw)
	rw.Flush()
	num := n / workers
	numRdy := num/rdyCount - 1
	rdy := rdyCount
	for i := 0; i < num; i += 1 {
		resp, err := nsq.ReadResponse(rw)
		if err != nil {
			panic(err.Error())
		}
		frameType, data, err := nsq.UnpackResponse(resp)
		if err != nil {
			panic(err.Error())
		}
		if frameType != nsq.FrameTypeMessage {
			panic("got something else")
		}
		msg, err := nsq.DecodeMessage(data)
		if err != nil {
			panic(err.Error())
		}
		nsq.Finish(msg.Id).Write(rw)
		rdy--
		if rdy == 0 && numRdy > 0 {
			nsq.Ready(rdyCount).Write(rw)
			rdy = rdyCount
			numRdy--
			rw.Flush()
		}
	}
}

func BenchmarkProtocolV2Sub256(b *testing.B)  { benchmarkProtocolV2Sub(b, 256) }
func BenchmarkProtocolV2Sub512(b *testing.B)  { benchmarkProtocolV2Sub(b, 512) }
func BenchmarkProtocolV2Sub1k(b *testing.B)   { benchmarkProtocolV2Sub(b, 1024) }
func BenchmarkProtocolV2Sub2k(b *testing.B)   { benchmarkProtocolV2Sub(b, 2*1024) }
func BenchmarkProtocolV2Sub4k(b *testing.B)   { benchmarkProtocolV2Sub(b, 4*1024) }
func BenchmarkProtocolV2Sub8k(b *testing.B)   { benchmarkProtocolV2Sub(b, 8*1024) }
func BenchmarkProtocolV2Sub16k(b *testing.B)  { benchmarkProtocolV2Sub(b, 16*1024) }
func BenchmarkProtocolV2Sub32k(b *testing.B)  { benchmarkProtocolV2Sub(b, 32*1024) }
func BenchmarkProtocolV2Sub64k(b *testing.B)  { benchmarkProtocolV2Sub(b, 64*1024) }
func BenchmarkProtocolV2Sub128k(b *testing.B) { benchmarkProtocolV2Sub(b, 128*1024) }
func BenchmarkProtocolV2Sub256k(b *testing.B) { benchmarkProtocolV2Sub(b, 256*1024) }
func BenchmarkProtocolV2Sub512k(b *testing.B) { benchmarkProtocolV2Sub(b, 512*1024) }
func BenchmarkProtocolV2Sub1m(b *testing.B)   { benchmarkProtocolV2Sub(b, 1024*1024) }

func benchmarkProtocolV2MultiSub(b *testing.B, num int) {
	var wg sync.WaitGroup
	b.StopTimer()

	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	options := NewNSQDOptions()
	options.MemQueueSize = int64(b.N)
	tcpAddr, _, nsqd := mustStartNSQD(options)
	msg := make([]byte, 256)
	b.SetBytes(int64(len(msg) * num))

	goChan := make(chan int)
	rdyChan := make(chan int)
	workers := runtime.GOMAXPROCS(0)
	for i := 0; i < num; i++ {
		topicName := "bench_v2" + strconv.Itoa(b.N) + "_" + strconv.Itoa(i) + "_" + strconv.Itoa(int(time.Now().Unix()))
		topic := nsqd.GetTopic(topicName)
		for i := 0; i < b.N; i++ {
			msg := nsq.NewMessage(<-nsqd.idChan, msg)
			topic.PutMessage(msg)
		}
		topic.GetChannel("ch")

		for j := 0; j < workers; j++ {
			wg.Add(1)
			go func() {
				subWorker(b.N, workers, tcpAddr, topicName, rdyChan, goChan)
				wg.Done()
			}()
			<-rdyChan
		}
	}
	b.StartTimer()

	close(goChan)
	wg.Wait()

	b.StopTimer()
	nsqd.Exit()
}

func BenchmarkProtocolV2MultiSub1(b *testing.B)  { benchmarkProtocolV2MultiSub(b, 1) }
func BenchmarkProtocolV2MultiSub2(b *testing.B)  { benchmarkProtocolV2MultiSub(b, 2) }
func BenchmarkProtocolV2MultiSub4(b *testing.B)  { benchmarkProtocolV2MultiSub(b, 4) }
func BenchmarkProtocolV2MultiSub8(b *testing.B)  { benchmarkProtocolV2MultiSub(b, 8) }
func BenchmarkProtocolV2MultiSub16(b *testing.B) { benchmarkProtocolV2MultiSub(b, 16) }
