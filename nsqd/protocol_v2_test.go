package nsqd

import (
	"bufio"
	"bytes"
	"compress/flate"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/bitly/go-nsq"
	"github.com/bitly/nsq/internal/protocol"
	"github.com/mreiferson/go-snappystream"
)

func mustStartNSQD(opts *nsqdOptions) (*net.TCPAddr, *net.TCPAddr, *NSQD) {
	opts.TCPAddress = "127.0.0.1:0"
	opts.HTTPAddress = "127.0.0.1:0"
	opts.HTTPSAddress = "127.0.0.1:0"
	opts.DataPath = os.TempDir()
	nsqd := NewNSQD(opts)
	nsqd.Main()
	return nsqd.tcpListener.Addr().(*net.TCPAddr),
		nsqd.httpListener.Addr().(*net.TCPAddr),
		nsqd
}

func mustConnectNSQD(tcpAddr *net.TCPAddr) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", tcpAddr.String(), time.Second)
	if err != nil {
		return nil, err
	}
	conn.Write(nsq.MagicV2)
	return conn, nil
}

func identify(t *testing.T, conn io.ReadWriter, extra map[string]interface{}, f int32) []byte {
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
	_, err := cmd.WriteTo(conn)
	equal(t, err, nil)
	resp, err := nsq.ReadResponse(conn)
	equal(t, err, nil)
	frameType, data, err := nsq.UnpackResponse(resp)
	equal(t, err, nil)
	equal(t, frameType, f)
	return data
}

func sub(t *testing.T, conn io.ReadWriter, topicName string, channelName string) {
	_, err := nsq.Subscribe(topicName, channelName).WriteTo(conn)
	equal(t, err, nil)
	readValidate(t, conn, frameTypeResponse, "OK")
}

func authCmd(t *testing.T, conn io.ReadWriter, authSecret string, expectSuccess string) {
	auth := &nsq.Command{[]byte("AUTH"), nil, []byte(authSecret)}
	_, err := auth.WriteTo(conn)
	equal(t, err, nil)
	if expectSuccess != "" {
		readValidate(t, conn, nsq.FrameTypeResponse, expectSuccess)
	}
}

func subFail(t *testing.T, conn io.ReadWriter, topicName string, channelName string) {
	_, err := nsq.Subscribe(topicName, channelName).WriteTo(conn)
	equal(t, err, nil)
	resp, err := nsq.ReadResponse(conn)
	frameType, _, err := nsq.UnpackResponse(resp)
	equal(t, frameType, frameTypeError)
}

func readValidate(t *testing.T, conn io.Reader, f int32, d string) []byte {
	resp, err := nsq.ReadResponse(conn)
	equal(t, err, nil)
	frameType, data, err := nsq.UnpackResponse(resp)
	t.Logf("%v %s %s", frameType, data, err)
	equal(t, err, nil)
	equal(t, frameType, f)
	equal(t, string(data), d)
	return data
}

// test channel/topic names
func TestChannelTopicNames(t *testing.T) {
	equal(t, protocol.IsValidChannelName("test"), true)
	equal(t, protocol.IsValidChannelName("test-with_period."), true)
	equal(t, protocol.IsValidChannelName("test#ephemeral"), true)
	equal(t, protocol.IsValidTopicName("test"), true)
	equal(t, protocol.IsValidTopicName("test-with_period."), true)
	equal(t, protocol.IsValidTopicName("test#ephemeral"), true)
	equal(t, protocol.IsValidTopicName("test:ephemeral"), false)
}

// exercise the basic operations of the V2 protocol
func TestBasicV2(t *testing.T) {
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	opts.ClientTimeout = 60 * time.Second
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	topicName := "test_v2" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	msg := NewMessage(<-nsqd.idChan, []byte("test body"))
	topic.PutMessage(msg)

	conn, err := mustConnectNSQD(tcpAddr)
	equal(t, err, nil)
	defer conn.Close()

	identify(t, conn, nil, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	_, err = nsq.Ready(1).WriteTo(conn)
	equal(t, err, nil)

	resp, err := nsq.ReadResponse(conn)
	equal(t, err, nil)
	frameType, data, err := nsq.UnpackResponse(resp)
	msgOut, _ := decodeMessage(data)
	equal(t, frameType, frameTypeMessage)
	equal(t, msgOut.ID, msg.ID)
	equal(t, msgOut.Body, msg.Body)
	equal(t, msgOut.Attempts, uint16(1))
}

func TestMultipleConsumerV2(t *testing.T) {
	msgChan := make(chan *Message)

	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	opts.ClientTimeout = 60 * time.Second
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	topicName := "test_multiple_v2" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	msg := NewMessage(<-nsqd.idChan, []byte("test body"))
	topic.GetChannel("ch1")
	topic.GetChannel("ch2")
	topic.PutMessage(msg)

	for _, i := range []string{"1", "2"} {
		conn, err := mustConnectNSQD(tcpAddr)
		equal(t, err, nil)
		defer conn.Close()

		identify(t, conn, nil, frameTypeResponse)
		sub(t, conn, topicName, "ch"+i)

		_, err = nsq.Ready(1).WriteTo(conn)
		equal(t, err, nil)

		go func(c net.Conn) {
			resp, _ := nsq.ReadResponse(c)
			_, data, _ := nsq.UnpackResponse(resp)
			msg, _ := decodeMessage(data)
			msgChan <- msg
		}(conn)
	}

	msgOut := <-msgChan
	equal(t, msgOut.ID, msg.ID)
	equal(t, msgOut.Body, msg.Body)
	equal(t, msgOut.Attempts, uint16(1))
	msgOut = <-msgChan
	equal(t, msgOut.ID, msg.ID)
	equal(t, msgOut.Body, msg.Body)
	equal(t, msgOut.Attempts, uint16(1))
}

func TestClientTimeout(t *testing.T) {
	topicName := "test_client_timeout_v2" + strconv.Itoa(int(time.Now().Unix()))

	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	opts.ClientTimeout = 150 * time.Millisecond
	opts.Verbose = true
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	equal(t, err, nil)
	defer conn.Close()

	identify(t, conn, nil, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	time.Sleep(150 * time.Millisecond)

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
	topicName := "test_hb_v2" + strconv.Itoa(int(time.Now().Unix()))

	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	opts.ClientTimeout = 200 * time.Millisecond
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	equal(t, err, nil)
	defer conn.Close()

	identify(t, conn, nil, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	_, err = nsq.Ready(1).WriteTo(conn)
	equal(t, err, nil)

	resp, _ := nsq.ReadResponse(conn)
	_, data, _ := nsq.UnpackResponse(resp)
	equal(t, data, []byte("_heartbeat_"))

	time.Sleep(20 * time.Millisecond)

	_, err = nsq.Nop().WriteTo(conn)
	equal(t, err, nil)

	// wait long enough that would have timed out (had we not sent the above cmd)
	time.Sleep(100 * time.Millisecond)

	_, err = nsq.Nop().WriteTo(conn)
	equal(t, err, nil)
}

func TestClientHeartbeatDisableSUB(t *testing.T) {
	topicName := "test_hb_v2" + strconv.Itoa(int(time.Now().Unix()))

	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	opts.ClientTimeout = 200 * time.Millisecond
	opts.Verbose = true
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	equal(t, err, nil)
	defer conn.Close()

	identify(t, conn, map[string]interface{}{
		"heartbeat_interval": -1,
	}, frameTypeResponse)
	subFail(t, conn, topicName, "ch")
}

func TestClientHeartbeatDisable(t *testing.T) {
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	opts.ClientTimeout = 100 * time.Millisecond
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	equal(t, err, nil)
	defer conn.Close()

	identify(t, conn, map[string]interface{}{
		"heartbeat_interval": -1,
	}, frameTypeResponse)

	time.Sleep(150 * time.Millisecond)

	_, err = nsq.Nop().WriteTo(conn)
	equal(t, err, nil)
}

func TestMaxHeartbeatIntervalValid(t *testing.T) {
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	opts.MaxHeartbeatInterval = 300 * time.Second
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	equal(t, err, nil)
	defer conn.Close()

	hbi := int(opts.MaxHeartbeatInterval / time.Millisecond)
	identify(t, conn, map[string]interface{}{
		"heartbeat_interval": hbi,
	}, frameTypeResponse)
}

func TestMaxHeartbeatIntervalInvalid(t *testing.T) {
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	opts.MaxHeartbeatInterval = 300 * time.Second
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	equal(t, err, nil)
	defer conn.Close()

	hbi := int(opts.MaxHeartbeatInterval/time.Millisecond + 1)
	data := identify(t, conn, map[string]interface{}{
		"heartbeat_interval": hbi,
	}, frameTypeError)
	equal(t, string(data), "E_BAD_BODY IDENTIFY heartbeat interval (300001) is invalid")
}

func TestPausing(t *testing.T) {
	topicName := "test_pause_v2" + strconv.Itoa(int(time.Now().Unix()))

	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	equal(t, err, nil)
	defer conn.Close()

	identify(t, conn, nil, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	_, err = nsq.Ready(1).WriteTo(conn)
	equal(t, err, nil)

	topic := nsqd.GetTopic(topicName)
	msg := NewMessage(<-nsqd.idChan, []byte("test body"))
	channel := topic.GetChannel("ch")
	topic.PutMessage(msg)

	// receive the first message via the client, finish it, and send new RDY
	resp, _ := nsq.ReadResponse(conn)
	_, data, _ := nsq.UnpackResponse(resp)
	msg, err = decodeMessage(data)
	equal(t, msg.Body, []byte("test body"))

	_, err = nsq.Finish(nsq.MessageID(msg.ID)).WriteTo(conn)
	equal(t, err, nil)

	_, err = nsq.Ready(1).WriteTo(conn)
	equal(t, err, nil)

	// sleep to allow the RDY state to take effect
	time.Sleep(50 * time.Millisecond)

	// pause the channel... the client shouldn't receive any more messages
	channel.Pause()

	// sleep to allow the paused state to take effect
	time.Sleep(50 * time.Millisecond)

	msg = NewMessage(<-nsqd.idChan, []byte("test body2"))
	topic.PutMessage(msg)

	// allow the client to possibly get a message, the test would hang indefinitely
	// if pausing was not working on the internal clientMsgChan read
	time.Sleep(50 * time.Millisecond)
	msg = <-channel.clientMsgChan
	equal(t, msg.Body, []byte("test body2"))

	// unpause the channel... the client should now be pushed a message
	channel.UnPause()

	msg = NewMessage(<-nsqd.idChan, []byte("test body3"))
	topic.PutMessage(msg)

	resp, _ = nsq.ReadResponse(conn)
	_, data, _ = nsq.UnpackResponse(resp)
	msg, err = decodeMessage(data)
	equal(t, msg.Body, []byte("test body3"))
}

func TestEmptyCommand(t *testing.T) {
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	equal(t, err, nil)
	defer conn.Close()

	_, err = conn.Write([]byte("\n\n"))
	equal(t, err, nil)

	// if we didn't panic here we're good, see issue #120
}

func TestSizeLimits(t *testing.T) {
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	opts.Verbose = true
	opts.MaxMsgSize = 100
	opts.MaxBodySize = 1000
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	equal(t, err, nil)
	defer conn.Close()

	topicName := "test_limits_v2" + strconv.Itoa(int(time.Now().Unix()))

	identify(t, conn, nil, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	// PUB that's valid
	nsq.Publish(topicName, make([]byte, 95)).WriteTo(conn)
	resp, _ := nsq.ReadResponse(conn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	equal(t, frameType, frameTypeResponse)
	equal(t, data, []byte("OK"))

	// PUB that's invalid (too big)
	nsq.Publish(topicName, make([]byte, 105)).WriteTo(conn)
	resp, _ = nsq.ReadResponse(conn)
	frameType, data, _ = nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	equal(t, frameType, frameTypeError)
	equal(t, string(data), fmt.Sprintf("E_BAD_MESSAGE PUB message too big 105 > 100"))

	// need to reconnect
	conn, err = mustConnectNSQD(tcpAddr)
	equal(t, err, nil)
	defer conn.Close()

	// PUB thats empty
	nsq.Publish(topicName, []byte{}).WriteTo(conn)
	resp, _ = nsq.ReadResponse(conn)
	frameType, data, _ = nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	equal(t, frameType, frameTypeError)
	equal(t, string(data), fmt.Sprintf("E_BAD_MESSAGE PUB invalid message body size 0"))

	// need to reconnect
	conn, err = mustConnectNSQD(tcpAddr)
	equal(t, err, nil)
	defer conn.Close()

	// MPUB body that's valid
	mpub := make([][]byte, 5)
	for i := range mpub {
		mpub[i] = make([]byte, 100)
	}
	cmd, _ := nsq.MultiPublish(topicName, mpub)
	cmd.WriteTo(conn)
	resp, _ = nsq.ReadResponse(conn)
	frameType, data, _ = nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	equal(t, frameType, frameTypeResponse)
	equal(t, data, []byte("OK"))

	// MPUB body that's invalid (body too big)
	mpub = make([][]byte, 11)
	for i := range mpub {
		mpub[i] = make([]byte, 100)
	}
	cmd, _ = nsq.MultiPublish(topicName, mpub)
	cmd.WriteTo(conn)
	resp, _ = nsq.ReadResponse(conn)
	frameType, data, _ = nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	equal(t, frameType, frameTypeError)
	equal(t, string(data), fmt.Sprintf("E_BAD_BODY MPUB body too big 1148 > 1000"))

	// need to reconnect
	conn, err = mustConnectNSQD(tcpAddr)
	equal(t, err, nil)
	defer conn.Close()

	// MPUB that's invalid (one message empty)
	mpub = make([][]byte, 5)
	for i := range mpub {
		mpub[i] = make([]byte, 100)
	}
	mpub = append(mpub, []byte{})
	cmd, _ = nsq.MultiPublish(topicName, mpub)
	cmd.WriteTo(conn)
	resp, _ = nsq.ReadResponse(conn)
	frameType, data, _ = nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	equal(t, frameType, frameTypeError)
	equal(t, string(data), fmt.Sprintf("E_BAD_MESSAGE MPUB invalid message(5) body size 0"))

	// need to reconnect
	conn, err = mustConnectNSQD(tcpAddr)
	equal(t, err, nil)
	defer conn.Close()

	// MPUB body that's invalid (one of the messages is too big)
	mpub = make([][]byte, 5)
	for i := range mpub {
		mpub[i] = make([]byte, 101)
	}
	cmd, _ = nsq.MultiPublish(topicName, mpub)
	cmd.WriteTo(conn)
	resp, _ = nsq.ReadResponse(conn)
	frameType, data, _ = nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	equal(t, frameType, frameTypeError)
	equal(t, string(data), fmt.Sprintf("E_BAD_MESSAGE MPUB message too big 101 > 100"))
}

func TestTouch(t *testing.T) {
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	opts.Verbose = true
	opts.MsgTimeout = 150 * time.Millisecond
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	topicName := "test_touch" + strconv.Itoa(int(time.Now().Unix()))

	conn, err := mustConnectNSQD(tcpAddr)
	equal(t, err, nil)
	defer conn.Close()

	identify(t, conn, nil, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("ch")
	msg := NewMessage(<-nsqd.idChan, []byte("test body"))
	topic.PutMessage(msg)

	_, err = nsq.Ready(1).WriteTo(conn)
	equal(t, err, nil)

	resp, err := nsq.ReadResponse(conn)
	equal(t, err, nil)
	frameType, data, err := nsq.UnpackResponse(resp)
	msgOut, _ := decodeMessage(data)
	equal(t, frameType, frameTypeMessage)
	equal(t, msgOut.ID, msg.ID)

	time.Sleep(75 * time.Millisecond)

	_, err = nsq.Touch(nsq.MessageID(msg.ID)).WriteTo(conn)
	equal(t, err, nil)

	time.Sleep(75 * time.Millisecond)

	_, err = nsq.Finish(nsq.MessageID(msg.ID)).WriteTo(conn)
	equal(t, err, nil)

	equal(t, channel.timeoutCount, uint64(0))
}

func TestMaxRdyCount(t *testing.T) {
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	opts.Verbose = true
	opts.MaxRdyCount = 50
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	topicName := "test_max_rdy_count" + strconv.Itoa(int(time.Now().Unix()))

	conn, err := mustConnectNSQD(tcpAddr)
	equal(t, err, nil)
	defer conn.Close()

	topic := nsqd.GetTopic(topicName)
	msg := NewMessage(<-nsqd.idChan, []byte("test body"))
	topic.PutMessage(msg)

	data := identify(t, conn, nil, frameTypeResponse)
	r := struct {
		MaxRdyCount int64 `json:"max_rdy_count"`
	}{}
	err = json.Unmarshal(data, &r)
	equal(t, err, nil)
	equal(t, r.MaxRdyCount, int64(50))
	sub(t, conn, topicName, "ch")

	_, err = nsq.Ready(int(opts.MaxRdyCount)).WriteTo(conn)
	equal(t, err, nil)

	resp, err := nsq.ReadResponse(conn)
	equal(t, err, nil)
	frameType, data, err := nsq.UnpackResponse(resp)
	msgOut, _ := decodeMessage(data)
	equal(t, frameType, frameTypeMessage)
	equal(t, msgOut.ID, msg.ID)

	_, err = nsq.Ready(int(opts.MaxRdyCount) + 1).WriteTo(conn)
	equal(t, err, nil)

	resp, err = nsq.ReadResponse(conn)
	equal(t, err, nil)
	frameType, data, err = nsq.UnpackResponse(resp)
	equal(t, frameType, int32(1))
	equal(t, string(data), "E_INVALID RDY count 51 out of range 0-50")
}

func TestFatalError(t *testing.T) {
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	equal(t, err, nil)
	defer conn.Close()

	_, err = conn.Write([]byte("ASDF\n"))
	equal(t, err, nil)

	resp, err := nsq.ReadResponse(conn)
	equal(t, err, nil)
	frameType, data, err := nsq.UnpackResponse(resp)
	equal(t, frameType, int32(1))
	equal(t, string(data), "E_INVALID invalid command ASDF")

	_, err = nsq.ReadResponse(conn)
	nequal(t, err, nil)
}

func TestOutputBuffering(t *testing.T) {
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	opts.Verbose = true
	opts.MaxOutputBufferSize = 512 * 1024
	opts.MaxOutputBufferTimeout = time.Second
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	topicName := "test_output_buffering" + strconv.Itoa(int(time.Now().Unix()))

	conn, err := mustConnectNSQD(tcpAddr)
	equal(t, err, nil)
	defer conn.Close()

	outputBufferSize := 256 * 1024
	outputBufferTimeout := 500

	topic := nsqd.GetTopic(topicName)
	msg := NewMessage(<-nsqd.idChan, make([]byte, outputBufferSize-1024))
	topic.PutMessage(msg)

	start := time.Now()
	data := identify(t, conn, map[string]interface{}{
		"output_buffer_size":    outputBufferSize,
		"output_buffer_timeout": outputBufferTimeout,
	}, frameTypeResponse)
	var decoded map[string]interface{}
	json.Unmarshal(data, &decoded)
	v, ok := decoded["output_buffer_size"]
	equal(t, ok, true)
	equal(t, int(v.(float64)), outputBufferSize)
	v, ok = decoded["output_buffer_timeout"]
	equal(t, int(v.(float64)), outputBufferTimeout)
	sub(t, conn, topicName, "ch")

	_, err = nsq.Ready(10).WriteTo(conn)
	equal(t, err, nil)

	resp, err := nsq.ReadResponse(conn)
	equal(t, err, nil)
	end := time.Now()

	equal(t, int(end.Sub(start)/time.Millisecond) >= outputBufferTimeout, true)

	frameType, data, err := nsq.UnpackResponse(resp)
	msgOut, _ := decodeMessage(data)
	equal(t, frameType, frameTypeMessage)
	equal(t, msgOut.ID, msg.ID)
}

func TestOutputBufferingValidity(t *testing.T) {
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	opts.Verbose = true
	opts.MaxOutputBufferSize = 512 * 1024
	opts.MaxOutputBufferTimeout = time.Second
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	equal(t, err, nil)
	defer conn.Close()

	identify(t, conn, map[string]interface{}{
		"output_buffer_size":    512 * 1024,
		"output_buffer_timeout": 1000,
	}, frameTypeResponse)
	identify(t, conn, map[string]interface{}{
		"output_buffer_size":    -1,
		"output_buffer_timeout": -1,
	}, frameTypeResponse)
	identify(t, conn, map[string]interface{}{
		"output_buffer_size":    0,
		"output_buffer_timeout": 0,
	}, frameTypeResponse)
	data := identify(t, conn, map[string]interface{}{
		"output_buffer_size":    512*1024 + 1,
		"output_buffer_timeout": 0,
	}, frameTypeError)
	equal(t, string(data), fmt.Sprintf("E_BAD_BODY IDENTIFY output buffer size (%d) is invalid", 512*1024+1))

	conn, err = mustConnectNSQD(tcpAddr)
	equal(t, err, nil)
	defer conn.Close()

	data = identify(t, conn, map[string]interface{}{
		"output_buffer_size":    0,
		"output_buffer_timeout": 1001,
	}, frameTypeError)
	equal(t, string(data), "E_BAD_BODY IDENTIFY output buffer timeout (1001) is invalid")
}

func TestTLS(t *testing.T) {
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	opts.Verbose = true
	opts.TLSCert = "./test/certs/server.pem"
	opts.TLSKey = "./test/certs/server.key"
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	equal(t, err, nil)
	defer conn.Close()

	data := identify(t, conn, map[string]interface{}{
		"tls_v1": true,
	}, frameTypeResponse)
	r := struct {
		TLSv1 bool `json:"tls_v1"`
	}{}
	err = json.Unmarshal(data, &r)
	equal(t, err, nil)
	equal(t, r.TLSv1, true)

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}
	tlsConn := tls.Client(conn, tlsConfig)

	err = tlsConn.Handshake()
	equal(t, err, nil)

	resp, _ := nsq.ReadResponse(tlsConn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	equal(t, frameType, frameTypeResponse)
	equal(t, data, []byte("OK"))
}

func TestTLSRequired(t *testing.T) {
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	opts.Verbose = true
	opts.TLSCert = "./test/certs/server.pem"
	opts.TLSKey = "./test/certs/server.key"
	opts.TLSRequired = TLSRequiredExceptHTTP

	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	topicName := "test_tls_required" + strconv.Itoa(int(time.Now().Unix()))

	conn, err := mustConnectNSQD(tcpAddr)
	equal(t, err, nil)
	defer conn.Close()

	subFail(t, conn, topicName, "ch")

	conn, err = mustConnectNSQD(tcpAddr)
	equal(t, err, nil)
	defer conn.Close()

	data := identify(t, conn, map[string]interface{}{
		"tls_v1": true,
	}, frameTypeResponse)
	r := struct {
		TLSv1 bool `json:"tls_v1"`
	}{}
	err = json.Unmarshal(data, &r)
	equal(t, err, nil)
	equal(t, r.TLSv1, true)

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}
	tlsConn := tls.Client(conn, tlsConfig)

	err = tlsConn.Handshake()
	equal(t, err, nil)

	resp, _ := nsq.ReadResponse(tlsConn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	equal(t, frameType, frameTypeResponse)
	equal(t, data, []byte("OK"))
}

func TestTLSAuthRequire(t *testing.T) {
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	opts.Verbose = true
	opts.TLSCert = "./test/certs/server.pem"
	opts.TLSKey = "./test/certs/server.key"
	opts.TLSClientAuthPolicy = "require"

	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	// No Certs
	conn, err := mustConnectNSQD(tcpAddr)
	equal(t, err, nil)
	defer conn.Close()

	data := identify(t, conn, map[string]interface{}{
		"tls_v1": true,
	}, frameTypeResponse)
	r := struct {
		TLSv1 bool `json:"tls_v1"`
	}{}
	err = json.Unmarshal(data, &r)
	equal(t, err, nil)
	equal(t, r.TLSv1, true)
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}
	tlsConn := tls.Client(conn, tlsConfig)
	err = tlsConn.Handshake()
	nequal(t, err, nil)

	// With Unsigned Cert
	conn, err = mustConnectNSQD(tcpAddr)
	equal(t, err, nil)
	defer conn.Close()

	data = identify(t, conn, map[string]interface{}{
		"tls_v1": true,
	}, frameTypeResponse)
	r = struct {
		TLSv1 bool `json:"tls_v1"`
	}{}
	err = json.Unmarshal(data, &r)
	equal(t, err, nil)
	equal(t, r.TLSv1, true)

	cert, err := tls.LoadX509KeyPair("./test/certs/cert.pem", "./test/certs/key.pem")
	equal(t, err, nil)
	tlsConfig = &tls.Config{
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: true,
	}
	tlsConn = tls.Client(conn, tlsConfig)
	err = tlsConn.Handshake()
	equal(t, err, nil)

	resp, _ := nsq.ReadResponse(tlsConn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	equal(t, frameType, frameTypeResponse)
	equal(t, data, []byte("OK"))

}

func TestTLSAuthRequireVerify(t *testing.T) {
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	opts.Verbose = true
	opts.TLSCert = "./test/certs/server.pem"
	opts.TLSKey = "./test/certs/server.key"
	opts.TLSRootCAFile = "./test/certs/ca.pem"
	opts.TLSClientAuthPolicy = "require-verify"

	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	// with no cert
	conn, err := mustConnectNSQD(tcpAddr)
	equal(t, err, nil)
	defer conn.Close()

	data := identify(t, conn, map[string]interface{}{
		"tls_v1": true,
	}, frameTypeResponse)
	r := struct {
		TLSv1 bool `json:"tls_v1"`
	}{}
	err = json.Unmarshal(data, &r)
	equal(t, err, nil)
	equal(t, r.TLSv1, true)
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}
	tlsConn := tls.Client(conn, tlsConfig)
	err = tlsConn.Handshake()
	nequal(t, err, nil)

	// with invalid cert
	conn, err = mustConnectNSQD(tcpAddr)
	equal(t, err, nil)
	defer conn.Close()

	data = identify(t, conn, map[string]interface{}{
		"tls_v1": true,
	}, frameTypeResponse)
	r = struct {
		TLSv1 bool `json:"tls_v1"`
	}{}
	err = json.Unmarshal(data, &r)
	equal(t, err, nil)
	equal(t, r.TLSv1, true)
	cert, err := tls.LoadX509KeyPair("./test/certs/cert.pem", "./test/certs/key.pem")
	equal(t, err, nil)
	tlsConfig = &tls.Config{
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: true,
	}
	tlsConn = tls.Client(conn, tlsConfig)
	err = tlsConn.Handshake()
	nequal(t, err, nil)

	// with valid cert
	conn, err = mustConnectNSQD(tcpAddr)
	equal(t, err, nil)
	defer conn.Close()

	data = identify(t, conn, map[string]interface{}{
		"tls_v1": true,
	}, frameTypeResponse)
	r = struct {
		TLSv1 bool `json:"tls_v1"`
	}{}
	err = json.Unmarshal(data, &r)
	equal(t, err, nil)
	equal(t, r.TLSv1, true)
	cert, err = tls.LoadX509KeyPair("./test/certs/client.pem", "./test/certs/client.key")
	equal(t, err, nil)
	tlsConfig = &tls.Config{
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: true,
	}
	tlsConn = tls.Client(conn, tlsConfig)
	err = tlsConn.Handshake()
	equal(t, err, nil)

	resp, _ := nsq.ReadResponse(tlsConn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	equal(t, frameType, frameTypeResponse)
	equal(t, data, []byte("OK"))
}

func TestDeflate(t *testing.T) {
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	opts.Verbose = true
	opts.DeflateEnabled = true
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	equal(t, err, nil)
	defer conn.Close()

	data := identify(t, conn, map[string]interface{}{
		"deflate": true,
	}, frameTypeResponse)
	r := struct {
		Deflate bool `json:"deflate"`
	}{}
	err = json.Unmarshal(data, &r)
	equal(t, err, nil)
	equal(t, r.Deflate, true)

	compressConn := flate.NewReader(conn)
	resp, _ := nsq.ReadResponse(compressConn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	equal(t, frameType, frameTypeResponse)
	equal(t, data, []byte("OK"))
}

type readWriter struct {
	io.Reader
	io.Writer
}

func TestSnappy(t *testing.T) {
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	opts.Verbose = true
	opts.SnappyEnabled = true
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	equal(t, err, nil)
	defer conn.Close()

	data := identify(t, conn, map[string]interface{}{
		"snappy": true,
	}, frameTypeResponse)
	r := struct {
		Snappy bool `json:"snappy"`
	}{}
	err = json.Unmarshal(data, &r)
	equal(t, err, nil)
	equal(t, r.Snappy, true)

	compressConn := snappystream.NewReader(conn, snappystream.SkipVerifyChecksum)
	resp, _ := nsq.ReadResponse(compressConn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	equal(t, frameType, frameTypeResponse)
	equal(t, data, []byte("OK"))

	msgBody := make([]byte, 128000)
	w := snappystream.NewWriter(conn)

	rw := readWriter{compressConn, w}

	topicName := "test_snappy" + strconv.Itoa(int(time.Now().Unix()))
	sub(t, rw, topicName, "ch")

	_, err = nsq.Ready(1).WriteTo(rw)
	equal(t, err, nil)

	topic := nsqd.GetTopic(topicName)
	msg := NewMessage(<-nsqd.idChan, msgBody)
	topic.PutMessage(msg)

	resp, _ = nsq.ReadResponse(compressConn)
	frameType, data, _ = nsq.UnpackResponse(resp)
	msgOut, _ := decodeMessage(data)
	equal(t, frameType, frameTypeMessage)
	equal(t, msgOut.ID, msg.ID)
	equal(t, msgOut.Body, msg.Body)
}

func TestTLSDeflate(t *testing.T) {
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	opts.Verbose = true
	opts.DeflateEnabled = true
	opts.TLSCert = "./test/certs/cert.pem"
	opts.TLSKey = "./test/certs/key.pem"
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	equal(t, err, nil)
	defer conn.Close()

	data := identify(t, conn, map[string]interface{}{
		"tls_v1":  true,
		"deflate": true,
	}, frameTypeResponse)
	r := struct {
		TLSv1   bool `json:"tls_v1"`
		Deflate bool `json:"deflate"`
	}{}
	err = json.Unmarshal(data, &r)
	equal(t, err, nil)
	equal(t, r.TLSv1, true)
	equal(t, r.Deflate, true)

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}
	tlsConn := tls.Client(conn, tlsConfig)

	err = tlsConn.Handshake()
	equal(t, err, nil)

	resp, _ := nsq.ReadResponse(tlsConn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	equal(t, frameType, frameTypeResponse)
	equal(t, data, []byte("OK"))

	compressConn := flate.NewReader(tlsConn)

	resp, _ = nsq.ReadResponse(compressConn)
	frameType, data, _ = nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	equal(t, frameType, frameTypeResponse)
	equal(t, data, []byte("OK"))
}

func TestSampling(t *testing.T) {
	rand.Seed(time.Now().UTC().UnixNano())

	num := 10000
	sampleRate := 42
	slack := 5

	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	opts.Verbose = true
	opts.MaxRdyCount = int64(num)
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	equal(t, err, nil)
	defer conn.Close()

	data := identify(t, conn, map[string]interface{}{
		"sample_rate": int32(sampleRate),
	}, frameTypeResponse)
	r := struct {
		SampleRate int32 `json:"sample_rate"`
	}{}
	err = json.Unmarshal(data, &r)
	equal(t, err, nil)
	equal(t, r.SampleRate, int32(sampleRate))

	topicName := "test_sampling" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	for i := 0; i < num; i++ {
		msg := NewMessage(<-nsqd.idChan, []byte("test body"))
		topic.PutMessage(msg)
	}
	channel := topic.GetChannel("ch")

	// let the topic drain into the channel
	time.Sleep(50 * time.Millisecond)

	sub(t, conn, topicName, "ch")
	_, err = nsq.Ready(num).WriteTo(conn)
	equal(t, err, nil)

	go func() {
		for {
			_, err := nsq.ReadResponse(conn)
			if err != nil {
				return
			}
		}
	}()

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

	channel.Lock()
	numInFlight := len(channel.inFlightMessages)
	channel.Unlock()

	equal(t, numInFlight <= int(float64(num)*float64(sampleRate+slack)/100.0), true)
	equal(t, numInFlight >= int(float64(num)*float64(sampleRate-slack)/100.0), true)
}

func TestTLSSnappy(t *testing.T) {
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	opts.Verbose = true
	opts.SnappyEnabled = true
	opts.TLSCert = "./test/certs/cert.pem"
	opts.TLSKey = "./test/certs/key.pem"
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	equal(t, err, nil)
	defer conn.Close()

	data := identify(t, conn, map[string]interface{}{
		"tls_v1": true,
		"snappy": true,
	}, frameTypeResponse)
	r := struct {
		TLSv1  bool `json:"tls_v1"`
		Snappy bool `json:"snappy"`
	}{}
	err = json.Unmarshal(data, &r)
	equal(t, err, nil)
	equal(t, r.TLSv1, true)
	equal(t, r.Snappy, true)

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}
	tlsConn := tls.Client(conn, tlsConfig)

	err = tlsConn.Handshake()
	equal(t, err, nil)

	resp, _ := nsq.ReadResponse(tlsConn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	equal(t, frameType, frameTypeResponse)
	equal(t, data, []byte("OK"))

	compressConn := snappystream.NewReader(tlsConn, snappystream.SkipVerifyChecksum)

	resp, _ = nsq.ReadResponse(compressConn)
	frameType, data, _ = nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	equal(t, frameType, frameTypeResponse)
	equal(t, data, []byte("OK"))
}

func TestClientMsgTimeout(t *testing.T) {
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	opts.Verbose = true
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	topicName := "test_cmsg_timeout" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	msg := NewMessage(<-nsqd.idChan, make([]byte, 100))
	topic.PutMessage(msg)

	// without this the race detector thinks there's a write
	// to msg.Attempts that races with the read in the protocol's messagePump...
	// it does not reflect a realistically possible condition
	topic.PutMessage(NewMessage(<-nsqd.idChan, make([]byte, 100)))

	conn, err := mustConnectNSQD(tcpAddr)
	equal(t, err, nil)
	defer conn.Close()

	identify(t, conn, map[string]interface{}{
		"msg_timeout": 1000,
	}, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	_, err = nsq.Ready(1).WriteTo(conn)
	equal(t, err, nil)

	resp, _ := nsq.ReadResponse(conn)
	_, data, _ := nsq.UnpackResponse(resp)
	msgOut, err := decodeMessage(data)
	equal(t, msgOut.ID, msg.ID)
	equal(t, msgOut.Body, msg.Body)

	_, err = nsq.Ready(0).WriteTo(conn)
	equal(t, err, nil)

	time.Sleep(1100 * time.Millisecond)

	_, err = nsq.Finish(nsq.MessageID(msgOut.ID)).WriteTo(conn)
	equal(t, err, nil)

	resp, _ = nsq.ReadResponse(conn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	equal(t, frameType, frameTypeError)
	equal(t, string(data),
		fmt.Sprintf("E_FIN_FAILED FIN %s failed ID not in flight", msgOut.ID))
}

func TestBadFin(t *testing.T) {
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	opts.Verbose = true
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	equal(t, err, nil)
	defer conn.Close()

	identify(t, conn, map[string]interface{}{}, frameTypeResponse)
	sub(t, conn, "test_fin", "ch")

	fin := &nsq.Command{[]byte("FIN"), [][]byte{[]byte("")}, nil}
	_, err = fin.WriteTo(conn)
	equal(t, err, nil)

	resp, _ := nsq.ReadResponse(conn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	equal(t, frameType, frameTypeError)
	equal(t, string(data), "E_INVALID Invalid Message ID")
}

func TestClientAuth(t *testing.T) {
	authResponse := `{"ttl":1, "authorizations":[]}`
	authSecret := "testsecret"
	authError := "E_UNAUTHORIZED AUTH No authorizations found"
	authSuccess := ""
	runAuthTest(t, authResponse, authSecret, authError, authSuccess)

	// now one that will succeed
	authResponse = `{"ttl":10, "authorizations":
		[{"topic":"test", "channels":[".*"], "permissions":["subscribe","publish"]}]
	}`
	authError = ""
	authSuccess = `{"identity":"","identity_url":"","permission_count":1}`
	runAuthTest(t, authResponse, authSecret, authError, authSuccess)

}

func runAuthTest(t *testing.T, authResponse, authSecret, authError, authSuccess string) {
	var err error
	var expectedAuthIP string
	expectedAuthTLS := "false"

	authd := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Logf("in test auth handler %s", r.RequestURI)
		r.ParseForm()
		equal(t, r.Form.Get("remote_ip"), expectedAuthIP)
		equal(t, r.Form.Get("tls"), expectedAuthTLS)
		equal(t, r.Form.Get("secret"), authSecret)
		fmt.Fprint(w, authResponse)
	}))
	defer authd.Close()

	addr, err := url.Parse(authd.URL)
	equal(t, err, nil)

	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	opts.Verbose = true
	opts.AuthHTTPAddresses = []string{addr.Host}
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	equal(t, err, nil)
	defer conn.Close()

	expectedAuthIP, _, _ = net.SplitHostPort(conn.LocalAddr().String())

	identify(t, conn, map[string]interface{}{
		"tls_v1": false,
	}, nsq.FrameTypeResponse)

	authCmd(t, conn, authSecret, authSuccess)
	if authError != "" {
		readValidate(t, conn, nsq.FrameTypeError, authError)
	} else {
		sub(t, conn, "test", "ch")
	}

}

func BenchmarkProtocolV2Exec(b *testing.B) {
	b.StopTimer()
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(b)
	nsqd := NewNSQD(opts)
	ctx := &context{nsqd}
	p := &protocolV2{ctx}
	c := newClientV2(0, nil, ctx)
	params := [][]byte{[]byte("NOP")}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		p.Exec(c, params)
	}
}

func benchmarkProtocolV2Pub(b *testing.B, size int) {
	var wg sync.WaitGroup
	b.StopTimer()
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(b)
	opts.MemQueueSize = int64(b.N)
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	msg := make([]byte, size)
	batchSize := 200
	batch := make([][]byte, batchSize)
	for i := range batch {
		batch[i] = msg
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
			for i := 0; i < num; i++ {
				cmd, _ := nsq.MultiPublish(topicName, batch)
				_, err := cmd.WriteTo(rw)
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
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(b)
	opts.MemQueueSize = int64(b.N)
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	msg := make([]byte, size)
	topicName := "bench_v2_sub" + strconv.Itoa(b.N) + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	for i := 0; i < b.N; i++ {
		msg := NewMessage(<-nsqd.idChan, msg)
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

	identify(nil, conn, nil, frameTypeResponse)
	sub(nil, conn, topicName, "ch")

	rdyCount := int(math.Min(math.Max(float64(n/workers), 1), 2500))
	rdyChan <- 1
	<-goChan
	nsq.Ready(rdyCount).WriteTo(rw)
	rw.Flush()
	num := n / workers
	numRdy := num/rdyCount - 1
	rdy := rdyCount
	for i := 0; i < num; i++ {
		resp, err := nsq.ReadResponse(rw)
		if err != nil {
			panic(err.Error())
		}
		frameType, data, err := nsq.UnpackResponse(resp)
		if err != nil {
			panic(err.Error())
		}
		if frameType != frameTypeMessage {
			panic("got something else")
		}
		msg, err := decodeMessage(data)
		if err != nil {
			panic(err.Error())
		}
		nsq.Finish(nsq.MessageID(msg.ID)).WriteTo(rw)
		rdy--
		if rdy == 0 && numRdy > 0 {
			nsq.Ready(rdyCount).WriteTo(rw)
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

	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(b)
	opts.MemQueueSize = int64(b.N)
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	msg := make([]byte, 256)
	b.SetBytes(int64(len(msg) * num))

	goChan := make(chan int)
	rdyChan := make(chan int)
	workers := runtime.GOMAXPROCS(0)
	for i := 0; i < num; i++ {
		topicName := "bench_v2" + strconv.Itoa(b.N) + "_" + strconv.Itoa(i) + "_" + strconv.Itoa(int(time.Now().Unix()))
		topic := nsqd.GetTopic(topicName)
		for i := 0; i < b.N; i++ {
			msg := NewMessage(<-nsqd.idChan, msg)
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
