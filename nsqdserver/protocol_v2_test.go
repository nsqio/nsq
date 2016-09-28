package nsqdserver

import (
	"bufio"
	"bytes"
	"compress/flate"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/absolute8511/go-nsq"
	"github.com/absolute8511/nsq/internal/levellogger"
	"github.com/absolute8511/nsq/internal/protocol"
	"github.com/absolute8511/nsq/internal/test"
	nsqdNs "github.com/absolute8511/nsq/nsqd"
	"github.com/mreiferson/go-snappystream"
)

func identify(t *testing.T, conn io.ReadWriter, extra map[string]interface{}, f int32) []byte {
	ci := make(map[string]interface{})
	ci["client_id"] = "test"
	ci["hostname"] = "test"
	ci["feature_negotiation"] = true
	if extra != nil {
		for k, v := range extra {
			ci[k] = v
		}
	}
	cmd, _ := nsq.Identify(ci)
	_, err := cmd.WriteTo(conn)
	test.Equal(t, err, nil)
	resp, err := nsq.ReadResponse(conn)
	test.Equal(t, err, nil)
	frameType, data, err := nsq.UnpackResponse(resp)
	test.Equal(t, err, nil)
	test.Equal(t, frameType, f)
	return data
}

func sub(t *testing.T, conn io.ReadWriter, topicName string, channelName string) {
	_, err := nsq.Subscribe(topicName, channelName).WriteTo(conn)
	test.Equal(t, err, nil)
	readValidate(t, conn, frameTypeResponse, "OK")
}

func subOrdered(t *testing.T, conn io.ReadWriter, topicName string, channelName string) {
	_, err := nsq.SubscribeOrdered(topicName, channelName, "0").WriteTo(conn)
	test.Equal(t, err, nil)
	readValidate(t, conn, frameTypeResponse, "OK")
}

func subTrace(t *testing.T, conn io.ReadWriter, topicName string, channelName string) {
	_, err := nsq.SubscribeAndTrace(topicName, channelName).WriteTo(conn)
	test.Equal(t, err, nil)
	readValidate(t, conn, frameTypeResponse, "OK")
}

func subOffset(t *testing.T, conn io.ReadWriter, topicName string, channelName string, queueOffset int64) {
	var startFrom nsq.ConsumeOffset
	if queueOffset == -1 {
		startFrom.SetToEnd()
	} else {
		startFrom.SetVirtualQueueOffset(queueOffset)
	}
	_, err := nsq.SubscribeAdvanced(topicName, channelName, "0", startFrom).WriteTo(conn)
	test.Equal(t, err, nil)
	readValidate(t, conn, frameTypeResponse, "OK")
}

func authCmd(t *testing.T, conn io.ReadWriter, authSecret string, expectSuccess string) {
	auth, _ := nsq.Auth(authSecret)
	_, err := auth.WriteTo(conn)
	test.Equal(t, err, nil)
	if expectSuccess != "" {
		readValidate(t, conn, nsq.FrameTypeResponse, expectSuccess)
	}
}

func subFail(t *testing.T, conn io.ReadWriter, topicName string, channelName string) {
	_, err := nsq.Subscribe(topicName, channelName).WriteTo(conn)
	test.Equal(t, err, nil)
	resp, err := nsq.ReadResponse(conn)
	frameType, _, err := nsq.UnpackResponse(resp)
	test.Equal(t, frameType, frameTypeError)
}

func readValidate(t *testing.T, conn io.Reader, f int32, d string) []byte {
	resp, err := nsq.ReadResponse(conn)
	test.Equal(t, err, nil)
	frameType, data, err := nsq.UnpackResponse(resp)
	test.Equal(t, err, nil)
	test.Equal(t, frameType, f)
	test.Equal(t, string(data), d)
	return data
}

// test channel/topic names
func TestChannelTopicNames(t *testing.T) {
	test.Equal(t, protocol.IsValidChannelName("test"), true)
	test.Equal(t, protocol.IsValidChannelName("test-with_period."), true)
	test.Equal(t, protocol.IsValidChannelName("test#ephemeral"), true)
	test.Equal(t, protocol.IsValidTopicName("test"), true)
	test.Equal(t, protocol.IsValidTopicName("test-with_period."), true)
	test.Equal(t, protocol.IsValidTopicName("test#ephemeral"), true)
	test.Equal(t, protocol.IsValidTopicName("test:ephemeral"), false)
}

// exercise the basic operations of the V2 protocol
func TestBasicV2(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts.ClientTimeout = 60 * time.Second
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_v2" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	msg := nsqdNs.NewMessage(0, []byte("test body"))
	topic.PutMessage(msg)

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	identify(t, conn, nil, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)

	resp, err := nsq.ReadResponse(conn)
	test.Equal(t, err, nil)
	frameType, data, err := nsq.UnpackResponse(resp)
	msgOut, _ := nsqdNs.DecodeMessage(data)
	test.Equal(t, frameType, frameTypeMessage)
	test.Equal(t, msgOut.ID, msg.ID)
	test.Equal(t, msgOut.Body, msg.Body)
}

func TestMultipleConsumerV2(t *testing.T) {
	msgChan := make(chan *nsqdNs.Message)

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts.ClientTimeout = 60 * time.Second
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_multiple_v2" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	msg := nsqdNs.NewMessage(0, []byte("test body"))
	topic.GetChannel("ch1")
	topic.GetChannel("ch2")
	topic.PutMessage(msg)

	for _, i := range []string{"1", "2"} {
		conn, err := mustConnectNSQD(tcpAddr)
		test.Equal(t, err, nil)
		defer conn.Close()

		identify(t, conn, nil, frameTypeResponse)
		sub(t, conn, topicName, "ch"+i)

		_, err = nsq.Ready(1).WriteTo(conn)
		test.Equal(t, err, nil)

		go func(c net.Conn) {
			resp, _ := nsq.ReadResponse(c)
			_, data, _ := nsq.UnpackResponse(resp)
			recvdMsg, _ := nsqdNs.DecodeMessage(data)
			msgChan <- recvdMsg
		}(conn)
	}

	msgOut := <-msgChan
	test.Equal(t, msgOut.ID, msg.ID)
	test.Equal(t, msgOut.Body, msg.Body)
	msgOut = <-msgChan
	test.Equal(t, msgOut.ID, msg.ID)
	test.Equal(t, msgOut.Body, msg.Body)
}

func TestClientTimeout(t *testing.T) {
	topicName := "test_client_timeout_v2" + strconv.Itoa(int(time.Now().Unix()))

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts.ClientTimeout = 150 * time.Millisecond
	opts.LogLevel = 3
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()
	topic := nsqd.GetTopicIgnPart(topicName)
	topic.GetChannel("ch")

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

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts.ClientTimeout = 200 * time.Millisecond
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	topic := nsqd.GetTopicIgnPart(topicName)
	topic.GetChannel("ch")

	identify(t, conn, nil, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)

	resp, _ := nsq.ReadResponse(conn)
	_, data, _ := nsq.UnpackResponse(resp)
	test.Equal(t, data, []byte("_heartbeat_"))

	time.Sleep(20 * time.Millisecond)

	_, err = nsq.Nop().WriteTo(conn)
	test.Equal(t, err, nil)

	// wait long enough that would have timed out (had we not sent the above cmd)
	time.Sleep(100 * time.Millisecond)

	_, err = nsq.Nop().WriteTo(conn)
	test.Equal(t, err, nil)
}

func TestClientHeartbeatDisableSUB(t *testing.T) {
	topicName := "test_hb_v2" + strconv.Itoa(int(time.Now().Unix()))

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts.ClientTimeout = 200 * time.Millisecond
	opts.LogLevel = 3
	tcpAddr, _, _, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	identify(t, conn, map[string]interface{}{
		"heartbeat_interval": -1,
	}, frameTypeResponse)
	subFail(t, conn, topicName, "ch")
}

func TestClientHeartbeatDisable(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts.ClientTimeout = 100 * time.Millisecond
	tcpAddr, _, _, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	identify(t, conn, map[string]interface{}{
		"heartbeat_interval": -1,
	}, frameTypeResponse)

	time.Sleep(150 * time.Millisecond)

	_, err = nsq.Nop().WriteTo(conn)
	test.Equal(t, err, nil)
}

func TestMaxHeartbeatIntervalValid(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts.MaxHeartbeatInterval = 300 * time.Second
	tcpAddr, _, _, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	hbi := int(opts.MaxHeartbeatInterval / time.Millisecond)
	identify(t, conn, map[string]interface{}{
		"heartbeat_interval": hbi,
	}, frameTypeResponse)
}

func TestMaxHeartbeatIntervalInvalid(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts.MaxHeartbeatInterval = 300 * time.Second
	tcpAddr, _, _, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	hbi := int(opts.MaxHeartbeatInterval/time.Millisecond + 1)
	data := identify(t, conn, map[string]interface{}{
		"heartbeat_interval": hbi,
	}, frameTypeError)
	test.Equal(t, string(data), "E_BAD_BODY IDENTIFY heartbeat interval (300001) is invalid")
}

func TestPausing(t *testing.T) {
	topicName := "test_pause_v2" + strconv.Itoa(int(time.Now().Unix()))

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()
	topic := nsqd.GetTopicIgnPart(topicName)
	topic.GetChannel("ch")

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	identify(t, conn, nil, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)

	msg := nsqdNs.NewMessage(0, []byte("test body"))
	channel := topic.GetChannel("ch")
	topic.PutMessage(msg)

	// receive the first message via the client, finish it, and send new RDY
	resp, _ := nsq.ReadResponse(conn)
	_, data, _ := nsq.UnpackResponse(resp)
	msg, err = nsqdNs.DecodeMessage(data)
	test.Equal(t, msg.Body, []byte("test body"))

	_, err = nsq.Finish(nsq.MessageID(msg.GetFullMsgID())).WriteTo(conn)
	test.Equal(t, err, nil)

	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)

	// sleep to allow the RDY state to take effect
	time.Sleep(50 * time.Millisecond)

	// pause the channel... the client shouldn't receive any more messages
	channel.Pause()

	// sleep to allow the paused state to take effect
	time.Sleep(50 * time.Millisecond)

	msg = nsqdNs.NewMessage(0, []byte("test body2"))
	topic.PutMessage(msg)

	// allow the client to possibly get a message, the test would hang indefinitely
	// if pausing was not working on the internal clientMsgChan read
	time.Sleep(50 * time.Millisecond)
	msg = <-channel.GetClientMsgChan()
	test.Equal(t, msg.Body, []byte("test body2"))

	// unpause the channel... the client should now be pushed a message
	channel.UnPause()

	msg = nsqdNs.NewMessage(0, []byte("test body3"))
	topic.PutMessage(msg)

	resp, _ = nsq.ReadResponse(conn)
	_, data, _ = nsq.UnpackResponse(resp)
	msg, err = nsqdNs.DecodeMessage(data)
	t.Log(msg)
	t.Log(string(msg.Body))
	test.Equal(t, msg.Body, []byte("test body3"))
}

func TestEmptyCommand(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	tcpAddr, _, _, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	_, err = conn.Write([]byte("\n\n"))
	test.Equal(t, err, nil)

	// if we didn't panic here we're good, see issue #120
}

func TestTcpPUBTRACE(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts.LogLevel = 3
	opts.MaxMsgSize = 100
	opts.MaxBodySize = 1000
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	topicName := "test_tcp_pubtrace" + strconv.Itoa(int(time.Now().Unix()))
	nsqd.GetTopicIgnPart(topicName).GetChannel("ch")

	identify(t, conn, nil, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	// PUBTRACE that's valid
	cmd, _ := nsq.PublishTrace(topicName, "0", 123, make([]byte, 5))
	cmd.WriteTo(conn)
	resp, _ := nsq.ReadResponse(conn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeResponse)
	test.Equal(t, len(data), 2+nsqdNs.MsgIDLength+8+4)
	test.Equal(t, data[:2], []byte("OK"))

	// PUBTRACE that's invalid (too big)
	cmd, _ = nsq.PublishTrace(topicName, "0", 123, make([]byte, 105))
	cmd.WriteTo(conn)
	resp, _ = nsq.ReadResponse(conn)
	frameType, data, _ = nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeError)
	// note: the trace body length should include the trace id
	test.Equal(t, string(data), fmt.Sprintf("E_BAD_BODY body too big 113 > 100"))

	conn, err = mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	identify(t, conn, nil, frameTypeResponse)
	test.Equal(t, err, nil)
	sub(t, conn, topicName, "ch")
	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)

	// sleep to allow the RDY state to take effect
	time.Sleep(50 * time.Millisecond)
	for {
		resp, _ := nsq.ReadResponse(conn)
		frameType, data, err := nsq.UnpackResponse(resp)
		test.Nil(t, err)
		test.NotEqual(t, frameTypeError, frameType)
		if frameType == frameTypeResponse {
			t.Logf("got response data: %v", string(data))
			continue
		}
		msgOut, err := nsq.DecodeMessage(data)
		test.Equal(t, 5, len(msgOut.Body))
		traceID := binary.BigEndian.Uint64(msgOut.ID[8:])
		test.Equal(t, uint64(123), traceID)
		_, err = nsq.Finish(msgOut.ID).WriteTo(conn)
		test.Nil(t, err)
		break
	}
	conn.Close()

	conn, err = mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()
	//MPUB
	mpub := make([][]byte, 5)
	traceIDList := make([]uint64, 5)
	for i := range mpub {
		mpub[i] = make([]byte, 1)
		traceIDList[i] = uint64(i)
	}
	cmd, _ = nsq.MultiPublishTrace(topicName, "0", traceIDList, mpub)
	cmd.WriteTo(conn)
	resp, _ = nsq.ReadResponse(conn)
	frameType, data, _ = nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeResponse)
	test.Equal(t, len(data), 2+nsqdNs.MsgIDLength+8+4)
	test.Equal(t, data[:2], []byte("OK"))

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
	test.Equal(t, frameType, frameTypeError)
	test.Equal(t, string(data), fmt.Sprintf("E_BAD_BODY body too big 1148 > 1000"))
}

func TestTcpPub(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts.LogLevel = 3
	opts.MaxMsgSize = 100
	opts.MaxBodySize = 1000
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)

	topicName := "test_tcp_pub" + strconv.Itoa(int(time.Now().Unix()))
	nsqd.GetTopicIgnPart(topicName).GetChannel("ch")

	identify(t, conn, nil, frameTypeResponse)

	// PUB that's valid
	cmd := nsq.Publish(topicName, make([]byte, 5))
	cmd.WriteTo(conn)
	resp, _ := nsq.ReadResponse(conn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeResponse)
	test.Equal(t, len(data), 2)
	test.Equal(t, data[:], []byte("OK"))

	// PUB that's invalid (too big)
	cmd = nsq.Publish(topicName, make([]byte, 105))
	cmd.WriteTo(conn)
	resp, _ = nsq.ReadResponse(conn)
	frameType, data, _ = nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeError)
	// note: the trace body length should include the trace id
	test.Equal(t, string(data), fmt.Sprintf("E_BAD_BODY body too big 105 > 100"))

	conn, err = mustConnectNSQD(tcpAddr)
	identify(t, conn, nil, frameTypeResponse)
	test.Equal(t, err, nil)
	sub(t, conn, topicName, "ch")
	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)

	// sleep to allow the RDY state to take effect
	time.Sleep(50 * time.Millisecond)

	for {
		resp, _ := nsq.ReadResponse(conn)
		frameType, data, err := nsq.UnpackResponse(resp)
		test.Nil(t, err)
		test.NotEqual(t, frameTypeError, frameType)
		if frameType == frameTypeResponse {
			t.Logf("got response data: %v", string(data))
			continue
		}
		msgOut, err := nsq.DecodeMessage(data)
		test.Equal(t, 5, len(msgOut.Body))
		_, err = nsq.Finish(msgOut.ID).WriteTo(conn)
		test.Nil(t, err)
		break
	}
	conn.Close()

	connList := make([]net.Conn, 0)
	for i := 0; i < 100; i++ {
		conn, err := mustConnectNSQD(tcpAddr)
		test.Equal(t, err, nil)
		defer conn.Close()
		identify(t, conn, nil, frameTypeResponse)
		connList = append(connList, conn)
	}
	// test several client pub and check pub loop
	for i := 0; i < len(connList); i++ {
		cmd := nsq.Publish(topicName, make([]byte, 5))
		go func(index int) {
			cmd.WriteTo(connList[index])
		}(i)
	}
	for i := 0; i < len(connList); i++ {
		resp, _ := nsq.ReadResponse(connList[i])
		frameType, data, _ := nsq.UnpackResponse(resp)
		test.Equal(t, frameType, frameTypeResponse)
		test.Equal(t, len(data), 2)
		test.Equal(t, data[:], []byte("OK"))
	}

	conn, err = mustConnectNSQD(tcpAddr)
	identify(t, conn, nil, frameTypeResponse)
	test.Equal(t, err, nil)
	sub(t, conn, topicName, "ch")
	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)

	for i := 0; i < len(connList); i++ {
		resp, _ := nsq.ReadResponse(conn)
		frameType, data, err := nsq.UnpackResponse(resp)
		test.Nil(t, err)
		test.NotEqual(t, frameTypeError, frameType)
		if frameType == frameTypeResponse {
			t.Logf("got response data: %v", string(data))
			continue
		}
		msgOut, err := nsq.DecodeMessage(data)
		test.Equal(t, 5, len(msgOut.Body))
		_, err = nsq.Finish(msgOut.ID).WriteTo(conn)
		test.Nil(t, err)
	}
	conn.Close()
}

func TestSizeLimits(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts.LogLevel = 3
	opts.MaxMsgSize = 100
	opts.MaxBodySize = 1000
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	topicName := "test_limits_v2" + strconv.Itoa(int(time.Now().Unix()))
	nsqd.GetTopicIgnPart(topicName).GetChannel("ch")

	identify(t, conn, nil, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	// PUB that's valid
	// small body
	nsq.Publish(topicName, make([]byte, 1)).WriteTo(conn)
	resp, _ := nsq.ReadResponse(conn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeResponse)
	test.Equal(t, data, []byte("OK"))

	// normal body
	nsq.Publish(topicName, make([]byte, 95)).WriteTo(conn)
	resp, _ = nsq.ReadResponse(conn)
	frameType, data, _ = nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeResponse)
	test.Equal(t, data, []byte("OK"))

	// PUB that's invalid (too big)
	nsq.Publish(topicName, make([]byte, 105)).WriteTo(conn)
	resp, _ = nsq.ReadResponse(conn)
	frameType, data, _ = nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeError)
	test.Equal(t, string(data), fmt.Sprintf("E_BAD_BODY body too big 105 > 100"))

	// need to reconnect
	conn, err = mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	// PUB thats empty
	nsq.Publish(topicName, []byte{}).WriteTo(conn)
	resp, _ = nsq.ReadResponse(conn)
	frameType, data, _ = nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeError)
	test.Equal(t, string(data), fmt.Sprintf("E_BAD_BODY invalid body size 0"))

	// need to reconnect
	conn, err = mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	// MPUB body that's valid
	// mpub small for each body
	mpub := make([][]byte, 5)
	for i := range mpub {
		mpub[i] = make([]byte, 1)
	}
	cmd, _ := nsq.MultiPublish(topicName, mpub)
	cmd.WriteTo(conn)
	resp, _ = nsq.ReadResponse(conn)
	frameType, data, _ = nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeResponse)
	test.Equal(t, data, []byte("OK"))

	mpub = make([][]byte, 5)
	for i := range mpub {
		mpub[i] = make([]byte, 100)
	}
	cmd, _ = nsq.MultiPublish(topicName, mpub)
	cmd.WriteTo(conn)
	resp, _ = nsq.ReadResponse(conn)
	frameType, data, _ = nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeResponse)
	test.Equal(t, data, []byte("OK"))

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
	test.Equal(t, frameType, frameTypeError)
	test.Equal(t, string(data), fmt.Sprintf("E_BAD_BODY body too big 1148 > 1000"))

	// need to reconnect
	conn, err = mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
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
	test.Equal(t, frameType, frameTypeError)
	test.Equal(t, string(data), fmt.Sprintf("E_BAD_MESSAGE MPUB invalid message(5) body size 0"))

	// need to reconnect
	conn, err = mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
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
	test.Equal(t, frameType, frameTypeError)
	test.Equal(t, string(data), fmt.Sprintf("E_BAD_MESSAGE MPUB message too big 101 > 100"))
}

func TestDelayMessage(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	//opts.Logger = &levellogger.GLogger{}
	opts.LogLevel = 2
	opts.SyncEvery = 1
	opts.LogLevel = 3
	opts.MsgTimeout = time.Second * 2
	opts.MaxReqTimeout = time.Second * 100
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	topicName := "test_requeue_delay" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	topic.GetChannel("ch")

	identify(t, conn, nil, frameTypeResponse)
	sub(t, conn, topicName, "ch")
	time.Sleep(opts.QueueScanRefreshInterval)

	msg := nsqdNs.NewMessage(0, []byte("test body"))
	topic.PutMessage(msg)
	topic.ForceFlush()

	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)

	resp, err := nsq.ReadResponse(conn)
	test.Equal(t, err, nil)
	frameType, data, err := nsq.UnpackResponse(resp)
	msgOut, _ := nsqdNs.DecodeMessage(data)
	test.Equal(t, frameType, frameTypeMessage)
	test.Equal(t, msgOut.ID, msg.ID)

	time.Sleep(75 * time.Millisecond)

	// requeue with valid timeout
	delayStart := time.Now()
	_, err = nsq.Requeue(nsq.MessageID(msg.GetFullMsgID()), opts.MsgTimeout).WriteTo(conn)
	test.Equal(t, err, nil)

	resp, err = nsq.ReadResponse(conn)
	test.Equal(t, err, nil)
	frameType, data, err = nsq.UnpackResponse(resp)
	msgOut, _ = nsqdNs.DecodeMessage(data)
	test.Equal(t, frameType, frameTypeMessage)
	test.Equal(t, msgOut.ID, msg.ID)
	delayDone := time.Since(delayStart)
	t.Log(delayDone)
	test.Equal(t, delayDone > opts.MsgTimeout, true)
	test.Equal(t, delayDone < opts.MsgTimeout+time.Duration(time.Millisecond*500*2), true)

	// requeue timeout less than msg timeout
	delayStart = time.Now()
	_, err = nsq.Requeue(nsq.MessageID(msg.GetFullMsgID()), opts.MsgTimeout-time.Second).WriteTo(conn)
	test.Equal(t, err, nil)

	resp, err = nsq.ReadResponse(conn)
	test.Equal(t, err, nil)
	frameType, data, err = nsq.UnpackResponse(resp)
	msgOut, _ = nsqdNs.DecodeMessage(data)
	test.Equal(t, frameType, frameTypeMessage)
	test.Equal(t, msgOut.ID, msg.ID)
	delayDone = time.Since(delayStart)
	t.Log(delayDone)
	test.Equal(t, delayDone > opts.MsgTimeout-time.Second, true)
	test.Equal(t, delayDone < opts.MsgTimeout-time.Second+time.Duration(time.Millisecond*500*2), true)

	// requeue timeout larger than msg timeout
	delayStart = time.Now()
	_, err = nsq.Requeue(nsq.MessageID(msg.GetFullMsgID()), opts.MsgTimeout+time.Second).WriteTo(conn)
	test.Equal(t, err, nil)

	resp, err = nsq.ReadResponse(conn)
	test.Equal(t, err, nil)
	frameType, data, err = nsq.UnpackResponse(resp)
	msgOut, _ = nsqdNs.DecodeMessage(data)
	test.Equal(t, frameType, frameTypeMessage)
	test.Equal(t, msgOut.ID, msg.ID)
	delayDone = time.Since(delayStart)
	t.Log(delayDone)
	test.Equal(t, delayDone > opts.MsgTimeout+time.Second, true)
	test.Equal(t, delayDone < opts.MsgTimeout+time.Second+time.Duration(time.Millisecond*500*2), true)

	time.Sleep(500 * time.Millisecond)

	_, err = nsq.Finish(nsq.MessageID(msg.GetFullMsgID())).WriteTo(conn)
	test.Equal(t, err, nil)

	time.Sleep(25 * time.Millisecond)

	// requeue duration out of range
	msg = nsqdNs.NewMessage(0, []byte("test body 2"))
	_, _, _, _, err = topic.PutMessage(msg)
	test.Equal(t, err, nil)
	topic.ForceFlush()

	resp, err = nsq.ReadResponse(conn)
	test.Equal(t, err, nil)
	frameType, data, err = nsq.UnpackResponse(resp)
	msgOut, _ = nsqdNs.DecodeMessage(data)
	test.Equal(t, frameType, frameTypeMessage)
	test.Equal(t, msgOut.ID, msg.ID)

	time.Sleep(75 * time.Millisecond)
	_, err = nsq.Requeue(nsq.MessageID(msg.GetFullMsgID()), opts.MaxReqTimeout+time.Second*2).WriteTo(conn)
	test.Equal(t, err, nil)
	resp, err = nsq.ReadResponse(conn)
	test.Equal(t, err, nil)
	frameType, data, err = nsq.UnpackResponse(resp)
	test.Equal(t, frameType, frameTypeError)
	test.Equal(t, string(data), fmt.Sprintf("E_INVALID REQ timeout %v out of range 0-%v", opts.MaxReqTimeout+time.Second*2, opts.MaxReqTimeout))
}

func TestTouch(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts.SyncEvery = 1
	opts.LogLevel = 3
	opts.MsgTimeout = 150 * time.Millisecond
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_touch" + strconv.Itoa(int(time.Now().Unix()))

	topic := nsqd.GetTopicIgnPart(topicName)
	ch := topic.GetChannel("ch")

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	identify(t, conn, nil, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	msg := nsqdNs.NewMessage(0, []byte("test body"))
	topic.PutMessage(msg)

	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)

	resp, err := nsq.ReadResponse(conn)
	test.Equal(t, err, nil)
	frameType, data, err := nsq.UnpackResponse(resp)
	msgOut, _ := nsqdNs.DecodeMessage(data)
	test.Equal(t, frameType, frameTypeMessage)
	test.Equal(t, msgOut.ID, msg.ID)

	time.Sleep(75 * time.Millisecond)

	_, err = nsq.Touch(nsq.MessageID(msg.GetFullMsgID())).WriteTo(conn)
	test.Equal(t, err, nil)

	time.Sleep(75 * time.Millisecond)

	_, err = nsq.Finish(nsq.MessageID(msg.GetFullMsgID())).WriteTo(conn)
	test.Equal(t, err, nil)

	stats := nsqdNs.NewChannelStats(ch, nil)
	test.Equal(t, stats.TimeoutCount, uint64(0))
}

func TestSubOrdered(t *testing.T) {
	topicName := "test_sub_ordered" + strconv.Itoa(int(time.Now().Unix()))

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts.LogLevel = 3
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	topic := nsqd.GetTopicIgnPart(topicName)
	topic.GetChannel("ordered_ch")

	identify(t, conn, nil, frameTypeResponse)
	subOrdered(t, conn, topicName, "ordered_ch")
	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)

	msg := nsqdNs.NewMessage(0, make([]byte, 100))
	topic.PutMessage(msg)
	for i := 0; i < 100; i++ {
		topic.PutMessage(nsqdNs.NewMessage(0, make([]byte, 100)))
	}

	expectedOffset := int64(0)
	var lastMsgID nsq.NewMessageID
	for i := 0; i < 50; i++ {
		resp, _ := nsq.ReadResponse(conn)
		frameType, data, err := nsq.UnpackResponse(resp)
		test.Nil(t, err)
		test.NotEqual(t, frameTypeError, frameType)
		if frameType == frameTypeResponse {
			t.Logf("got response data: %v", string(data))
			continue
		}
		msgOut, err := nsq.DecodeMessage(data)

		msgOut.Offset = uint64(binary.BigEndian.Uint64(msgOut.Body[:8]))
		msgOut.RawSize = uint32(binary.BigEndian.Uint32(msgOut.Body[8:12]))
		msgOut.Body = msgOut.Body[12:]
		test.Equal(t, msgOut.Body, msg.Body)
		if expectedOffset != int64(0) {
			if nsq.GetNewMessageID(msgOut.ID[:]) != lastMsgID {
				test.Equal(t, expectedOffset, int64(msgOut.Offset))
			} else {
				t.Logf("got dump message id: %v", lastMsgID)
			}
		}
		expectedOffset = int64(msgOut.Offset) + int64(msgOut.RawSize)
		lastMsgID = nsq.GetNewMessageID(msgOut.ID[:])
		_, err = nsq.Finish(msgOut.ID).WriteTo(conn)
		test.Nil(t, err)
	}
	conn.Close()
	time.Sleep(time.Second)
	// reconnect and try consume the message
	conn, err = mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	identify(t, conn, nil, frameTypeResponse)
	subOrdered(t, conn, topicName, "ordered_ch")
	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)
	defer conn.Close()
	for i := 0; i < 50; i++ {
		resp, _ := nsq.ReadResponse(conn)
		frameType, data, err := nsq.UnpackResponse(resp)
		test.Nil(t, err)
		test.NotEqual(t, frameTypeError, frameType)
		if frameType == frameTypeResponse {
			t.Logf("got response data: %v", string(data))
			continue
		}

		msgOut, err := nsq.DecodeMessage(data)
		msgOut.Offset = uint64(binary.BigEndian.Uint64(msgOut.Body[:8]))
		msgOut.RawSize = uint32(binary.BigEndian.Uint32(msgOut.Body[8:12]))
		msgOut.Body = msgOut.Body[12:]
		test.Equal(t, msgOut.Body, msg.Body)
		if expectedOffset != int64(0) {
			if nsq.GetNewMessageID(msgOut.ID[:]) != lastMsgID {
				test.Equal(t, expectedOffset, int64(msgOut.Offset))
			} else {
				t.Logf("got dump message id: %v", lastMsgID)
			}
		}
		expectedOffset = int64(msgOut.Offset) + int64(msgOut.RawSize)
		lastMsgID = nsq.GetNewMessageID(msgOut.ID[:])
		_, err = nsq.Finish(msgOut.ID).WriteTo(conn)
		test.Nil(t, err)
	}
}

func TestMaxRdyCount(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts.LogLevel = 3
	opts.MaxRdyCount = 50
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_max_rdy_count" + strconv.Itoa(int(time.Now().Unix()))

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	topic := nsqd.GetTopicIgnPart(topicName)
	topic.GetChannel("ch")
	msg := nsqdNs.NewMessage(0, []byte("test body"))
	topic.PutMessage(msg)

	data := identify(t, conn, nil, frameTypeResponse)
	r := struct {
		MaxRdyCount int64 `json:"max_rdy_count"`
	}{}
	err = json.Unmarshal(data, &r)
	test.Equal(t, err, nil)
	test.Equal(t, r.MaxRdyCount, int64(50))
	sub(t, conn, topicName, "ch")

	_, err = nsq.Ready(int(opts.MaxRdyCount)).WriteTo(conn)
	test.Equal(t, err, nil)

	resp, err := nsq.ReadResponse(conn)
	test.Equal(t, err, nil)
	frameType, data, err := nsq.UnpackResponse(resp)
	msgOut, _ := nsqdNs.DecodeMessage(data)
	test.Equal(t, frameType, frameTypeMessage)
	test.Equal(t, msgOut.ID, msg.ID)

	_, err = nsq.Ready(int(opts.MaxRdyCount) + 1).WriteTo(conn)
	test.Equal(t, err, nil)

	resp, err = nsq.ReadResponse(conn)
	test.Equal(t, err, nil)
	frameType, data, err = nsq.UnpackResponse(resp)
	test.Equal(t, frameType, int32(1))
	test.Equal(t, string(data), "E_INVALID RDY count 51 out of range 0-50")
}

func TestFatalError(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	tcpAddr, _, _, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	_, err = conn.Write([]byte("ASDF\n"))
	test.Equal(t, err, nil)

	resp, err := nsq.ReadResponse(conn)
	test.Equal(t, err, nil)
	frameType, data, err := nsq.UnpackResponse(resp)
	test.Equal(t, frameType, int32(1))
	test.Equal(t, strings.HasPrefix(string(data), "E_INVALID "), true)

	_, err = nsq.ReadResponse(conn)
	test.NotNil(t, err)
}

func TestOutputBuffering(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts.LogLevel = 3
	opts.MaxOutputBufferSize = 512 * 1024
	opts.MaxOutputBufferTimeout = time.Second
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_output_buffering" + strconv.Itoa(int(time.Now().Unix()))

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	outputBufferSize := 256 * 1024
	outputBufferTimeout := 500

	topic := nsqd.GetTopicIgnPart(topicName)
	topic.GetChannel("ch")
	msg := nsqdNs.NewMessage(0, make([]byte, outputBufferSize-1024))
	topic.PutMessage(msg)

	start := time.Now()
	data := identify(t, conn, map[string]interface{}{
		"output_buffer_size":    outputBufferSize,
		"output_buffer_timeout": outputBufferTimeout,
	}, frameTypeResponse)
	var decoded map[string]interface{}
	json.Unmarshal(data, &decoded)
	v, ok := decoded["output_buffer_size"]
	test.Equal(t, ok, true)
	test.Equal(t, int(v.(float64)), outputBufferSize)
	v, ok = decoded["output_buffer_timeout"]
	test.Equal(t, int(v.(float64)), outputBufferTimeout)
	sub(t, conn, topicName, "ch")

	_, err = nsq.Ready(10).WriteTo(conn)
	test.Equal(t, err, nil)

	resp, err := nsq.ReadResponse(conn)
	test.Equal(t, err, nil)
	end := time.Now()

	test.Equal(t, int(end.Sub(start)/time.Millisecond) >= outputBufferTimeout, true)

	frameType, data, err := nsq.UnpackResponse(resp)
	msgOut, _ := nsqdNs.DecodeMessage(data)
	test.Equal(t, frameType, frameTypeMessage)
	test.Equal(t, msgOut.ID, msg.ID)
}

func TestOutputBufferingValidity(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts.LogLevel = 3
	opts.MaxOutputBufferSize = 512 * 1024
	opts.MaxOutputBufferTimeout = time.Second
	tcpAddr, _, _, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
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
	test.Equal(t, string(data), fmt.Sprintf("E_BAD_BODY IDENTIFY output buffer size (%d) is invalid", 512*1024+1))

	conn, err = mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	data = identify(t, conn, map[string]interface{}{
		"output_buffer_size":    0,
		"output_buffer_timeout": 1001,
	}, frameTypeError)
	test.Equal(t, string(data), "E_BAD_BODY IDENTIFY output buffer timeout (1001) is invalid")
}

func TestTLS(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts.LogLevel = 3
	opts.TLSCert = "./test/certs/server.pem"
	opts.TLSKey = "./test/certs/server.key"
	tcpAddr, _, _, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	data := identify(t, conn, map[string]interface{}{
		"tls_v1": true,
	}, frameTypeResponse)
	r := struct {
		TLSv1 bool `json:"tls_v1"`
	}{}
	err = json.Unmarshal(data, &r)
	test.Equal(t, err, nil)
	test.Equal(t, r.TLSv1, true)

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}
	tlsConn := tls.Client(conn, tlsConfig)

	err = tlsConn.Handshake()
	test.Equal(t, err, nil)

	resp, _ := nsq.ReadResponse(tlsConn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeResponse)
	test.Equal(t, data, []byte("OK"))
}

func TestTLSRequired(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts.LogLevel = 3
	opts.TLSCert = "./test/certs/server.pem"
	opts.TLSKey = "./test/certs/server.key"
	opts.TLSRequired = TLSRequiredExceptHTTP

	tcpAddr, _, _, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_tls_required" + strconv.Itoa(int(time.Now().Unix()))

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	subFail(t, conn, topicName, "ch")

	conn, err = mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	data := identify(t, conn, map[string]interface{}{
		"tls_v1": true,
	}, frameTypeResponse)
	r := struct {
		TLSv1 bool `json:"tls_v1"`
	}{}
	err = json.Unmarshal(data, &r)
	test.Equal(t, err, nil)
	test.Equal(t, r.TLSv1, true)

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}
	tlsConn := tls.Client(conn, tlsConfig)

	err = tlsConn.Handshake()
	test.Equal(t, err, nil)

	resp, _ := nsq.ReadResponse(tlsConn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeResponse)
	test.Equal(t, data, []byte("OK"))
}

func TestTLSAuthRequire(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts.LogLevel = 3
	opts.TLSCert = "./test/certs/server.pem"
	opts.TLSKey = "./test/certs/server.key"
	opts.TLSClientAuthPolicy = "require"

	tcpAddr, _, _, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	// No Certs
	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	data := identify(t, conn, map[string]interface{}{
		"tls_v1": true,
	}, frameTypeResponse)
	r := struct {
		TLSv1 bool `json:"tls_v1"`
	}{}
	err = json.Unmarshal(data, &r)
	test.Equal(t, err, nil)
	test.Equal(t, r.TLSv1, true)
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}
	tlsConn := tls.Client(conn, tlsConfig)
	err = tlsConn.Handshake()
	test.NotNil(t, err)

	// With Unsigned Cert
	conn, err = mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	data = identify(t, conn, map[string]interface{}{
		"tls_v1": true,
	}, frameTypeResponse)
	r = struct {
		TLSv1 bool `json:"tls_v1"`
	}{}
	err = json.Unmarshal(data, &r)
	test.Equal(t, err, nil)
	test.Equal(t, r.TLSv1, true)

	cert, err := tls.LoadX509KeyPair("./test/certs/cert.pem", "./test/certs/key.pem")
	test.Equal(t, err, nil)
	tlsConfig = &tls.Config{
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: true,
	}
	tlsConn = tls.Client(conn, tlsConfig)
	err = tlsConn.Handshake()
	test.Equal(t, err, nil)

	resp, _ := nsq.ReadResponse(tlsConn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeResponse)
	test.Equal(t, data, []byte("OK"))

}

func TestTLSAuthRequireVerify(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts.LogLevel = 3
	opts.TLSCert = "./test/certs/server.pem"
	opts.TLSKey = "./test/certs/server.key"
	opts.TLSRootCAFile = "./test/certs/ca.pem"
	opts.TLSClientAuthPolicy = "require-verify"

	tcpAddr, _, _, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	// with no cert
	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	data := identify(t, conn, map[string]interface{}{
		"tls_v1": true,
	}, frameTypeResponse)
	r := struct {
		TLSv1 bool `json:"tls_v1"`
	}{}
	err = json.Unmarshal(data, &r)
	test.Equal(t, err, nil)
	test.Equal(t, r.TLSv1, true)
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}
	tlsConn := tls.Client(conn, tlsConfig)
	err = tlsConn.Handshake()
	test.NotNil(t, err)

	// with invalid cert
	conn, err = mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	data = identify(t, conn, map[string]interface{}{
		"tls_v1": true,
	}, frameTypeResponse)
	r = struct {
		TLSv1 bool `json:"tls_v1"`
	}{}
	err = json.Unmarshal(data, &r)
	test.Equal(t, err, nil)
	test.Equal(t, r.TLSv1, true)
	cert, err := tls.LoadX509KeyPair("./test/certs/cert.pem", "./test/certs/key.pem")
	test.Equal(t, err, nil)
	tlsConfig = &tls.Config{
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: true,
	}
	tlsConn = tls.Client(conn, tlsConfig)
	err = tlsConn.Handshake()
	test.NotNil(t, err)

	// with valid cert
	conn, err = mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	data = identify(t, conn, map[string]interface{}{
		"tls_v1": true,
	}, frameTypeResponse)
	r = struct {
		TLSv1 bool `json:"tls_v1"`
	}{}
	err = json.Unmarshal(data, &r)
	test.Equal(t, err, nil)
	test.Equal(t, r.TLSv1, true)
	cert, err = tls.LoadX509KeyPair("./test/certs/client.pem", "./test/certs/client.key")
	test.Equal(t, err, nil)
	tlsConfig = &tls.Config{
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: true,
	}
	tlsConn = tls.Client(conn, tlsConfig)
	err = tlsConn.Handshake()
	test.Equal(t, err, nil)

	resp, _ := nsq.ReadResponse(tlsConn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeResponse)
	test.Equal(t, data, []byte("OK"))
}

func TestDeflate(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts.LogLevel = 3
	opts.DeflateEnabled = true
	tcpAddr, _, _, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	data := identify(t, conn, map[string]interface{}{
		"deflate": true,
	}, frameTypeResponse)
	r := struct {
		Deflate bool `json:"deflate"`
	}{}
	err = json.Unmarshal(data, &r)
	test.Equal(t, err, nil)
	test.Equal(t, r.Deflate, true)

	compressConn := flate.NewReader(conn)
	resp, _ := nsq.ReadResponse(compressConn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeResponse)
	test.Equal(t, data, []byte("OK"))
}

type readWriter struct {
	io.Reader
	io.Writer
}

func TestSnappy(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts.LogLevel = 3
	opts.SnappyEnabled = true
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	data := identify(t, conn, map[string]interface{}{
		"snappy": true,
	}, frameTypeResponse)
	r := struct {
		Snappy bool `json:"snappy"`
	}{}
	err = json.Unmarshal(data, &r)
	test.Equal(t, err, nil)
	test.Equal(t, r.Snappy, true)

	compressConn := snappystream.NewReader(conn, snappystream.SkipVerifyChecksum)
	resp, _ := nsq.ReadResponse(compressConn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeResponse)
	test.Equal(t, data, []byte("OK"))

	msgBody := make([]byte, 128000)
	w := snappystream.NewWriter(conn)

	rw := readWriter{compressConn, w}

	topicName := "test_snappy" + strconv.Itoa(int(time.Now().Unix()))
	nsqd.GetTopicIgnPart(topicName).GetChannel("ch")
	sub(t, rw, topicName, "ch")

	_, err = nsq.Ready(1).WriteTo(rw)
	test.Equal(t, err, nil)

	topic := nsqd.GetTopicIgnPart(topicName)
	msg := nsqdNs.NewMessage(0, msgBody)
	topic.PutMessage(msg)

	resp, _ = nsq.ReadResponse(compressConn)
	frameType, data, _ = nsq.UnpackResponse(resp)
	msgOut, _ := nsqdNs.DecodeMessage(data)
	test.Equal(t, frameType, frameTypeMessage)
	test.Equal(t, msgOut.ID, msg.ID)
	test.Equal(t, msgOut.Body, msg.Body)
}

func TestTLSDeflate(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts.LogLevel = 3
	opts.DeflateEnabled = true
	opts.TLSCert = "./test/certs/cert.pem"
	opts.TLSKey = "./test/certs/key.pem"
	tcpAddr, _, _, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
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
	test.Equal(t, err, nil)
	test.Equal(t, r.TLSv1, true)
	test.Equal(t, r.Deflate, true)

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}
	tlsConn := tls.Client(conn, tlsConfig)

	err = tlsConn.Handshake()
	test.Equal(t, err, nil)

	resp, _ := nsq.ReadResponse(tlsConn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeResponse)
	test.Equal(t, data, []byte("OK"))

	compressConn := flate.NewReader(tlsConn)

	resp, _ = nsq.ReadResponse(compressConn)
	frameType, data, _ = nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeResponse)
	test.Equal(t, data, []byte("OK"))
}

func TestSampling(t *testing.T) {
	rand.Seed(time.Now().UTC().UnixNano())

	num := 10000
	sampleRate := 42
	slack := 5

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts.SyncEvery = 1
	opts.LogLevel = 2
	opts.MaxRdyCount = int64(num)
	opts.MaxConfirmWin = int64(num * 100)
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	data := identify(t, conn, map[string]interface{}{
		"sample_rate": int32(sampleRate),
	}, frameTypeResponse)
	r := struct {
		SampleRate int32 `json:"sample_rate"`
	}{}
	err = json.Unmarshal(data, &r)
	test.Equal(t, err, nil)
	test.Equal(t, r.SampleRate, int32(sampleRate))

	topicName := "test_sampling" + strconv.Itoa(int(time.Now().Unix()))
	testBody := []byte("test body")
	topic := nsqd.GetTopicIgnPart(topicName)
	channel := topic.GetChannel("ch")

	for i := 0; i < num; i++ {
		msg := nsqdNs.NewMessage(0, testBody)
		topic.PutMessage(msg)
	}

	// let the topic drain into the channel
	time.Sleep(50 * time.Millisecond)

	sub(t, conn, topicName, "ch")
	_, err = nsq.Ready(num).WriteTo(conn)
	test.Equal(t, err, nil)

	go func() {
		for {
			_, err := nsq.ReadResponse(conn)
			if err != nil {
				return
			}
			//frameType, data, _ := nsq.UnpackResponse(resp)
			//if frameType == frameTypeResponse {
			//	if !bytes.Equal(data, heartbeatBytes) {
			//		t.Fatalf("got response not heartbeat:" + string(data))
			//	}
			//	nsq.Nop().WriteTo(conn)
			//	continue
			//}
			//if frameType != frameTypeMessage {
			//	t.Fatalf("got something else")
			//}
			//msgOut, _ := nsqdNs.DecodeMessage(data)
			//nsq.Finish(nsq.MessageID(msgOut.GetFullMsgID())).WriteTo(conn)
		}
	}()

	time.Sleep(time.Second * 15)
	test.Equal(t, channel.GetChannelEnd().Offset(),
		nsqdNs.BackendOffset(num*(4+len(testBody)+10+16)))
	//doneChan := make(chan int)
	//go func() {
	//	for {
	//		if channel.GetConfirmedOffset() ==
	//			channel.GetChannelEnd() {
	//			close(doneChan)
	//			return
	//		}
	//		time.Sleep(5 * time.Millisecond)
	//	}
	//}()
	//<-doneChan

	numInFlight := channel.GetInflightNum()

	test.Equal(t, numInFlight <= int(float64(num)*float64(sampleRate+slack)/100.0), true)
	test.Equal(t, numInFlight >= int(float64(num)*float64(sampleRate-slack)/100.0), true)
}

func TestTLSSnappy(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts.LogLevel = 3
	opts.SnappyEnabled = true
	opts.TLSCert = "./test/certs/cert.pem"
	opts.TLSKey = "./test/certs/key.pem"
	tcpAddr, _, _, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
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
	test.Equal(t, err, nil)
	test.Equal(t, r.TLSv1, true)
	test.Equal(t, r.Snappy, true)

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}
	tlsConn := tls.Client(conn, tlsConfig)

	err = tlsConn.Handshake()
	test.Equal(t, err, nil)

	resp, _ := nsq.ReadResponse(tlsConn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeResponse)
	test.Equal(t, data, []byte("OK"))

	compressConn := snappystream.NewReader(tlsConn, snappystream.SkipVerifyChecksum)

	resp, _ = nsq.ReadResponse(compressConn)
	frameType, data, _ = nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeResponse)
	test.Equal(t, data, []byte("OK"))
}

func TestClientMsgTimeout(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts.LogLevel = 3
	opts.QueueScanRefreshInterval = 100 * time.Millisecond
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_cmsg_timeout" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	msg := nsqdNs.NewMessage(0, make([]byte, 100))
	topic.PutMessage(msg)

	// without this the race detector thinks there's a write
	// to msg.Attempts that races with the read in the protocol's messagePump...
	// it does not reflect a realistically possible condition
	topic.PutMessage(nsqdNs.NewMessage(0, make([]byte, 100)))

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	identify(t, conn, map[string]interface{}{
		"msg_timeout": 1000,
	}, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)

	resp, _ := nsq.ReadResponse(conn)
	_, data, _ := nsq.UnpackResponse(resp)
	msgOut, err := nsqdNs.DecodeMessage(data)
	test.Equal(t, msgOut.ID, msg.ID)
	test.Equal(t, msgOut.Body, msg.Body)

	_, err = nsq.Ready(0).WriteTo(conn)
	test.Equal(t, err, nil)

	time.Sleep(1100*time.Millisecond + opts.QueueScanInterval)

	_, err = nsq.Finish(nsq.MessageID(msgOut.GetFullMsgID())).WriteTo(conn)
	test.Equal(t, err, nil)

	resp, _ = nsq.ReadResponse(conn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	test.Equal(t, frameType, frameTypeError)
	test.Equal(t, string(data),
		fmt.Sprintf("E_FIN_FAILED FIN %v failed Message ID not in flight", msgOut.GetFullMsgID()))
}

// fail to finish some messages and wait server requeue.
func TestTimeoutFin(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	//opts.Logger = &levellogger.GLogger{}
	opts.LogLevel = 3
	opts.LogLevel = 3
	opts.QueueScanRefreshInterval = 100 * time.Millisecond
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_cmsg_timeout_requeue" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	tmpCh := topic.GetChannel("ch")
	tmpCh.EnableTrace = 1
	msg := nsqdNs.NewMessage(0, make([]byte, 100))
	topic.PutMessage(msg)

	// without this the race detector thinks there's a write
	// to msg.Attempts that races with the read in the protocol's messagePump...
	// it does not reflect a realistically possible condition
	topic.PutMessage(nsqdNs.NewMessage(0, make([]byte, 100)))

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	identify(t, conn, map[string]interface{}{
		"msg_timeout": 1000,
	}, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	attemp := 1
	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)

	resp, _ := nsq.ReadResponse(conn)
	_, data, _ := nsq.UnpackResponse(resp)
	msgOut, err := nsqdNs.DecodeMessage(data)
	test.Equal(t, msgOut.ID, msg.ID)
	test.Equal(t, msgOut.Attempts, uint16(attemp))
	test.Equal(t, msgOut.Body, msg.Body)

	attemp++
	for i := 0; i < 6; i++ {
		//time.Sleep(1100*time.Millisecond + opts.QueueScanInterval)

		// wait timeout and requeue
		resp, _ = nsq.ReadResponse(conn)
		_, data, _ = nsq.UnpackResponse(resp)
		msgOut, err = nsqdNs.DecodeMessage(data)
		if msgOut.ID == msg.ID {
			t.Log(msgOut)
			test.Equal(t, msgOut.Attempts, uint16(attemp))
			test.Equal(t, msgOut.Body, msg.Body)
			attemp++
			if i > 3 {
				_, err = nsq.Finish(nsq.MessageID(msg.GetFullMsgID())).WriteTo(conn)
				test.Nil(t, err)
			}
		} else {
			test.Equal(t, msgOut.ID, msg.ID+1)
		}
	}

	time.Sleep(10 * time.Millisecond)

	for i := 0; i < 3; i++ {
		resp, _ = nsq.ReadResponse(conn)
		_, data, _ = nsq.UnpackResponse(resp)
		msgOut, err = nsqdNs.DecodeMessage(data)
		t.Log(msgOut)
		test.NotEqual(t, msgOut.ID, msg.ID)
	}
}

func TestSetChannelOffset(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts.LogLevel = 3
	opts.QueueScanRefreshInterval = 100 * time.Millisecond
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_channel_setoffset" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	tmpCh := topic.GetChannel("ch")
	tmpCh.EnableTrace = 1
	msg := nsqdNs.NewMessage(0, make([]byte, 100))
	topic.PutMessage(msg)
	for i := 0; i < 100; i++ {
		topic.PutMessage(nsqdNs.NewMessage(0, make([]byte, 100)))
	}
	topic.ForceFlush()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)

	identify(t, conn, map[string]interface{}{
		"msg_timeout": 1000,
	}, frameTypeResponse)
	subTrace(t, conn, topicName, "ch")

	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)

	resp, _ := nsq.ReadResponse(conn)
	_, data, _ := nsq.UnpackResponse(resp)
	msgOut, err := nsq.DecodeMessage(data)
	msgOut.Offset = uint64(binary.BigEndian.Uint64(msgOut.Body[:8]))
	msgOut.RawSize = uint32(binary.BigEndian.Uint32(msgOut.Body[8:12]))
	msgOut.Body = msgOut.Body[12:]
	test.Equal(t, uint64(nsq.GetNewMessageID(msgOut.ID[:])), uint64(msg.ID))
	test.Equal(t, msgOut.Body, msg.Body)

	conn.Close()

	msgRawSize := msgOut.RawSize
	conn, err = mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	identify(t, conn, map[string]interface{}{
		"msg_timeout": 1000,
	}, frameTypeResponse)

	subOffset(t, conn, topicName, "ch", int64(-1))
	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)
	for i := 0; i < 100; i++ {
		topic.PutMessage(nsqdNs.NewMessage(0, make([]byte, 100)))
	}

	resp, _ = nsq.ReadResponse(conn)
	_, data, _ = nsq.UnpackResponse(resp)
	msgOut, err = nsq.DecodeMessage(data)
	msgOut.Offset = uint64(binary.BigEndian.Uint64(msgOut.Body[:8]))
	msgOut.RawSize = uint32(binary.BigEndian.Uint32(msgOut.Body[8:12]))
	msgOut.Body = msgOut.Body[12:]
	test.Equal(t, int64(msgRawSize*101), int64(msgOut.Offset))
	test.Equal(t, uint64(nsq.GetNewMessageID(msgOut.ID[:])), uint64(msg.ID+101))
	test.Equal(t, msgOut.Body, msg.Body)

	conn.Close()
}

func TestBadFin(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts.SyncEvery = 1
	opts.LogLevel = 3
	opts.Verbose = true
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()
	nsqd.GetTopicIgnPart("test_fin").GetChannel("ch")

	identify(t, conn, map[string]interface{}{}, frameTypeResponse)
	sub(t, conn, "test_fin", "ch")
	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)

	var emptyID nsq.MessageID
	fin := nsq.Finish(emptyID)
	fin.Params[0] = emptyID[:]
	_, err = fin.WriteTo(conn)
	test.Equal(t, err, nil)

	resp, _ := nsq.ReadResponse(conn)
	t.Logf("%v", resp)
	frameType, data, _ := nsq.UnpackResponse(resp)
	if string(data) == string(heartbeatBytes) {
		//resp, _ = nsq.ReadResponse(conn)
		//frameType, data, _ = nsq.UnpackResponse(resp)
	}
	test.Equal(t, string(data), "E_INVALID Invalid Message ID")
	test.Equal(t, frameType, frameTypeError)
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
		test.Equal(t, r.Form.Get("remote_ip"), expectedAuthIP)
		test.Equal(t, r.Form.Get("tls"), expectedAuthTLS)
		test.Equal(t, r.Form.Get("secret"), authSecret)
		fmt.Fprint(w, authResponse)
	}))
	defer authd.Close()

	addr, err := url.Parse(authd.URL)
	test.Equal(t, err, nil)

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts.LogLevel = 3
	opts.AuthHTTPAddresses = []string{addr.Host}
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	expectedAuthIP, _, _ = net.SplitHostPort(conn.LocalAddr().String())

	identify(t, conn, map[string]interface{}{
		"tls_v1": false,
	}, nsq.FrameTypeResponse)

	authCmd(t, conn, authSecret, authSuccess)
	if authError != "" {
		readValidate(t, conn, nsq.FrameTypeError, authError)
	} else {
		nsqd.GetTopicIgnPart("test").GetChannel("ch")
		sub(t, conn, "test", "ch")
	}

}

func TestIOLoopReturnsClientErrWhenSendFails(t *testing.T) {
	fakeConn := test.NewFakeNetConn()
	fakeConn.WriteFunc = func(b []byte) (int, error) {
		return 0, errors.New("write error")
	}

	testIOLoopReturnsClientErr(t, fakeConn)
}

func TestIOLoopReturnsClientErrWhenSendSucceeds(t *testing.T) {
	fakeConn := test.NewFakeNetConn()
	fakeConn.WriteFunc = func(b []byte) (int, error) {
		return len(b), nil
	}

	testIOLoopReturnsClientErr(t, fakeConn)
}

func testIOLoopReturnsClientErr(t *testing.T, fakeConn test.FakeNetConn) {
	fakeConn.ReadFunc = func(b []byte) (int, error) {
		return copy(b, []byte("INVALID_COMMAND\n")), nil
	}

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts.LogLevel = 3

	prot := &protocolV2{ctx: &context{nsqd: nsqdNs.New(opts)}}
	defer prot.ctx.nsqd.Exit()

	err := prot.IOLoop(fakeConn)

	test.NotNil(t, err)
	test.Equal(t, strings.HasPrefix(err.Error(), "E_INVALID "), true)
	test.NotNil(t, err.(*protocol.FatalClientErr))
}

func BenchmarkProtocolV2Exec(b *testing.B) {
	b.StopTimer()
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(b)
	opts.LogLevel = 0
	_ = &levellogger.GLogger{}
	_, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()
	ctx := &context{0, nsqd, nil, nil, nil, nil, ""}
	p := &protocolV2{ctx}
	c := nsqdNs.NewClientV2(0, nil, ctx.getOpts(), nil)
	params := [][]byte{[]byte("NOP")}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		p.Exec(c, params)
	}
}

func benchmarkProtocolV2PubWithArg(b *testing.B, size int, single bool) {
	var wg sync.WaitGroup
	b.StopTimer()
	opts := nsqdNs.NewOptions()
	batchSize := int(opts.MaxBodySize) / (size + 4)
	opts.Logger = newTestLogger(b)
	//opts.Logger = &levellogger.GLogger{}
	opts.LogLevel = 0
	opts.MemQueueSize = int64(b.N)
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	msg := make([]byte, size)
	batch := make([][]byte, batchSize)
	for i := range batch {
		batch[i] = msg
	}
	topicName := "bench_v2_pub" + strconv.Itoa(int(time.Now().Unix()))
	testTopic := nsqd.GetTopic(topicName, 0)

	b.SetBytes(int64(len(msg)))
	b.StartTimer()

	for j := 0; j < runtime.GOMAXPROCS(0); j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := mustConnectNSQD(tcpAddr)
			if err != nil {
				panic(err.Error())
			}
			rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

			num := b.N / runtime.GOMAXPROCS(0) / batchSize
			if single {
				num = b.N / runtime.GOMAXPROCS(0)
			}
			if num <= 0 {
				num = 1
			}
			for i := 0; i < num; i++ {
				var cmd *nsq.Command
				if single {
					cmd = nsq.PublishWithPart(topicName, "0", msg)
				} else {
					cmd, _ = nsq.MultiPublishWithPart(topicName, "0", batch)
				}
				_, err := cmd.WriteTo(rw)
				if err != nil {
					b.Error(err.Error())
					return
				}
				err = rw.Flush()
				if err != nil {
					b.Error(err.Error())
					return
				}
				resp, err := nsq.ReadResponse(rw)
				if err != nil {
					b.Error(err.Error())
					return
				}
				_, data, err := nsq.UnpackResponse(resp)
				if err != nil {
					b.Error(err.Error())
					return
				}

				if bytes.Equal(data, []byte("_heartbeat_")) {
					nsq.Nop().WriteTo(rw)
					rw.Flush()
					continue
				}
				if !bytes.Equal(data, []byte("OK")) {
					b.Error("response not OK :" + string(data))
					return
				}
			}
		}()
	}

	wg.Wait()

	b.StopTimer()
	b.Log(testTopic.GetDetailStats().GetPubClientStats())
	nsqdServer.Exit()

}

func benchmarkProtocolV2Pub(b *testing.B, size int) {
	benchmarkProtocolV2PubWithArg(b, size, false)
}

func benchmarkProtocolV2PubSingle(b *testing.B, size int) {
	benchmarkProtocolV2PubWithArg(b, size, true)
}

func BenchmarkProtocolV2Pub128Single(b *testing.B) { benchmarkProtocolV2PubSingle(b, 128) }
func BenchmarkProtocolV2Pub512Single(b *testing.B) { benchmarkProtocolV2PubSingle(b, 512) }

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
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(b)
	//opts.Logger = &levellogger.GLogger{}
	//glog.SetFlags(2, "INFO", "./")
	opts.LogLevel = 0
	opts.MemQueueSize = int64(b.N)
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	msg := make([]byte, size)
	topicName := "bench_v2_sub" + strconv.Itoa(b.N) + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	for i := 0; i < b.N; i++ {
		msg := nsqdNs.NewMessage(0, msg)
		topic.PutMessage(msg)
	}
	topic.ForceFlush()
	topic.GetChannel("ch").SetTrace(false)
	b.SetBytes(int64(len(msg)))
	goChan := make(chan int)
	rdyChan := make(chan int)
	workers := runtime.GOMAXPROCS(0)
	for j := 0; j < workers; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := subWorker(b.N, workers, tcpAddr, topicName, rdyChan, goChan)
			if err != nil {
				opts.Logger.Output(1, fmt.Sprintf("%v", err))
				b.Error(err.Error())
			}
		}()
		<-rdyChan
	}
	b.Logf("starting :%v", b.N)
	b.StartTimer()

	close(goChan)
	wg.Wait()
	b.Logf("done : %v", b.N)

	b.StopTimer()
	nsqdServer.Exit()
}

func subWorker(n int, workers int, tcpAddr *net.TCPAddr, topicName string, rdyChan chan int, goChan chan int) error {
	conn, err := mustConnectNSQD(tcpAddr)
	if err != nil {
		return err
	}
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriterSize(conn, 65536))

	identify(nil, conn, nil, frameTypeResponse)
	sub(nil, conn, topicName, "ch")

	rdyCount := int(math.Min(math.Max(float64(n/workers), 1), 2500))
	rdyChan <- 1
	<-goChan
	nsq.Ready(rdyCount).WriteTo(rw)
	rw.Flush()
	//traceLog := &levellogger.GLogger{}
	//traceLog.Output(1, fmt.Sprintf("begin from client: %v", conn.LocalAddr()))
	num := n / workers
	for i := 0; i < num; i++ {
		conn.SetReadDeadline(time.Now().Add(time.Second))
		resp, err := nsq.ReadResponse(rw)
		if err != nil {
			if err == io.EOF {
				return err
			} else {
				rw.Flush()
				continue
			}
		}
		frameType, data, err := nsq.UnpackResponse(resp)
		if err != nil {
			return err
		}
		if frameType == frameTypeResponse {
			if !bytes.Equal(data, heartbeatBytes) {
				return errors.New("got response not heartbeat:" + string(data))
			}
			nsq.Nop().WriteTo(rw)
			rw.Flush()
			continue
		}
		if frameType != frameTypeMessage {
			return errors.New("got something else")
		}
		msg, err := nsqdNs.DecodeMessage(data)
		if err != nil {
			return err
		}
		nsq.Finish(nsq.MessageID(msg.GetFullMsgID())).WriteTo(rw)
		if (i+1)%rdyCount == 0 || i+1 == num {
			if i+1 == num {
				nsq.Ready(0).WriteTo(conn)
			}
			rw.Flush()
		}
	}

	rw.Flush()
	conn.Close()
	//traceLog.Output(1, fmt.Sprintf("done from client: %v", conn.LocalAddr()))
	return nil
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

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(b)
	//opts.Logger = &levellogger.GLogger{}
	//glog.SetFlags(2, "INFO", "./")
	opts.LogLevel = 0
	opts.MemQueueSize = int64(b.N)
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	msg := make([]byte, 256)
	b.SetBytes(int64(len(msg) * num))

	goChan := make(chan int)
	rdyChan := make(chan int)
	workers := runtime.GOMAXPROCS(0)
	for i := 0; i < num; i++ {
		topicName := "bench_v2" + strconv.Itoa(b.N) + "_" + strconv.Itoa(i) + "_" + strconv.Itoa(int(time.Now().Unix()))
		topic := nsqd.GetTopicIgnPart(topicName)
		for i := 0; i < b.N; i++ {
			msg := nsqdNs.NewMessage(0, msg)
			topic.PutMessage(msg)
		}
		topic.ForceFlush()
		topic.GetChannel("ch")

		for j := 0; j < workers; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := subWorker(b.N, workers, tcpAddr, topicName, rdyChan, goChan)
				if err != nil {
					b.Error(err.Error())
				} else {
					b.Logf("sub finished ok")
				}
			}()
			<-rdyChan
		}
	}
	b.StartTimer()

	close(goChan)
	wg.Wait()

	b.StopTimer()
	nsqdServer.Exit()
}

func BenchmarkProtocolV2MultiSub1(b *testing.B)  { benchmarkProtocolV2MultiSub(b, 1) }
func BenchmarkProtocolV2MultiSub2(b *testing.B)  { benchmarkProtocolV2MultiSub(b, 2) }
func BenchmarkProtocolV2MultiSub4(b *testing.B)  { benchmarkProtocolV2MultiSub(b, 4) }
func BenchmarkProtocolV2MultiSub8(b *testing.B)  { benchmarkProtocolV2MultiSub(b, 8) }
func BenchmarkProtocolV2MultiSub16(b *testing.B) { benchmarkProtocolV2MultiSub(b, 16) }
