package main

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/bitly/nsq/nsq"
	"github.com/bmizerany/assert"
	"io/ioutil"
	"log"
	"math"
	"net"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
)

func mustStartNSQd(options *nsqdOptions) (*net.TCPAddr, *net.TCPAddr) {
	tcpAddr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	httpAddr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	nsqd = NewNSQd(1, options)
	nsqd.tcpAddr = tcpAddr
	nsqd.httpAddr = httpAddr
	nsqd.Main()
	return nsqd.tcpListener.Addr().(*net.TCPAddr), nsqd.httpListener.Addr().(*net.TCPAddr)
}

func mustConnectNSQd(tcpAddr *net.TCPAddr) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", tcpAddr.String(), time.Second)
	if err != nil {
		return nil, err
	}
	conn.Write(nsq.MagicV2)
	return conn, nil
}

func identify(t *testing.T, conn net.Conn) {
	ci := make(map[string]interface{})
	ci["short_id"] = "test"
	ci["long_id"] = "test"
	cmd, _ := nsq.Identify(ci)
	err := cmd.Write(conn)
	assert.Equal(t, err, nil)
	readValidateOK(t, conn)
}

func identifyDisabledHearbeat(t *testing.T, conn net.Conn) {
	ci := make(map[string]interface{})
	ci["short_id"] = "test"
	ci["long_id"] = "test"
	ci["heartbeat_interval"] = -1
	cmd, _ := nsq.Identify(ci)
	err := cmd.Write(conn)
	assert.Equal(t, err, nil)
	readValidateOK(t, conn)
}

func sub(t *testing.T, conn net.Conn, topicName string, channelName string) {
	err := nsq.Subscribe(topicName, channelName).Write(conn)
	assert.Equal(t, err, nil)
	readValidateOK(t, conn)
}

func subFail(t *testing.T, conn net.Conn, topicName string, channelName string) {
	err := nsq.Subscribe(topicName, channelName).Write(conn)
	assert.Equal(t, err, nil)
	resp, err := nsq.ReadResponse(conn)
	frameType, _, err := nsq.UnpackResponse(resp)
	assert.Equal(t, frameType, nsq.FrameTypeError)
}

func readValidateOK(t *testing.T, conn net.Conn) {
	resp, err := nsq.ReadResponse(conn)
	assert.Equal(t, err, nil)
	frameType, data, err := nsq.UnpackResponse(resp)
	assert.Equal(t, err, nil)
	assert.Equal(t, frameType, nsq.FrameTypeResponse)
	assert.Equal(t, data, []byte("OK"))
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

	options := NewNsqdOptions()
	options.clientTimeout = 60 * time.Second
	tcpAddr, _ := mustStartNSQd(options)
	defer nsqd.Exit()

	topicName := "test_v2" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	msg := nsq.NewMessage(<-nsqd.idChan, []byte("test body"))
	topic.PutMessage(msg)

	conn, err := mustConnectNSQd(tcpAddr)
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

	options := NewNsqdOptions()
	options.clientTimeout = 60 * time.Second
	tcpAddr, _ := mustStartNSQd(options)
	defer nsqd.Exit()

	topicName := "test_multiple_v2" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	msg := nsq.NewMessage(<-nsqd.idChan, []byte("test body"))
	topic.GetChannel("ch1")
	topic.GetChannel("ch2")
	topic.PutMessage(msg)

	for _, i := range []string{"1", "2"} {
		conn, err := mustConnectNSQd(tcpAddr)
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

	options := NewNsqdOptions()
	options.clientTimeout = 50 * time.Millisecond
	tcpAddr, _ := mustStartNSQd(options)
	defer nsqd.Exit()

	conn, err := mustConnectNSQd(tcpAddr)
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

	options := NewNsqdOptions()
	options.clientTimeout = 100 * time.Millisecond
	tcpAddr, _ := mustStartNSQd(options)
	defer nsqd.Exit()

	conn, err := mustConnectNSQd(tcpAddr)
	assert.Equal(t, err, nil)

	identify(t, conn)
	sub(t, conn, topicName, "ch")

	err = nsq.Ready(1).Write(conn)
	assert.Equal(t, err, nil)

	resp, _ := nsq.ReadResponse(conn)
	_, data, _ := nsq.UnpackResponse(resp)
	assert.Equal(t, data, []byte("_heartbeat_"))

	time.Sleep(10 * time.Millisecond)

	err = nsq.Nop().Write(conn)
	assert.Equal(t, err, nil)

	// wait long enough that would have timed out (had we not sent the above cmd)
	time.Sleep(50 * time.Millisecond)

	err = nsq.Nop().Write(conn)
	assert.Equal(t, err, nil)
}

func TestClientHeartbeatDisableSUB(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	topicName := "test_hb_v2" + strconv.Itoa(int(time.Now().Unix()))

	options := NewNsqdOptions()
	options.clientTimeout = 200 * time.Millisecond
	tcpAddr, _ := mustStartNSQd(options)
	defer nsqd.Exit()

	conn, err := mustConnectNSQd(tcpAddr)
	assert.Equal(t, err, nil)

	identifyDisabledHearbeat(t, conn)
	subFail(t, conn, topicName, "ch")
}

func TestClientHeartbeatDisable(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	options := NewNsqdOptions()
	options.clientTimeout = 100 * time.Millisecond
	tcpAddr, _ := mustStartNSQd(options)
	defer nsqd.Exit()

	conn, err := mustConnectNSQd(tcpAddr)
	assert.Equal(t, err, nil)

	identifyDisabledHearbeat(t, conn)

	time.Sleep(150 * time.Millisecond)

	err = nsq.Nop().Write(conn)
	assert.Equal(t, err, nil)

}

func TestPausing(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	topicName := "test_pause_v2" + strconv.Itoa(int(time.Now().Unix()))

	tcpAddr, _ := mustStartNSQd(NewNsqdOptions())
	defer nsqd.Exit()

	conn, err := mustConnectNSQd(tcpAddr)
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

	tcpAddr, _ := mustStartNSQd(NewNsqdOptions())
	defer nsqd.Exit()

	conn, err := mustConnectNSQd(tcpAddr)
	assert.Equal(t, err, nil)

	_, err = conn.Write([]byte("\n\n"))
	assert.Equal(t, err, nil)

	// if we didn't panic here we're good, see issue #120
}

func TestSizeLimits(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	options := NewNsqdOptions()
	*verbose = true
	options.maxMessageSize = 100
	options.maxBodySize = 1000
	tcpAddr, _ := mustStartNSQd(options)
	defer nsqd.Exit()

	conn, err := mustConnectNSQd(tcpAddr)
	assert.Equal(t, err, nil)

	topicName := "test_limits_v2" + strconv.Itoa(int(time.Now().Unix()))

	identify(t, conn)
	sub(t, conn, topicName, "ch")

	nsq.Publish(topicName, make([]byte, 95)).Write(conn)
	resp, _ := nsq.ReadResponse(conn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	log.Printf("frameType: %d, data: %s", frameType, data)
	assert.Equal(t, frameType, nsq.FrameTypeResponse)
	assert.Equal(t, data, []byte("OK"))

	nsq.Publish(topicName, make([]byte, 105)).Write(conn)
	resp, _ = nsq.ReadResponse(conn)
	frameType, data, _ = nsq.UnpackResponse(resp)
	log.Printf("frameType: %d, data: %s", frameType, data)
	assert.Equal(t, frameType, nsq.FrameTypeError)
	assert.Equal(t, string(data), fmt.Sprintf("E_BAD_MESSAGE PUB message too big 105 > 100"))

	// need to reconnect
	conn, err = mustConnectNSQd(tcpAddr)
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
	conn, err = mustConnectNSQd(tcpAddr)
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
	options := NewNsqdOptions()
	options.msgTimeout = 50 * time.Millisecond
	tcpAddr, _ := mustStartNSQd(options)
	defer nsqd.Exit()

	topicName := "test_touch" + strconv.Itoa(int(time.Now().Unix()))

	conn, err := mustConnectNSQd(tcpAddr)
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

func BenchmarkProtocolV2Exec(b *testing.B) {
	b.StopTimer()
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)
	p := &ProtocolV2{}
	c := NewClientV2(nil)
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
	options := NewNsqdOptions()
	options.memQueueSize = int64(b.N)
	tcpAddr, _ := mustStartNSQd(options)
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
			conn, err := mustConnectNSQd(tcpAddr)
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
	options := NewNsqdOptions()
	options.memQueueSize = int64(b.N)
	tcpAddr, _ := mustStartNSQd(options)
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
	conn, err := mustConnectNSQd(tcpAddr)
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

	options := NewNsqdOptions()
	options.memQueueSize = int64(b.N)
	tcpAddr, _ := mustStartNSQd(options)
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
