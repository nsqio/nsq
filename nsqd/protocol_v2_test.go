package main

import (
	"../nsq"
	"bufio"
	"bytes"
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

	err = nsq.Subscribe(topicName, "ch").Write(conn)
	assert.Equal(t, err, nil)

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

		err = nsq.Subscribe(topicName, "ch"+i).Write(conn)
		assert.Equal(t, err, nil)

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

	err = nsq.Subscribe(topicName, "ch").Write(conn)
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

	options := NewNsqdOptions()
	options.clientTimeout = 100 * time.Millisecond
	tcpAddr, _ := mustStartNSQd(options)
	defer nsqd.Exit()

	conn, err := mustConnectNSQd(tcpAddr)
	assert.Equal(t, err, nil)

	err = nsq.Subscribe(topicName, "ch").Write(conn)
	assert.Equal(t, err, nil)

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

func TestPausing(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	topicName := "test_pause_v2" + strconv.Itoa(int(time.Now().Unix()))

	tcpAddr, _ := mustStartNSQd(NewNsqdOptions())
	defer nsqd.Exit()

	conn, err := mustConnectNSQd(tcpAddr)
	assert.Equal(t, err, nil)

	err = nsq.Subscribe(topicName, "ch").Write(conn)
	assert.Equal(t, err, nil)

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
		go func(id int) {
			subWorker(b.N, workers, tcpAddr, topicName, rdyChan, goChan, id)
			wg.Done()
		}(j)
		<-rdyChan
	}
	b.StartTimer()

	close(goChan)
	wg.Wait()

	b.StopTimer()
	nsqd.Exit()
}

func subWorker(n int, workers int, tcpAddr *net.TCPAddr, topicName string, rdyChan chan int, goChan chan int, id int) {
	conn, err := mustConnectNSQd(tcpAddr)
	if err != nil {
		panic(err.Error())
	}
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	ci := make(map[string]interface{})
	ci["short_id"] = "test"
	ci["long_id"] = "test"
	cmd, _ := nsq.Identify(ci)
	cmd.Write(rw)
	nsq.Subscribe(topicName, "ch").Write(rw)
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
