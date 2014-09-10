package nsq

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"testing"
	"time"
)

type instruction struct {
	delay     time.Duration
	frameType int32
	body      []byte
}

type mockNSQD struct {
	script      []instruction
	got         [][]byte
	tcpAddr     *net.TCPAddr
	tcpListener net.Listener
	exitChan    chan int
}

func newMockNSQD(script []instruction) *mockNSQD {
	n := &mockNSQD{
		script:   script,
		exitChan: make(chan int),
	}

	addr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	tcpListener, err := net.Listen("tcp", addr.String())
	if err != nil {
		log.Fatalf("FATAL: listen (%s) failed - %s", n.tcpAddr.String(), err)
	}
	n.tcpListener = tcpListener
	n.tcpAddr = tcpListener.Addr().(*net.TCPAddr)

	go n.listen()

	return n
}

func (n *mockNSQD) listen() {
	log.Printf("TCP: listening on %s", n.tcpListener.Addr().String())

	for {
		conn, err := n.tcpListener.Accept()
		if err != nil {
			break
		}
		go n.handle(conn)
	}

	log.Printf("TCP: closing %s", n.tcpListener.Addr().String())
	close(n.exitChan)
}

func (n *mockNSQD) handle(conn net.Conn) {
	var idx int

	log.Printf("TCP: new client(%s)", conn.RemoteAddr())

	buf := make([]byte, 4)
	_, err := io.ReadFull(conn, buf)
	if err != nil {
		log.Fatalf("ERROR: failed to read protocol version - %s", err)
	}

	readChan := make(chan []byte)
	readDoneChan := make(chan int)
	scriptTime := time.After(n.script[0].delay)
	rdr := bufio.NewReader(conn)

	go func() {
		for {
			line, err := rdr.ReadBytes('\n')
			if err != nil {
				return
			}
			// trim the '\n'
			line = line[:len(line)-1]
			readChan <- line
			<-readDoneChan
		}
	}()

	var rdyCount int
	for idx < len(n.script) {
		select {
		case line := <-readChan:
			log.Printf("mock: %s", line)
			n.got = append(n.got, line)
			params := bytes.Split(line, []byte(" "))
			switch {
			case bytes.Equal(params[0], []byte("IDENTIFY")):
				l := make([]byte, 4)
				_, err := io.ReadFull(rdr, l)
				if err != nil {
					log.Printf(err.Error())
					goto exit
				}
				size := int32(binary.BigEndian.Uint32(l))
				b := make([]byte, size)
				_, err = io.ReadFull(rdr, b)
				if err != nil {
					log.Printf(err.Error())
					goto exit
				}
				log.Printf("%s", b)
			case bytes.Equal(params[0], []byte("RDY")):
				rdy, _ := strconv.Atoi(string(params[1]))
				rdyCount = rdy
			case bytes.Equal(params[0], []byte("FIN")):
			case bytes.Equal(params[0], []byte("REQ")):
			}
			readDoneChan <- 1
		case <-scriptTime:
			inst := n.script[idx]
			if bytes.Equal(inst.body, []byte("exit")) {
				goto exit
			}
			if inst.frameType == FrameTypeMessage {
				if rdyCount == 0 {
					log.Printf("!!! RDY == 0")
					scriptTime = time.After(n.script[idx+1].delay)
					continue
				}
				rdyCount--
			}
			_, err := conn.Write(framedResponse(inst.frameType, inst.body))
			if err != nil {
				log.Printf(err.Error())
				goto exit
			}
			scriptTime = time.After(n.script[idx+1].delay)
			idx++
		}
	}

exit:
	n.tcpListener.Close()
}

func framedResponse(frameType int32, data []byte) []byte {
	var w bytes.Buffer

	beBuf := make([]byte, 4)
	size := uint32(len(data)) + 4

	binary.BigEndian.PutUint32(beBuf, size)
	_, err := w.Write(beBuf)
	if err != nil {
		return nil
	}

	binary.BigEndian.PutUint32(beBuf, uint32(frameType))
	_, err = w.Write(beBuf)
	if err != nil {
		return nil
	}

	_, err = w.Write(data)
	return w.Bytes()
}

type testHandler struct{}

func (h *testHandler) HandleMessage(message *Message) error {
	if bytes.Equal(message.Body, []byte("bad")) {
		return errors.New("bad")
	}
	return nil
}

func TestConsumerBackoff(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	logger := log.New(ioutil.Discard, "", log.LstdFlags)

	var mgood bytes.Buffer
	msgIDGood := MessageID{'1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'a', 's', 'd', 'f', 'g', 'h'}
	msgGood := NewMessage(msgIDGood, []byte("good"))
	msgGood.WriteTo(&mgood)
	msgBytesGood := mgood.Bytes()

	var mbad bytes.Buffer
	msgIDBad := MessageID{'z', 'x', 'c', 'v', 'b', '6', '7', '8', '9', '0', 'a', 's', 'd', 'f', 'g', 'h'}
	msgBad := NewMessage(msgIDBad, []byte("bad"))
	msgBad.WriteTo(&mbad)
	msgBytesBad := mbad.Bytes()

	script := []instruction{
		// SUB
		instruction{0, FrameTypeResponse, []byte("OK")},
		// IDENTIFY
		instruction{0, FrameTypeResponse, []byte("OK")},
		instruction{20 * time.Millisecond, FrameTypeMessage, msgBytesGood},
		instruction{20 * time.Millisecond, FrameTypeMessage, msgBytesGood},
		instruction{20 * time.Millisecond, FrameTypeMessage, msgBytesGood},
		instruction{20 * time.Millisecond, FrameTypeMessage, msgBytesBad},
		instruction{20 * time.Millisecond, FrameTypeMessage, msgBytesBad},
		instruction{20 * time.Millisecond, FrameTypeMessage, msgBytesGood},
		instruction{20 * time.Millisecond, FrameTypeMessage, msgBytesGood},
		// needed to exit test
		instruction{200 * time.Millisecond, -1, []byte("exit")},
	}
	n := newMockNSQD(script)

	topicName := "test_consumer_commands" + strconv.Itoa(int(time.Now().Unix()))
	config := NewConfig()
	config.MaxInFlight = 5
	config.BackoffMultiplier = 10 * time.Millisecond
	q, _ := NewConsumer(topicName, "ch", config)
	q.SetLogger(logger, LogLevelDebug)
	q.AddHandler(&testHandler{})
	err := q.ConnectToNSQD(n.tcpAddr.String())
	if err != nil {
		t.Fatalf(err.Error())
	}

	<-n.exitChan

	for i, r := range n.got {
		log.Printf("%d: %s", i, r)
	}

	expected := []string{
		"IDENTIFY",
		"SUB " + topicName + " ch",
		"RDY 5",
		fmt.Sprintf("FIN %s", msgIDGood),
		fmt.Sprintf("FIN %s", msgIDGood),
		fmt.Sprintf("FIN %s", msgIDGood),
		"RDY 5",
		fmt.Sprintf("REQ %s 0", msgIDBad),
		"RDY 0",
		"RDY 1",
		fmt.Sprintf("REQ %s 0", msgIDBad),
		"RDY 0",
		"RDY 1",
		fmt.Sprintf("FIN %s", msgIDGood),
		"RDY 0",
		"RDY 1",
		fmt.Sprintf("FIN %s", msgIDGood),
		"RDY 5",
	}
	if len(n.got) != len(expected) {
		t.Fatalf("we got %d commands != %d expected", len(n.got), len(expected))
	}
	for i, r := range n.got {
		if string(r) != expected[i] {
			t.Fatalf("cmd %d bad %s != %s", i, r, expected[i])
		}
	}
}
