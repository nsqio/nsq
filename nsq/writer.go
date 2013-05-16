package nsq

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

type Writer struct {
	net.Conn
	transactionChan   chan *writerTransaction
	dataChan          chan []byte
	WriteTimeout      time.Duration
	transactions      []*writerTransaction
	state             int32
	stopFlag          int32
	transactionStat   int32
	HeartbeatInterval int
	ShortIdentifier   string
	LongIdentifier    string
	exitChan          chan int
}

type writerTransaction struct {
	doneChan  chan int
	cmd       *Command
	frameType int32
	data      []byte
	err       error
}

func NewWriter(heartbeatInterval int) *Writer {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("ERROR: unable to get hostname %s", err.Error())
	}
	this := &Writer{
		transactionChan:   make(chan *writerTransaction),
		exitChan:          make(chan int),
		dataChan:          make(chan []byte),
		state:             StateDisconnected,
		stopFlag:          0,
		transactionStat:   0,
		WriteTimeout:      time.Second,
		HeartbeatInterval: heartbeatInterval,
		ShortIdentifier:   strings.Split(hostname, ".")[0],
		LongIdentifier:    hostname,
	}
	return this
}

func (this *Writer) Stop() {
	if !atomic.CompareAndSwapInt32(&this.stopFlag, 0, 1) {
		return
	}
	this.close()
}

func (this *Writer) Publish(topic string, body []byte) (int32, []byte, error) {
	cmd := Publish(topic, body)
	return this.sendCommand(cmd)
}

func (this *Writer) MultiPublish(topic string, body [][]byte) (int32, []byte, error) {
	cmd, err := MultiPublish(topic, body)
	if err != nil {
		return -1, nil, err
	}
	return this.sendCommand(cmd)
}

func (this *Writer) sendCommand(cmd *Command) (int32, []byte, error) {
	if atomic.LoadInt32(&this.state) != StateConnected {
		return -1, nil, errors.New("not connected")
	}
	t := &writerTransaction{
		cmd:       cmd,
		frameType: -1,
		doneChan:  make(chan int),
	}
	this.transactionChan <- t
	<-t.doneChan
	return t.frameType, t.data, t.err
}

func (this *Writer) ConnectToNSQ(addr string) error {
	if atomic.LoadInt32(&this.stopFlag) == 1 {
		return errors.New("writer stopped")
	}
	if atomic.LoadInt32(&this.transactionStat) != 0 {
		return errors.New("writer transactions not cleaned")
	}
	if !atomic.CompareAndSwapInt32(&this.state, StateDisconnected, StateConnected) {
		return nil
	}
	var err error
	this.Conn, err = net.DialTimeout("tcp", addr, time.Second*5)
	if err != nil {
		atomic.StoreInt32(&this.state, StateDisconnected)
		return errors.New("failed to connect nsq")
	}
	this.SetWriteDeadline(time.Now().Add(this.WriteTimeout))
	if _, err := this.Conn.Write(MagicV2); err != nil {
		this.close()
		return err
	}
	atomic.StoreInt32(&this.state, StateConnected)
	this.exitChan = make(chan int)
	ci := make(map[string]interface{})
	ci["short_id"] = this.ShortIdentifier
	ci["long_id"] = this.LongIdentifier
	ci["heartbeat_interval"] = this.HeartbeatInterval
	ci["feature_negotiation"] = true
	cmd, err := Identify(ci)
	if err != nil {
		this.close()
		return fmt.Errorf("[%s] failed to create identify command - %s", this.RemoteAddr(), err.Error())
	}
	go this.readLoop()
	go this.messageRouter()
	frameType, data, err := this.sendCommand(cmd)
	if frameType == FrameTypeError {
		if err == nil {
			this.close()
			err = fmt.Errorf("[%s] error failed to identify - %s", this.RemoteAddr(), string(data))
		}
	}
	return err
}

func (this *Writer) close() {
	if atomic.CompareAndSwapInt32(&this.state, StateConnected, StateDisconnected) {
		close(this.exitChan)
		this.Conn.Close()
	}
}

func (this *Writer) messageRouter() {
	var err error
	defer this.transactionCleanup()
	atomic.StoreInt32(&this.transactionStat, 1)
	for {
		select {
		case t := <-this.transactionChan:
			this.transactions = append(this.transactions, t)
			this.SetWriteDeadline(time.Now().Add(this.WriteTimeout))
			if err = t.cmd.Write(this.Conn); err != nil {
				log.Printf("[%s] error writing %s", this.RemoteAddr(), err.Error())
				this.close()
				return
			}
		case buf := <-this.dataChan:
			frameType, data, err := UnpackResponse(buf)
			if err != nil {
				log.Printf("[%s] error (%s) unpacking response %d %s", this.RemoteAddr(), err.Error(), frameType, data)
				return
			}
			if frameType == FrameTypeResponse &&
				bytes.Equal(data, []byte("_heartbeat_")) {
				log.Printf("[%s] received heartbeat", this.RemoteAddr())
				if err := this.heartbeat(); err != nil {
					log.Printf("[%s] error sending heartbeat - %s", this.RemoteAddr(), err.Error())
					this.close()
					return
				}
			} else {
				t := this.transactions[0]
				this.transactions = this.transactions[1:]
				t.frameType = frameType
				t.data = data
				t.err = err
				t.doneChan <- 1
			}
		case <-this.exitChan:
			return
		}
	}
}

//send heartbeat
func (this *Writer) heartbeat() error {
	this.SetWriteDeadline(time.Now().Add(this.WriteTimeout))
	if err := Nop().Write(this.Conn); err != nil {
		return err
	}
	return nil
}

// cleanup transactions
func (this *Writer) transactionCleanup() {
	for _, t := range this.transactions {
		t.err = errors.New("not connected")
		t.doneChan <- 1
	}
	this.transactions = this.transactions[:0]
	atomic.StoreInt32(&this.transactionStat, 0)
}

func (this *Writer) readLoop() {
	rbuf := bufio.NewReader(this.Conn)
	for {
		resp, err := ReadResponse(rbuf)
		if err != nil {
			log.Printf("[%s] error reading response %s", this.RemoteAddr(), err.Error())
			if !strings.Contains(err.Error(), "use of closed network connection") {
				this.close()
			}
			break
		}
		select {
		case this.dataChan <- resp:
		case <-this.exitChan:
			return
		}
	}
}
