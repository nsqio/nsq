package main

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/bitly/nsq/nsq"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type IdentifyDataV2 struct {
	ShortId             string `json:"short_id"`
	LongId              string `json:"long_id"`
	HeartbeatInterval   int    `json:"heartbeat_interval"`
	OutputBufferSize    int    `json:"output_buffer_size"`
	OutputBufferTimeout int    `json:"output_buffer_timeout"`
	FeatureNegotiation  bool   `json:"feature_negotiation"`
}

type ClientV2 struct {
	net.Conn
	sync.Mutex

	// buffered IO
	Reader                        *bufio.Reader
	Writer                        *bufio.Writer
	OutputBufferTimeout           *time.Ticker
	OutputBufferTimeoutUpdateChan chan time.Duration

	State           int32
	ReadyCount      int64
	LastReadyCount  int64
	InFlightCount   int64
	MessageCount    uint64
	FinishCount     uint64
	RequeueCount    uint64
	ConnectTime     time.Time
	Channel         *Channel
	ReadyStateChan  chan int
	ExitChan        chan int
	ShortIdentifier string
	LongIdentifier  string
	SubEventChan    chan *Channel

	// re-usable buffer for reading the 4-byte lengths off the wire
	lenBuf   [4]byte
	lenSlice []byte

	// heartbeats are client configurable via IDENTIFY
	Heartbeat           *time.Ticker
	HeartbeatInterval   time.Duration
	HeartbeatUpdateChan chan time.Duration
}

func NewClientV2(conn net.Conn) *ClientV2 {
	var identifier string
	if conn != nil {
		identifier, _, _ = net.SplitHostPort(conn.RemoteAddr().String())
	}

	c := &ClientV2{
		Conn: conn,

		Reader:                        bufio.NewReaderSize(conn, 16*1024),
		Writer:                        bufio.NewWriterSize(conn, 16*1024),
		OutputBufferTimeout:           time.NewTicker(5 * time.Millisecond),
		OutputBufferTimeoutUpdateChan: make(chan time.Duration, 1),

		// ReadyStateChan has a buffer of 1 to guarantee that in the event
		// there is a race the state update is not lost
		ReadyStateChan:  make(chan int, 1),
		ExitChan:        make(chan int),
		ConnectTime:     time.Now(),
		ShortIdentifier: identifier,
		LongIdentifier:  identifier,
		State:           nsq.StateInit,
		SubEventChan:    make(chan *Channel, 1),

		// heartbeats are client configurable but default to 30s
		Heartbeat:           time.NewTicker(nsqd.options.clientTimeout / 2),
		HeartbeatInterval:   nsqd.options.clientTimeout / 2,
		HeartbeatUpdateChan: make(chan time.Duration, 1),
	}
	c.lenSlice = c.lenBuf[:]
	return c
}

func (c *ClientV2) String() string {
	return c.RemoteAddr().String()
}

func (c *ClientV2) Identify(data IdentifyDataV2) error {
	c.ShortIdentifier = data.ShortId
	c.LongIdentifier = data.LongId
	err := c.SetHeartbeatInterval(data.HeartbeatInterval)
	if err != nil {
		return err
	}
	err = c.SetOutputBufferSize(data.OutputBufferSize)
	if err != nil {
		return err
	}
	return c.SetOutputBufferTimeout(data.OutputBufferTimeout)
}

func (c *ClientV2) Stats() ClientStats {
	return ClientStats{
		Version:       "V2",
		RemoteAddress: c.RemoteAddr().String(),
		Name:          c.ShortIdentifier,
		State:         atomic.LoadInt32(&c.State),
		ReadyCount:    atomic.LoadInt64(&c.ReadyCount),
		InFlightCount: atomic.LoadInt64(&c.InFlightCount),
		MessageCount:  atomic.LoadUint64(&c.MessageCount),
		FinishCount:   atomic.LoadUint64(&c.FinishCount),
		RequeueCount:  atomic.LoadUint64(&c.RequeueCount),
		ConnectTime:   c.ConnectTime.Unix(),
	}
}

func (c *ClientV2) IsReadyForMessages() bool {
	if c.Channel.IsPaused() {
		return false
	}

	readyCount := atomic.LoadInt64(&c.ReadyCount)
	lastReadyCount := atomic.LoadInt64(&c.LastReadyCount)
	inFlightCount := atomic.LoadInt64(&c.InFlightCount)

	if *verbose {
		log.Printf("[%s] state rdy: %4d lastrdy: %4d inflt: %4d", c,
			readyCount, lastReadyCount, inFlightCount)
	}

	if inFlightCount >= lastReadyCount || readyCount <= 0 {
		return false
	}

	return true
}

func (c *ClientV2) SetReadyCount(count int64) {
	atomic.StoreInt64(&c.ReadyCount, count)
	atomic.StoreInt64(&c.LastReadyCount, count)
	c.tryUpdateReadyState()
}

func (c *ClientV2) tryUpdateReadyState() {
	// you can always *try* to write to ReadyStateChan because in the cases
	// where you cannot the message pump loop would have iterated anyway.
	// the atomic integer operations guarantee correctness of the value.
	select {
	case c.ReadyStateChan <- 1:
	default:
	}
}

func (c *ClientV2) FinishedMessage() {
	atomic.AddUint64(&c.FinishCount, 1)
	atomic.AddInt64(&c.InFlightCount, -1)
	c.tryUpdateReadyState()
}

func (c *ClientV2) Empty() {
	atomic.StoreInt64(&c.InFlightCount, 0)
	c.tryUpdateReadyState()
}

func (c *ClientV2) SendingMessage() {
	atomic.AddInt64(&c.ReadyCount, -1)
	atomic.AddInt64(&c.InFlightCount, 1)
	atomic.AddUint64(&c.MessageCount, 1)
}

func (c *ClientV2) TimedOutMessage() {
	atomic.AddInt64(&c.InFlightCount, -1)
	c.tryUpdateReadyState()
}

func (c *ClientV2) RequeuedMessage() {
	atomic.AddUint64(&c.RequeueCount, 1)
	atomic.AddInt64(&c.InFlightCount, -1)
	c.tryUpdateReadyState()
}

func (c *ClientV2) StartClose() {
	// Force the client into ready 0
	c.SetReadyCount(0)
	// mark this client as closing
	atomic.StoreInt32(&c.State, nsq.StateClosing)
	// TODO: start a timer to actually close the channel (in case the client doesn't do it first)
}

func (c *ClientV2) Pause() {
	c.tryUpdateReadyState()
}

func (c *ClientV2) UnPause() {
	c.tryUpdateReadyState()
}

func (c *ClientV2) SetHeartbeatInterval(desiredInterval int) error {
	// clients can modify the rate of heartbeats (or disable)
	var interval time.Duration

	switch {
	case desiredInterval == -1:
		interval = -1
	case desiredInterval == 0:
		// do nothing (use default)
	case desiredInterval >= 1000 &&
		desiredInterval <= int(nsqd.options.maxHeartbeatInterval/time.Millisecond):
		interval = (time.Duration(desiredInterval) * time.Millisecond)
	default:
		return errors.New(fmt.Sprintf("heartbeat interval (%d) is invalid", desiredInterval))
	}

	// leave the default heartbeat in place
	if desiredInterval != 0 {
		select {
		case c.HeartbeatUpdateChan <- interval:
		default:
		}
		c.HeartbeatInterval = interval
	}

	return nil
}

func (c *ClientV2) SetOutputBufferSize(desiredSize int) error {
	c.Lock()
	defer c.Unlock()

	var size int

	switch {
	case desiredSize == -1:
		// effectively no buffer (every write will go directly to the wrapped net.Conn)
		size = 1
	case desiredSize == 0:
		// do nothing (use default)
	case desiredSize >= 64 && desiredSize <= int(nsqd.options.maxOutputBufferSize):
		size = desiredSize
	default:
		return errors.New(fmt.Sprintf("output buffer size (%d) is invalid", desiredSize))
	}

	if size > 0 {
		err := c.Writer.Flush()
		if err != nil {
			return err
		}
		c.Writer = bufio.NewWriterSize(c.Conn, size)
	}

	return nil
}

func (c *ClientV2) SetOutputBufferTimeout(desiredTimeout int) error {
	var timeout time.Duration

	switch {
	case desiredTimeout == -1:
		timeout = -1
	case desiredTimeout == 0:
		// do nothing (use default)
	case desiredTimeout >= 5 &&
		desiredTimeout <= int(nsqd.options.maxOutputBufferTimeout/time.Millisecond):
		timeout = (time.Duration(desiredTimeout) * time.Millisecond)
	default:
		return errors.New(fmt.Sprintf("output buffer timeout (%d) is invalid", desiredTimeout))
	}

	if desiredTimeout != 0 {
		select {
		case c.OutputBufferTimeoutUpdateChan <- timeout:
		default:
		}
	}

	return nil
}
