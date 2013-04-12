package main

import (
	"bufio"
	"github.com/bitly/nsq/nsq"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type ClientV2 struct {
	net.Conn
	sync.Mutex
	Reader          *bufio.Reader
	Writer          *bufio.Writer
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

	return &ClientV2{
		Conn: conn,
		// ReadyStateChan has a buffer of 1 to guarantee that in the event
		// there is a race the state update is not lost
		ReadyStateChan:  make(chan int, 1),
		ExitChan:        make(chan int),
		ConnectTime:     time.Now(),
		ShortIdentifier: identifier,
		LongIdentifier:  identifier,
		Reader:          bufio.NewReaderSize(conn, 16*1024),
		Writer:          bufio.NewWriterSize(conn, 16*1024),
		State:           nsq.StateInit,
		SubEventChan:    make(chan *Channel, 1),

		// heartbeats are client configurable but default to 30s
		Heartbeat:           time.NewTicker(nsqd.options.clientTimeout / 2),
		HeartbeatInterval:   nsqd.options.clientTimeout / 2,
		HeartbeatUpdateChan: make(chan time.Duration, 1),
	}
}

func (c *ClientV2) String() string {
	return c.RemoteAddr().String()
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
