package main

import (
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type ClientV2 struct {
	net.Conn
	sync.Mutex
	State            int
	ReadyCount       int64
	LastReadyCount   int64
	InFlightCount    int64
	MessageCount     uint64
	FinishCount      uint64
	RequeueCount     uint64
	ConnectTime      time.Time
	Channel          *Channel
	ReadyStateChange chan int
	ExitChan         chan int
	ShortIdentifier  string
	LongIdentifier   string
}

func NewClientV2(conn net.Conn) *ClientV2 {
	var identifier string
	if conn != nil {
		identifier = conn.RemoteAddr().String()
	}
	return &ClientV2{
		net.Conn:         conn,
		ReadyStateChange: make(chan int, 10),
		ExitChan:         make(chan int),
		ConnectTime:      time.Now(),
		ShortIdentifier:  identifier,
		LongIdentifier:   identifier,
	}
}

func (c *ClientV2) Stats() ClientStats {
	return ClientStats{
		version:       "V2",
		address:       c.RemoteAddr().String(),
		name:          c.ShortIdentifier,
		state:         c.State,
		readyCount:    atomic.LoadInt64(&c.ReadyCount),
		inFlightCount: atomic.LoadInt64(&c.InFlightCount),
		messageCount:  atomic.LoadUint64(&c.MessageCount),
		finishCount:   atomic.LoadUint64(&c.FinishCount),
		requeueCount:  atomic.LoadUint64(&c.RequeueCount),
		connectTime:   c.ConnectTime,
	}
}

func (c *ClientV2) IsReadyForMessages() bool {
	readyCount := atomic.LoadInt64(&c.ReadyCount)
	lastReadyCount := atomic.LoadInt64(&c.LastReadyCount)
	inFlightCount := atomic.LoadInt64(&c.InFlightCount)

	if *verbose {
		log.Printf("[%s] state rdy: %4d inflt: %4d", c.RemoteAddr().String(), readyCount, inFlightCount)
	}

	if inFlightCount >= lastReadyCount || readyCount <= 0 {
		return false
	}

	return true
}

func (c *ClientV2) SetReadyCount(newCount int) {
	count := int64(newCount)
	readyCount := atomic.LoadInt64(&c.ReadyCount)
	// lastReadyCount := atomic.LoadInt64(&c.LastReadyCount)
	atomic.StoreInt64(&c.ReadyCount, count)
	atomic.StoreInt64(&c.LastReadyCount, count)
	// inFlightCount := atomic.LoadInt64(&c.InFlightCount)
	if count != readyCount {
		c.ReadyStateChange <- 1
	}
}

func (c *ClientV2) FinishedMessage() {
	atomic.AddUint64(&c.FinishCount, 1)
	c.decrementInFlightCount()
}

func (c *ClientV2) decrementInFlightCount() {
	ready := atomic.LoadInt64(&c.ReadyCount)
	atomic.AddInt64(&c.InFlightCount, -1)
	if ready > 0 {
		// potentially push into the readyStateChange, as this could unblock the client
		c.ReadyStateChange <- 1
	}
}

func (c *ClientV2) SendingMessage() {
	atomic.AddInt64(&c.ReadyCount, -1)
	atomic.AddInt64(&c.InFlightCount, 1)
	atomic.AddUint64(&c.MessageCount, 1)
}

func (c *ClientV2) TimedOutMessage() {
	c.decrementInFlightCount()
}

func (c *ClientV2) RequeuedMessage() {
	atomic.AddUint64(&c.RequeueCount, 1)
	c.decrementInFlightCount()
}
