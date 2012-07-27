package main

import (
	"../nsq"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type ClientV2 struct {
	net.Conn
	sync.Mutex
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
}

func NewClientV2(conn net.Conn) *ClientV2 {
	var identifier string
	if conn != nil {
		identifier, _, _ = net.SplitHostPort(conn.RemoteAddr().String())
	}
	return &ClientV2{
		net.Conn:        conn,
		ReadyStateChan:  make(chan int, 1),
		ExitChan:        make(chan int),
		ConnectTime:     time.Now(),
		ShortIdentifier: identifier,
		LongIdentifier:  identifier,
	}
}

func (c *ClientV2) Stats() ClientStats {
	return ClientStats{
		version:       "V2",
		address:       c.RemoteAddr().String(),
		name:          c.ShortIdentifier,
		state:         atomic.LoadInt32(&c.State),
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
	atomic.StoreInt64(&c.ReadyCount, count)
	atomic.StoreInt64(&c.LastReadyCount, count)

	if count != readyCount {
		select {
		case c.ReadyStateChan <- 1:
		case <-c.ExitChan:
		}
	}
}

func (c *ClientV2) FinishedMessage() {
	atomic.AddUint64(&c.FinishCount, 1)
	c.decrementInFlightCount()
}

func (c *ClientV2) decrementInFlightCount() {
	atomic.AddInt64(&c.InFlightCount, -1)
	if atomic.LoadInt64(&c.ReadyCount) > 0 {
		// potentially push into the readyStateChange, as this could unblock the client
		select {
		case c.ReadyStateChan <- 1:
		case <-c.ExitChan:
		}
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

func (c *ClientV2) Exit() {
	c.StartClose()
}

func (c *ClientV2) StartClose() {
	// Force the client into ready 0
	c.SetReadyCount(0)
	// mark this client as closing
	atomic.StoreInt32(&c.State, nsq.StateClosing)
	// TODO: start a timer to actually close the channel (in case the client doesn't do it first)
}
