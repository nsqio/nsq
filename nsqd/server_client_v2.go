package main

import (
	"../nsq"
	"sync/atomic"
)

type ServerClientV2 struct {
	*nsq.ServerClient
	State            int
	ReadyCount       int64
	LastReadyCount   int64
	InFlightCount    int64
	MessageCount     uint64
	FinishCount      uint64
	RequeueCount     uint64
	Channel          *Channel
	ReadyStateChange chan int
	ExitChan         chan int
}

func NewServerClientV2(client *nsq.ServerClient) *ServerClientV2 {
	return &ServerClientV2{
		ServerClient:     client,
		ReadyStateChange: make(chan int, 10),
		ExitChan:         make(chan int),
	}
}

func (c *ServerClientV2) Stats() ClientStats {
	return ClientStats{
		version:       "V2",
		name:          c.String(),
		state:         c.State,
		readyCount:    atomic.LoadInt64(&c.ReadyCount),
		inFlightCount: atomic.LoadInt64(&c.InFlightCount),
		messageCount:  atomic.LoadUint64(&c.MessageCount),
		finishCount:   atomic.LoadUint64(&c.FinishCount),
		requeueCount:  atomic.LoadUint64(&c.RequeueCount),
		connectTime:   c.ConnectTime,
	}
}

func (c *ServerClientV2) IsReadyForMessages() bool {
	readyCount := atomic.LoadInt64(&c.ReadyCount)
	lastReadyCount := atomic.LoadInt64(&c.LastReadyCount)
	inFlightCount := atomic.LoadInt64(&c.InFlightCount)
	// log.Printf("[%s] state rdy: %4d inflt: %4d", c.String(), readyCount, inFlightCount)

	if inFlightCount >= lastReadyCount || readyCount <= 0 {
		return false
	}

	return true
}

func (c *ServerClientV2) SetReadyCount(newCount int) {
	count := int64(newCount)
	readyCount := atomic.LoadInt64(&c.ReadyCount)
	lastReadyCount := atomic.LoadInt64(&c.LastReadyCount)
	atomic.StoreInt64(&c.ReadyCount, count)
	atomic.StoreInt64(&c.LastReadyCount, count)
	inFlightCount := atomic.LoadInt64(&c.InFlightCount)
	if count == 0 || readyCount == 0 || count > lastReadyCount || count > inFlightCount {
		c.ReadyStateChange <- 1
	}
}

func (c *ServerClientV2) FinishMessage() {
	atomic.AddUint64(&c.FinishCount, 1)
	c.decrementInFlightCount()
}

func (c *ServerClientV2) decrementInFlightCount() {
	ready := atomic.LoadInt64(&c.ReadyCount)
	inFlight := atomic.AddInt64(&c.InFlightCount, -1)
	if ready > 0 && inFlight == ready-1 {
		// potentially push into the readyStateChange, as this could unblock the client
		c.ReadyStateChange <- 1
	}
}

func (c *ServerClientV2) SendingMessage() {
	atomic.AddInt64(&c.ReadyCount, -1)
	atomic.AddInt64(&c.InFlightCount, 1)
	atomic.AddUint64(&c.MessageCount, 1)
}

func (c *ServerClientV2) TimedOutMessage() {
	c.decrementInFlightCount()
}

func (c *ServerClientV2) RequeuedMessage() {
	atomic.AddUint64(&c.RequeueCount, 1)
	c.decrementInFlightCount()
}
