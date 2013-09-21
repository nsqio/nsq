package main

import (
	"bufio"
	"compress/flate"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/bitly/go-nsq"
	"github.com/mreiferson/go-snappystream"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const DefaultBufferSize = 16 * 1024

type IdentifyDataV2 struct {
	ShortId             string `json:"short_id"`
	LongId              string `json:"long_id"`
	HeartbeatInterval   int    `json:"heartbeat_interval"`
	OutputBufferSize    int    `json:"output_buffer_size"`
	OutputBufferTimeout int    `json:"output_buffer_timeout"`
	FeatureNegotiation  bool   `json:"feature_negotiation"`
	TLSv1               bool   `json:"tls_v1"`
	Deflate             bool   `json:"deflate"`
	DeflateLevel        int    `json:"deflate_level"`
	Snappy              bool   `json:"snappy"`
}

type ClientV2 struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	ReadyCount     int64
	LastReadyCount int64
	InFlightCount  int64
	MessageCount   uint64
	FinishCount    uint64
	RequeueCount   uint64

	sync.Mutex

	ID      int64
	context *Context

	// original connection
	net.Conn

	// connections based on negotiated features
	tlsConn     *tls.Conn
	flateWriter *flate.Writer

	// reading/writing interfaces
	Reader *bufio.Reader
	Writer *bufio.Writer

	// output buffering
	OutputBufferSize              int
	OutputBufferTimeout           *time.Ticker
	OutputBufferTimeoutUpdateChan chan time.Duration

	State           int32
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

func NewClientV2(id int64, conn net.Conn, context *Context) *ClientV2 {
	var identifier string
	if conn != nil {
		identifier, _, _ = net.SplitHostPort(conn.RemoteAddr().String())
	}

	c := &ClientV2{
		ID:      id,
		context: context,

		Conn: conn,

		Reader: bufio.NewReaderSize(conn, DefaultBufferSize),
		Writer: bufio.NewWriterSize(conn, DefaultBufferSize),

		OutputBufferSize:              DefaultBufferSize,
		OutputBufferTimeout:           time.NewTicker(250 * time.Millisecond),
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
		Heartbeat:           time.NewTicker(context.nsqd.options.clientTimeout / 2),
		HeartbeatInterval:   context.nsqd.options.clientTimeout / 2,
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
		desiredInterval <= int(c.context.nsqd.options.maxHeartbeatInterval/time.Millisecond):
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
	case desiredSize >= 64 && desiredSize <= int(c.context.nsqd.options.maxOutputBufferSize):
		size = desiredSize
	default:
		return errors.New(fmt.Sprintf("output buffer size (%d) is invalid", desiredSize))
	}

	if size > 0 {
		err := c.Writer.Flush()
		if err != nil {
			return err
		}
		c.OutputBufferSize = size
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
	case desiredTimeout >= 1 &&
		desiredTimeout <= int(c.context.nsqd.options.maxOutputBufferTimeout/time.Millisecond):
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

func (c *ClientV2) UpgradeTLS() error {
	c.Lock()
	defer c.Unlock()

	tlsConn := tls.Server(c.Conn, c.context.nsqd.tlsConfig)
	err := tlsConn.Handshake()
	if err != nil {
		return err
	}
	c.tlsConn = tlsConn

	c.Reader = bufio.NewReaderSize(c.tlsConn, DefaultBufferSize)
	c.Writer = bufio.NewWriterSize(c.tlsConn, c.OutputBufferSize)

	return nil
}

func (c *ClientV2) UpgradeDeflate(level int) error {
	c.Lock()
	defer c.Unlock()

	conn := c.Conn
	if c.tlsConn != nil {
		conn = c.tlsConn
	}

	c.Reader = bufio.NewReaderSize(flate.NewReader(conn), DefaultBufferSize)

	fw, _ := flate.NewWriter(conn, level)
	c.flateWriter = fw
	c.Writer = bufio.NewWriterSize(fw, c.OutputBufferSize)

	return nil
}

func (c *ClientV2) UpgradeSnappy() error {
	c.Lock()
	defer c.Unlock()

	conn := c.Conn
	if c.tlsConn != nil {
		conn = c.tlsConn
	}

	c.Reader = bufio.NewReaderSize(snappystream.NewReader(conn, snappystream.SkipVerifyChecksum), DefaultBufferSize)
	c.Writer = bufio.NewWriterSize(snappystream.NewWriter(conn), c.OutputBufferSize)

	return nil
}

func (c *ClientV2) Flush() error {
	c.SetWriteDeadline(time.Now().Add(time.Second))

	err := c.Writer.Flush()
	if err != nil {
		return err
	}

	if c.flateWriter != nil {
		return c.flateWriter.Flush()
	}

	return nil
}
