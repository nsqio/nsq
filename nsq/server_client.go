package nsq

import (
	"encoding/binary"
	"log"
	"net"
	"sync/atomic"
)

type ServerClient struct {
	conn                 net.Conn
	State                int
	ReadyCount           int64
	LastReadyCount       int64
	ReadyStateChange     chan int
	ExitChan             chan int
	Channel              interface{}
	sm                   *interface{}
	InFlightMessageCount int64
}

// ServerClient constructor
func NewServerClient(conn net.Conn) *ServerClient {
	return &ServerClient{
		conn:             conn,
		ReadyStateChange: make(chan int, 10),
		ExitChan:         make(chan int),
	}
}

func (c *ServerClient) IsReadyForMessages() bool {
	readyCount := atomic.LoadInt64(&c.ReadyCount)
	lastReadyCount := atomic.LoadInt64(&c.LastReadyCount)
	inFlightMessageCount := atomic.LoadInt64(&c.InFlightMessageCount)
	log.Printf("[%s] state rdy: %4d inflt: %4d", c.String(), readyCount, inFlightMessageCount)

	if inFlightMessageCount >= lastReadyCount || readyCount <= 0 {
		return false
	}
	return true
}

func (c *ServerClient) SetReadyCount(count int) {
	readyCount := atomic.LoadInt64(&c.ReadyCount)
	lastReadyCount := atomic.LoadInt64(&c.LastReadyCount)
	atomic.StoreInt64(&c.ReadyCount, int64(count))
	atomic.StoreInt64(&c.LastReadyCount, int64(count))
	if readyCount == 0 || int64(count) > lastReadyCount {
		c.ReadyStateChange <- 1
	}
}

func (c *ServerClient) FinishMessage() {
	if atomic.AddInt64(&c.InFlightMessageCount, -1) <= 1 {
		// potentially push into the readyStateChange, as this could unblock the client
		c.ReadyStateChange <- 1
	}
}

func (c *ServerClient) SendingMessage() {
	atomic.AddInt64(&c.ReadyCount, -1)
	atomic.AddInt64(&c.InFlightMessageCount, 1)
}

func (c *ServerClient) TimedOutMessage() {
	if atomic.AddInt64(&c.InFlightMessageCount, -1) <= 1 {
		// potentially push into the readyStateChange, as this could unblock the client
		c.ReadyStateChange <- 1
	}
}

func (c *ServerClient) RequeuedMessage() {
	if atomic.AddInt64(&c.InFlightMessageCount, -1) <= 1 {
		// potentially push into the readyStateChange, as this could unblock the client
		c.ReadyStateChange <- 1
	}
}

func (c *ServerClient) String() string {
	return c.conn.RemoteAddr().String()
}

// Read proxies a read from `conn`
func (c *ServerClient) Read(data []byte) (int, error) {
	return c.conn.Read(data)
}

// Write prefixes the byte array with a size and 
// proxies the write to `conn`
func (c *ServerClient) Write(data []byte) (int, error) {
	var err error

	err = binary.Write(c.conn, binary.BigEndian, int32(len(data)))
	if err != nil {
		return 0, err
	}

	n, err := c.conn.Write(data)
	if err != nil {
		return 0, err
	}

	return (n + 4), nil
}

// Close proxies the call to `conn`
func (c *ServerClient) Close() {
	log.Printf("CLIENT(%s): closing", c.String())
	c.conn.Close()
}

// Handle reads data from the client, keeps state, and
// responds.  It is executed in a goroutine.
func (c *ServerClient) Handle(protocols map[int32]Protocol) {
	var err error
	var protocolVersion int32

	defer c.Close()

	// the client should initialize itself by sending a 4 byte sequence indicating
	// the version of the protocol that it intends to communicate, this will allow us 
	// to gracefully upgrade the protocol away from text/line oriented to whatever...
	err = binary.Read(c.conn, binary.BigEndian, &protocolVersion)
	if err != nil {
		log.Printf("ERROR: client(%s) failed to read protocol version", c.String())
		return
	}

	log.Printf("CLIENT(%s): desired protocol %d", c.String(), protocolVersion)

	prot, ok := protocols[protocolVersion]
	if !ok {
		c.Write([]byte(ClientErrBadProtocol.Error()))
		log.Printf("ERROR: client(%s) bad protocol version %d", c.String(), protocolVersion)
		return
	}

	err = prot.IOLoop(c)
	if err != nil {
		log.Printf("ERROR: client(%s) - %s", c.String(), err.Error())
		return
	}
}
