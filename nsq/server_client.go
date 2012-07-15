package nsq

import (
	"encoding/binary"
	"log"
	"net"
)

type ServerClient struct {
	conn net.Conn
}

// ServerClient constructor
func NewServerClient(conn net.Conn) *ServerClient {
	return &ServerClient{
		conn: conn,
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
