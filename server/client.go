package server

import (
	"../protocol"
	"encoding/binary"
	"log"
	"net"
)

type Client struct {
	conn  net.Conn
	state int
}

// Client constructor
func NewClient(conn net.Conn) *Client {
	return &Client{conn, -1}
}

func (c *Client) String() string {
	if c.conn == nil {
		return "<nil>"
	}
	return c.conn.RemoteAddr().String()
}

func (c *Client) GetState() int {
	return c.state
}

func (c *Client) SetState(state int) {
	c.state = state
}

func (c *Client) GetConnection() net.Conn {
	return c.conn
}

// Write prefixes the byte array with a size and 
// sends it to the Client
func (c *Client) Write(data []byte) error {
	var err error

	err = binary.Write(c.conn, binary.BigEndian, int32(len(data)))
	if err != nil {
		return err
	}

	_, err = c.conn.Write(data)
	if err != nil {
		return err
	}

	return nil
}

// WriteError is a convenience function to send
// an error string
func (c *Client) WriteError(err error) error {
	return c.Write([]byte(err.Error()))
}

// Handle reads data from the client, keeps state, and
// responds.  It is executed in a goroutine.
func (c *Client) Handle() {
	var err error
	var protocolVersion int32

	defer c.Close()

	// the client should initialize itself by sending a 4 byte sequence indicating
	// the version of the protocol that it intends to communicate, this will allow us 
	// to gracefully upgrade the protocol away from text/line oriented to whatever...
	err = binary.Read(c.conn, binary.BigEndian, &protocolVersion)
	if err != nil {
		log.Printf("CLIENT(%s): failed to read protocol version", c.String())
		return
	}

	log.Printf("CLIENT(%s): desired protocol %d", c.String(), protocolVersion)

	prot, ok := protocol.Protocols[protocolVersion]
	if !ok {
		c.WriteError(protocol.ClientErrBadProtocol)
		log.Printf("CLIENT(%s): bad protocol version %d", c.String(), protocolVersion)
		return
	}

	err = prot.IOLoop(c)
	if err != nil {
		log.Printf("ERROR: client(%s) - %s", c.String(), err.Error())
		return
	}
}

func (c *Client) Close() {
	log.Printf("CLIENT(%s): closing", c.String())
	c.conn.Close()
}
