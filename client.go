package main

import (
	"encoding/binary"
	"log"
	"net"
)

type Client struct {
	conn    net.Conn
	state   int
	channel *Channel
}

const (
	clientInit         = 0
	clientWaitGet      = 1
	clientWaitAck      = 2
	clientWaitResponse = 3
)

type ClientError struct {
	errStr string
}

func (e ClientError) Error() string {
	return e.errStr
}

var (
	clientErrInvalid     = ClientError{"E_INVALID"}
	clientErrBadProtocol = ClientError{"E_BAD_PROTOCOL"}
	clientErrBadTopic    = ClientError{"E_BAD_TOPIC"}
	clientErrBadChannel  = ClientError{"E_BAD_CHANNEL"}
	clientErrBadMessage  = ClientError{"E_BAD_MESSAGE"}
)

// Client constructor
func NewClient(conn net.Conn) *Client {
	return &Client{conn, clientInit, nil}
}

func (c *Client) String() string {
	return c.conn.RemoteAddr().String()
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

	protocol, ok := protocols[protocolVersion]
	if !ok {
		c.WriteError(clientErrBadProtocol)
		log.Printf("CLIENT(%s): bad protocol version %d", c.String(), protocolVersion)
		return
	}

	err = protocol.IOLoop(c)
	if err != nil {
		log.Printf("ERROR: client(%s) - %s", c.String(), err.Error())
		return
	}
}

func (c *Client) Close() {
	log.Printf("CLIENT(%s): closing", c.String())
	if c.channel != nil {
		c.channel.RemoveClient(c)
	}
	c.conn.Close()
}
