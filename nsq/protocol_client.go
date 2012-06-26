package nsq

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"
)

const (
	stateDisconnected = 0
	stateConnected    = 1
)

type ProtocolClient struct {
	conn    io.ReadWriteCloser
	state   int
	tcpAddr *net.TCPAddr
}

type ProtocolCommand struct {
	name   []byte
	params [][]byte
}

func (c *ProtocolClient) String() string {
	return c.tcpAddr.String()
}

func (c *ProtocolClient) Connect() error {
	conn, err := net.DialTimeout("tcp", c.tcpAddr.String(), time.Second)
	if err != nil {
		return err
	}
	c.conn = conn
	c.state = stateConnected
	return nil
}

func (c *ProtocolClient) IsConnected() bool {
	return c.state == stateConnected
}

func (c *ProtocolClient) Close() {
	c.conn.Close()
	c.state = stateDisconnected
}

func (c *ProtocolClient) Read(data []byte) (int, error) {
	return c.conn.Read(data)
}

func (c *ProtocolClient) Version(version string) error {
	_, err := c.Write([]byte(version))
	return err
}

func (c *ProtocolClient) Write(data []byte) (int, error) {
	return c.conn.Write(data)
}

func (c *ProtocolClient) WriteCommand(cmd *ProtocolCommand) error {
	if len(cmd.params) > 0 {
		_, err := fmt.Fprintf(c.conn, "%s %s\n", cmd.name, bytes.Join(cmd.params, []byte(" ")))
		if err != nil {
			return err
		}
	} else {
		_, err := fmt.Fprintf(c.conn, "%s\n", cmd.name)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *ProtocolClient) ReadResponse() ([]byte, error) {
	var err error
	var msgSize int32

	// message size
	err = binary.Read(c.conn, binary.BigEndian, &msgSize)
	if err != nil {
		return nil, err
	}

	// message binary data
	buf := make([]byte, msgSize)
	_, err = io.ReadFull(c.conn, buf)
	if err != nil {
		return nil, err
	}

	return buf, err
}
