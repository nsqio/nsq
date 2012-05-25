package nsq

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
)

type ProtocolClient struct {
	conn io.ReadWriteCloser
}

type ProtocolCommand struct {
	name   []byte
	params [][]byte
}

type ProtocolResponse struct {
	FrameType int32
	Data      interface{}
}

func (c *ProtocolClient) Connect(address string, port int) error {
	fqAddress := address + ":" + strconv.Itoa(port)
	conn, err := net.Dial("tcp", fqAddress)
	if err != nil {
		return err
	}
	c.conn = conn
	return nil
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
		_, err := fmt.Fprintf(c.conn, "%s %s\n", cmd.name, string(bytes.Join(cmd.params, []byte(" "))))
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

func (c *ProtocolClient) ReadResponse() (*ProtocolResponse, error) {
	var err error
	var msgSize int32
	var frameType int32

	// message size
	err = binary.Read(c.conn, binary.BigEndian, &msgSize)
	if err != nil {
		return nil, err
	}

	// frame type
	err = binary.Read(c.conn, binary.BigEndian, &frameType)
	if err != nil {
		return nil, err
	}

	// message binary data
	buf := make([]byte, msgSize-4)
	_, err = io.ReadFull(c.conn, buf)
	if err != nil {
		return nil, err
	}

	resp := ProtocolResponse{}
	resp.FrameType = frameType
	switch resp.FrameType {
	case FrameTypeMessage:
		resp.Data = NewMessage(buf)
		break
	default:
		resp.Data = buf
	}

	return &resp, nil
}
