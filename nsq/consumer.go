package nsq

import (
	"bytes"
	"encoding/binary"
	"errors"
	"log"
	"net"
	"strconv"
)

type Consumer struct {
	*ProtocolClient
}

func NewConsumer(tcpAddr *net.TCPAddr) *Consumer {
	return &Consumer{&ProtocolClient{tcpAddr: tcpAddr}}
}

func (c *ProtocolClient) Subscribe(topic string, channel string) *ProtocolCommand {
	var params = [][]byte{[]byte(topic), []byte(channel)}
	return &ProtocolCommand{[]byte("SUB"), params}
}

func (c *ProtocolClient) Ready(count int) *ProtocolCommand {
	var params = [][]byte{[]byte(strconv.Itoa(count))}
	return &ProtocolCommand{[]byte("RDY"), params}
}

func (c *ProtocolClient) Finish(uuid string) *ProtocolCommand {
	var params = [][]byte{[]byte(uuid)}
	return &ProtocolCommand{[]byte("FIN"), params}
}

func (c *ProtocolClient) Requeue(uuid string) *ProtocolCommand {
	var params = [][]byte{[]byte(uuid)}
	return &ProtocolCommand{[]byte("REQ"), params}
}

func (c *ProtocolClient) StartClose() *ProtocolCommand {
	// start a close cycle. server will ACK after which we can finish up 
	// pending messages, and then close the connection
	log.Printf("starting CLS on client")
	return &ProtocolCommand{[]byte("CLS"), nil}
}

func (c *ProtocolClient) UnpackResponse(response []byte) (int32, interface{}, error) {
	var err error
	var frameType int32
	var ret interface{}

	if len(response) < 4 {
		return -1, nil, errors.New("response invalid")
	}

	// frame type
	buf := bytes.NewBuffer(response[:4])
	err = binary.Read(buf, binary.BigEndian, &frameType)
	if err != nil {
		return -1, nil, err
	}

	switch frameType {
	case FrameTypeMessage:
		ret, err = DecodeMessage(response[4:])
		if err != nil {
			return -1, nil, err
		}
		break
	default:
		ret = response[4:]
	}

	return frameType, ret, nil
}
