package nsq

import (
	"bytes"
	"encoding/binary"
	"errors"
	"net"
	"strconv"
)

// Consumer is a low-level type for connecting/reading/writing to nsqd
type Consumer struct {
	*ProtocolClient
}

// NewConsumer creates a new Consumer, initializes
// the underlying ProtocolClient, and returns a pointer
func NewConsumer(tcpAddr *net.TCPAddr) *Consumer {
	return &Consumer{&ProtocolClient{tcpAddr: tcpAddr}}
}

// Subscribe creates a new ProtocolCommand to subscribe
// to the given topic/channel
func (c *Consumer) Subscribe(topic string, channel string) *ProtocolCommand {
	var params = [][]byte{[]byte(topic), []byte(channel)}
	return &ProtocolCommand{[]byte("SUB"), params}
}

// Ready creates a new ProtocolCommand to specify
// the number of messages a client is willing to receive
func (c *Consumer) Ready(count int) *ProtocolCommand {
	var params = [][]byte{[]byte(strconv.Itoa(count))}
	return &ProtocolCommand{[]byte("RDY"), params}
}

// Finish creates a new ProtocolCommand to indiciate that 
// a given message (by id) has been processed successfully
func (c *Consumer) Finish(id []byte) *ProtocolCommand {
	var params = [][]byte{id}
	return &ProtocolCommand{[]byte("FIN"), params}
}

// Requeue creats a new ProtocolCommand to indicate that 
// a given message (by id) should be requeued after the given timeout (in ms)
// NOTE: a timeout of 0 indicates immediate requeue
func (c *Consumer) Requeue(id []byte, timeoutMs int) *ProtocolCommand {
	var params = [][]byte{id, []byte(strconv.Itoa(timeoutMs))}
	return &ProtocolCommand{[]byte("REQ"), params}
}

// StartClose creates a new ProtocolCommand to indicate that the
// client would like to start a close cycle.  nsqd will no longer
// send messages to a client in this state and the client is expected
// to ACK after which it can finish pending messages and close the connection
func (c *Consumer) StartClose() *ProtocolCommand {
	return &ProtocolCommand{[]byte("CLS"), nil}
}

// UnpackResponse is a helper function that takes serialized data (as []byte), 
// unpackes, (optionally) decodes, and returns a triplicate of:
//    frame type, data (interface {}), error
func (c *Consumer) UnpackResponse(response []byte) (int32, interface{}, error) {
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
