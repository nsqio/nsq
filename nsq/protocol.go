package nsq

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"regexp"
	"time"
)

var MagicV1 = []byte("  V1")
var MagicV2 = []byte("  V2")

// The maximum value a client can specify via RDY
const MaxReadyCount = 2500

const (
	// when successful
	FrameTypeResponse int32 = 0
	// when an error occurred
	FrameTypeError int32 = 1
	// when it's a serialized message
	FrameTypeMessage int32 = 2
)

// The amount of time nsqd will allow a client to idle, can be overriden
const DefaultClientTimeout = 60 * time.Second

var validTopicNameRegex = regexp.MustCompile(`^[\.a-zA-Z0-9_-]+$`)
var validChannelNameRegex = regexp.MustCompile(`^[\.a-zA-Z0-9_-]+(#ephemeral)?$`)

// IsValidTopicName checks a topic name for correctness
func IsValidTopicName(name string) bool {
	if len(name) > 32 || len(name) < 1 {
		return false
	}
	return validTopicNameRegex.MatchString(name)
}

// IsValidChannelName checks a channel name for correctness
func IsValidChannelName(name string) bool {
	if len(name) > 32 || len(name) < 1 {
		return false
	}
	return validChannelNameRegex.MatchString(name)
}

// Protocol describes the basic behavior of any protocol in the system
type Protocol interface {
	IOLoop(conn net.Conn) error
}

// ReadMagic is a server-side utility function to read the 4-byte magic id
// from the supplied Reader.
//
// The client should initialize itself by sending a 4 byte sequence indicating
// the version of the protocol that it intends to communicate, this will allow us 
// to gracefully upgrade the protocol away from text/line oriented to whatever...
func ReadMagic(r io.Reader) (int32, error) {
	var protocolMagic int32

	err := binary.Read(r, binary.BigEndian, &protocolMagic)
	if err != nil {
		return 0, err
	}

	return protocolMagic, nil
}

// SendResponse is a server side utility function to prefix data with a length header
// and write to the supplied Writer
func SendResponse(w io.Writer, data []byte) (int, error) {
	err := binary.Write(w, binary.BigEndian, int32(len(data)))
	if err != nil {
		return 0, err
	}

	n, err := w.Write(data)
	if err != nil {
		return 0, err
	}

	return (n + 4), nil
}

// ReadResponse is a client-side utility function to read from the supplied Reader
// according to the NSQ protocol spec:
//
//    [x][x][x][x][x][x][x][x]...
//    |  (int32) || (binary)
//    |  4-byte  || N-byte
//    ------------------------...
//        size       data
func ReadResponse(r io.Reader) ([]byte, error) {
	var msgSize int32

	// message size
	err := binary.Read(r, binary.BigEndian, &msgSize)
	if err != nil {
		return nil, err
	}

	// message binary data
	buf := make([]byte, msgSize)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

// DEPRECATED in 0.2.5, use: cmd.Write(w).
//
// SendCommand is a client-side utility function to serialize a command to the supplied Writer
func SendCommand(w io.Writer, cmd *Command) error {
	return cmd.Write(w)
}

// Frame is a server-side utility function to write the specified frameType 
// and data to the supplied Writer
func Frame(w io.Writer, frameType int32, data []byte) error {
	err := binary.Write(w, binary.BigEndian, &frameType)
	if err != nil {
		return err
	}

	_, err = w.Write(data)
	if err != nil {
		return err
	}

	return nil
}

// UnpackResponse is a client-side utility function that unpacks serialized data 
// according to NSQ protocol spec:
//
//    [x][x][x][x][x][x][x][x]...
//    |  (int32) || (binary)
//    |  4-byte  || N-byte
//    ------------------------...
//      frame ID     data
//
// Returns a triplicate of: frame type, data ([]byte), error
func UnpackResponse(response []byte) (int32, []byte, error) {
	var frameType int32

	if len(response) < 4 {
		return -1, nil, errors.New("length of response is too small")
	}

	// frame type
	buf := bytes.NewBuffer(response[:4])
	err := binary.Read(buf, binary.BigEndian, &frameType)
	if err != nil {
		return -1, nil, err
	}

	return frameType, response[4:], nil
}
