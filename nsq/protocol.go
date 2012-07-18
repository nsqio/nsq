package nsq

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

var MagicV1 = []byte("  V1")
var MagicV2 = []byte("  V2")

const (
	FrameTypeResponse int32 = 0
	FrameTypeError    int32 = 1
	FrameTypeMessage  int32 = 2
)

// describes the basic behavior of any protocol in the system
type Protocol interface {
	IOLoop(conn net.Conn) error
}

func ReadMagic(r io.Reader) (int32, error) {
	var protocolMagic int32

	// the client should initialize itself by sending a 4 byte sequence indicating
	// the version of the protocol that it intends to communicate, this will allow us 
	// to gracefully upgrade the protocol away from text/line oriented to whatever...
	err := binary.Read(r, binary.BigEndian, &protocolMagic)
	if err != nil {
		return 0, err
	}

	return protocolMagic, nil
}

func SendResponse(w io.Writer, data []byte) (int, error) {
	var err error

	err = binary.Write(w, binary.BigEndian, int32(len(data)))
	if err != nil {
		return 0, err
	}

	n, err := w.Write(data)
	if err != nil {
		return 0, err
	}

	return (n + 4), nil
}

func ReadResponse(r io.Reader) ([]byte, error) {
	var err error
	var msgSize int32

	// message size
	err = binary.Read(r, binary.BigEndian, &msgSize)
	if err != nil {
		return nil, err
	}

	// message binary data
	buf := make([]byte, msgSize)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return nil, err
	}

	return buf, err
}

func SendCommand(w io.Writer, cmd *Command) error {
	if len(cmd.Params) > 0 {
		_, err := fmt.Fprintf(w, "%s %s\n", cmd.Name, bytes.Join(cmd.Params, []byte(" ")))
		if err != nil {
			return err
		}
	} else {
		_, err := fmt.Fprintf(w, "%s\n", cmd.Name)
		if err != nil {
			return err
		}
	}
	return nil
}

func Frame(frameType int32, data []byte) ([]byte, error) {
	var buf bytes.Buffer
	var err error

	err = binary.Write(&buf, binary.BigEndian, &frameType)
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(data)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// UnpackResponse is a helper function that takes serialized data (as []byte), 
// unpacks and returns a triplicate of:
//    frame type, data ([]byte), error
func UnpackResponse(response []byte) (int32, []byte, error) {
	var err error
	var frameType int32

	if len(response) < 4 {
		return -1, nil, ClientErrInvalid
	}

	// frame type
	buf := bytes.NewBuffer(response[:4])
	err = binary.Read(buf, binary.BigEndian, &frameType)
	if err != nil {
		return -1, nil, err
	}

	return frameType, response[4:], nil
}
