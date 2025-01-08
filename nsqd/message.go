package nsqd

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

const (
	MsgIDLength       = 16
	minValidMsgLength = MsgIDLength + 8 + 2 // Timestamp + Attempts
)

var deferMsgMagicFlag = []byte("#DEFER_MSG#")

type MessageID [MsgIDLength]byte

type Message struct {
	ID        MessageID
	Body      []byte
	Timestamp int64
	Attempts  uint16

	// for in-flight handling
	deliveryTS time.Time
	clientID   int64
	pri        int64
	index      int
	deferred   time.Duration
	deadline   int64
}

func NewMessage(id MessageID, body []byte) *Message {
	return &Message{
		ID:        id,
		Body:      body,
		Timestamp: time.Now().UnixNano(),
	}
}

func (m *Message) WriteTo(w io.Writer) (int64, error) {
	var buf [10]byte
	var total int64

	binary.BigEndian.PutUint64(buf[:8], uint64(m.Timestamp))
	binary.BigEndian.PutUint16(buf[8:10], uint16(m.Attempts))

	n, err := w.Write(buf[:])
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(m.ID[:])
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(m.Body)
	total += int64(n)
	if err != nil {
		return total, err
	}

	if m.deadline > time.Now().UnixNano() {
		n, err = w.Write(deferMsgMagicFlag)
		total += int64(n)
		if err != nil {
			return total, err
		}

		var deferBuf [8]byte
		binary.BigEndian.PutUint64(deferBuf[:8], uint64(m.deadline))

		n, err := w.Write(deferBuf[:])
		total += int64(n)
		if err != nil {
			return total, err
		}
	}

	return total, nil
}

// decodeMessage deserializes data (as []byte) and creates a new Message
//
//	[x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x]... [x][x][x][x][x][x][x][x]
//	|       (int64)        ||    ||      (hex string encoded in ASCII)           || (binary)     ||       (int64)
//	|       8-byte         ||    ||                 16-byte                      || N-byte       ||       8-byte
//	------------------------------------------------------------------------------------------... ------------------------
//	  nanosecond timestamp    ^^                   message ID                       message body     nanosecond deadline
//	                       (uint16)
//	                        2-byte
//	                       attempts
func decodeMessage(b []byte) (*Message, error) {
	var msg Message

	if len(b) < minValidMsgLength {
		return nil, fmt.Errorf("invalid message buffer size (%d)", len(b))
	}

	msg.Timestamp = int64(binary.BigEndian.Uint64(b[:8]))
	msg.Attempts = binary.BigEndian.Uint16(b[8:10])
	copy(msg.ID[:], b[10:10+MsgIDLength])

	if bytes.Equal(b[len(b)-8-len(deferMsgMagicFlag):len(b)-8], deferMsgMagicFlag) {
		msg.deadline = int64(binary.BigEndian.Uint64(b[len(b)-8:]))
		if deferred := msg.deadline - time.Now().UnixNano(); deferred > 0 {
			msg.deferred = time.Duration(deferred)
		}
		msg.Body = b[10+MsgIDLength : len(b)-8-len(deferMsgMagicFlag)]
	} else {
		msg.Body = b[10+MsgIDLength:]
	}

	return &msg, nil
}

func writeMessageToBackend(msg *Message, bq BackendQueue) error {
	buf := bufferPoolGet()
	defer bufferPoolPut(buf)
	_, err := msg.WriteTo(buf)
	if err != nil {
		return err
	}
	return bq.Put(buf.Bytes())
}
