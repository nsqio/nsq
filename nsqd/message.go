package nsqd

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/vmihailenco/msgpack"
)

var (
	// First 4 bytes picked from hex representation of a day before epoch timestamp which should never exist in normal timestamp.
	// python3 -c 'import struct; import datetime; print(struct.pack(">Q", int((datetime.datetime(1990, 1, 1).timestamp() - 60*60*24) * 10**9)))'
	msgMagic = []byte{0x08, 0xc1, 0xe4, 0xa0}

	metaKey               = []byte("meta")
	bodyKey               = []byte("body")
	metaLengthPlaceholder = []byte("01")       // 2 bytes
	bodyLengthPlaceholder = []byte("01234567") // 8 bytes. Because `MaxMsgSize` is in int64 type.

	// No const or reference directly are mainly used for unit test.
	maxMetaLen uint16 = math.MaxUint16
)

const (
	MsgIDLength       = 16
	minValidMsgLength = 4 + 4 + 2 + 4 + 8                  // msgMagic + metaKey + metaLen + bodyKey + bodyLen
	maxValidMsgLength = minValidMsgLength + math.MaxUint16 // minValidMsgLength + maxMetaLength
)

type MessageID [MsgIDLength]byte

type Message struct {
	ID        MessageID `msgpack:"message_id"`
	Body      []byte    `msgpack:"-"`
	Timestamp int64     `msgpack:"timestamp"`
	Attempts  uint16    `msgpack:"attempts"`
	AbsTs     int64     `msgpack:"abs_ts"`

	// for in-flight handling
	deliveryTS time.Time
	clientID   int64
	pri        int64
	index      int
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
	return total, nil
}

func (m *Message) WriteToBackend(w io.Writer) (int64, error) {
	var total int64

	// magic bytes
	n, err := w.Write(msgMagic)
	total += int64(n)
	if err != nil {
		return total, err
	}

	// meta bytes
	meta, err := msgpack.Marshal(m)
	if err != nil {
		return total, err
	}

	if len(meta) > int(maxMetaLen) {
		return total, errors.New("marshaled meta data length exceeds max meta length")
	}

	var metaPrefix = append(metaKey, metaLengthPlaceholder...)
	binary.BigEndian.PutUint16(metaPrefix[len(metaKey):len(metaKey)+len(metaLengthPlaceholder)], uint16(len(meta)))

	n, err = w.Write(metaPrefix[:])
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(meta)
	total += int64(n)
	if err != nil {
		return total, err
	}

	// msg body
	bodyPrefix := append(bodyKey, bodyLengthPlaceholder...)
	binary.BigEndian.PutUint64(bodyPrefix[len(bodyKey):len(bodyKey)+len(bodyLengthPlaceholder)], uint64(len(m.Body)))

	n, err = w.Write(bodyPrefix[:])
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(m.Body)
	total += int64(n)
	if err != nil {
		return total, err
	}

	return total, nil
}

// decodeMessage deserializes data (as []byte) and creates a new Message
// message format:
//
// Old message format:
// [x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x]...
// |       (int64)        ||    ||      (hex string encoded in ASCII)           || (binary)
// |       8-byte         ||    ||                 16-byte                      || N-byte
// ------------------------------------------------------------------------------------------...
//   nanosecond timestamp    ^^                   message ID                       message body
//                        (uint16)
//                         2-byte
//                        attempts
//
// New message format:
// [x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x]...
// | ([]byte)||      (metaKey+metaLen+meta)      ||         (bodyKey+bodyLen+body)
// |  4-byte ||           (4+2+N)-byte           ||              (4+8+N)-byte
// ------------------------------------------------------------------------------------------...
// message magic          message meta                           message body
//
func decodeMessage(b []byte) (*Message, error) {
	var msg Message

	prefixBytes := b[:len(msgMagic)]
	if bytes.Equal(prefixBytes, msgMagic) {
		// New message format
		metaStartIndex := len(msgMagic)
		if !bytes.Equal(b[metaStartIndex:metaStartIndex+len(metaKey)], metaKey) {
			return nil, fmt.Errorf("bad msg format. \"meta\" key should be after msg magic")
		}

		metaSize := binary.BigEndian.Uint16(b[metaStartIndex+len(metaKey) : metaStartIndex+len(metaKey)+len(metaLengthPlaceholder)])
		err := msgpack.Unmarshal(b[metaStartIndex+len(metaKey)+len(metaLengthPlaceholder):metaStartIndex+len(metaKey)+len(metaLengthPlaceholder)+int(metaSize)], &msg)
		if err != nil {
			return nil, err
		}

		bodyStartIndex := metaStartIndex + len(bodyKey) + len(metaLengthPlaceholder) + int(metaSize)
		if !bytes.Equal(b[bodyStartIndex:bodyStartIndex+len(bodyKey)], bodyKey) {
			return nil, fmt.Errorf("bad msg format. \"body\" key should be after meta content")
		}
		bodySize := binary.BigEndian.Uint64(b[bodyStartIndex+len(bodyKey) : bodyStartIndex+len(bodyKey)+len(bodyLengthPlaceholder)])
		msg.Body = b[bodyStartIndex+len(bodyKey)+len(bodyLengthPlaceholder) : uint64(bodyStartIndex+len(bodyKey)+len(bodyLengthPlaceholder))+bodySize]
	} else {
		// Old message format
		msg.Timestamp = int64(binary.BigEndian.Uint64(b[:8]))
		msg.Attempts = binary.BigEndian.Uint16(b[8:10])
		copy(msg.ID[:], b[10:10+MsgIDLength])
		msg.Body = b[10+MsgIDLength:]
	}

	return &msg, nil
}

func writeMessageToBackend(buf *bytes.Buffer, msg *Message, bq BackendQueue) error {
	buf.Reset()
	_, err := msg.WriteToBackend(buf)
	if err != nil {
		return err
	}
	return bq.Put(buf.Bytes())
}
