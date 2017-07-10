package nsqd

import (
	"encoding/binary"
	"fmt"
	"github.com/absolute8511/nsq/internal/ext"
	"io"
	"sync/atomic"
	"time"
)

const (
	MsgIDLength       = 16
	MsgTraceIDLength  = 8
	minValidMsgLength = MsgIDLength + 8 + 2 // Timestamp + Attempts
)

// the new message total id will be ID+TraceID, the length is same with old id
// slice, the traceid only used for trace for business, the ID is used for internal.
// In order to be compatible with old format, we keep the attempts field.
type FullMessageID [MsgIDLength]byte
type MessageID uint64

func GetMessageIDFromFullMsgID(id FullMessageID) MessageID {
	return MessageID(binary.BigEndian.Uint64(id[:8]))
}

func GetTraceIDFromFullMsgID(id FullMessageID) uint64 {
	return binary.BigEndian.Uint64(id[8 : 8+MsgTraceIDLength])
}

func PrintMessage(m *Message) string {
	return fmt.Sprintf("%v %v %s %v %v %v %v %v %v %v %v %v",
		m.ID, m.TraceID, string(m.Body), m.Timestamp, m.Attempts, m.deliveryTS,
		m.pri, m.index, m.deferredCnt, m.offset, m.rawMoveSize, m.queueCntIndex)
}

type Message struct {
	ID        MessageID
	TraceID   uint64
	Body      []byte
	Timestamp int64
	Attempts  uint16
	ExtBytes  []byte
	ExtVer    ext.ExtVer

	// for in-flight handling
	deliveryTS time.Time
	//clientID    int64
	belongedConsumer Consumer
	pri              int64
	index            int
	deferredCnt      int32
	//for backend queue
	offset        BackendOffset
	rawMoveSize   BackendOffset
	queueCntIndex int64
	// for delayed queue message
	// 1 - delayed message by channel
	// 2 - delayed pub
	// 3 - uncommitted transaction
	//
	DelayedType    int32
	DelayedTs      int64
	DelayedOrigID  MessageID
	DelayedChannel string
}

func MessageHeaderBytes() int {
	return MsgIDLength + 8 + 2
}

func NewMessageWithExt(id MessageID, body []byte, extVer ext.ExtVer, extBytes []byte) *Message {
	return &Message{
		ID:        id,
		TraceID:   0,
		ExtVer:    extVer,
		ExtBytes:  extBytes,
		Body:      body,
		Timestamp: time.Now().UnixNano(),
	}
}

func NewMessage(id MessageID, body []byte) *Message {
	return &Message{
		ID:        id,
		TraceID:   0,
		Body:      body,
		Timestamp: time.Now().UnixNano(),
	}
}

func NewMessageWithTs(id MessageID, body []byte, ts int64) *Message {
	return &Message{
		ID:        id,
		TraceID:   0,
		Body:      body,
		Timestamp: ts,
	}
}

func (m *Message) GetFullMsgID() FullMessageID {
	var buf FullMessageID
	binary.BigEndian.PutUint64(buf[:8], uint64(m.ID))
	binary.BigEndian.PutUint64(buf[8:8+MsgTraceIDLength], uint64(m.TraceID))
	return buf
}

func (m *Message) IsDeferred() bool {
	return atomic.LoadInt32(&m.deferredCnt) > 0
}

func (m *Message) GetCopy() *Message {
	newMsg := *m
	newMsg.Body = make([]byte, len(m.Body))
	copy(newMsg.Body, m.Body)
	return &newMsg
}

func (m *Message) GetClientID() int64 {
	if m.belongedConsumer != nil {
		return m.belongedConsumer.GetID()
	}
	return 0
}

func (m *Message) WriteToWithDetail(w io.Writer, writeExt bool) (int64, error) {
	return m.internalWriteTo(w, writeExt, true)
}

func (m *Message) WriteTo(w io.Writer, writeExt bool) (int64, error) {
	return m.internalWriteTo(w, writeExt, false)
}

func (m *Message) internalWriteTo(w io.Writer, writeExt bool, writeDetail bool) (int64, error) {
	var buf [16]byte
	var total int64

	binary.BigEndian.PutUint64(buf[:8], uint64(m.Timestamp))
	binary.BigEndian.PutUint16(buf[8:10], m.Attempts)

	n, err := w.Write(buf[:10])
	total += int64(n)
	if err != nil {
		return total, err
	}

	binary.BigEndian.PutUint64(buf[:8], uint64(m.ID))
	binary.BigEndian.PutUint64(buf[8:8+MsgTraceIDLength], uint64(m.TraceID))
	n, err = w.Write(buf[:MsgIDLength])
	total += int64(n)
	if err != nil {
		return total, err
	}

	//write ext content
	if writeExt {
		//write ext version
		buf[0] = byte(m.ExtVer)
		n, err = w.Write(buf[:1])
		total += int64(n)
		if err != nil {
			return total, err
		}

		if m.ExtVer != ext.NO_EXT_VER {
			binary.BigEndian.PutUint16(buf[1:1+2], uint16(len(m.ExtBytes)))
			n, err = w.Write(buf[1 : 1+2])
			total += int64(n)
			if err != nil {
				return total, err
			}

			n, err = w.Write(m.ExtBytes)
			total += int64(n)
			if err != nil {
				return total, err
			}
		}
	}

	if writeDetail {
		binary.BigEndian.PutUint64(buf[:8], uint64(m.offset))
		binary.BigEndian.PutUint32(buf[8:8+4], uint32(m.rawMoveSize))
		n, err = w.Write(buf[:8+4])
		total += int64(n)
		if err != nil {
			return total, err
		}
	}
	n, err = w.Write(m.Body)
	total += int64(n)
	if err != nil {
		return total, err
	}

	return total, nil
}

func (m *Message) WriteDelayedTo(w io.Writer, writeExt bool) (int64, error) {
	var buf [32]byte
	var total int64

	binary.BigEndian.PutUint64(buf[:8], uint64(m.Timestamp))
	binary.BigEndian.PutUint16(buf[8:10], m.Attempts)

	n, err := w.Write(buf[:10])
	total += int64(n)
	if err != nil {
		return total, err
	}

	binary.BigEndian.PutUint64(buf[:8], uint64(m.ID))
	binary.BigEndian.PutUint64(buf[8:8+MsgTraceIDLength], uint64(m.TraceID))
	n, err = w.Write(buf[:MsgIDLength])
	total += int64(n)
	if err != nil {
		return total, err
	}

	pos := 0
	binary.BigEndian.PutUint32(buf[pos:pos+4], uint32(m.DelayedType))
	pos += 4
	binary.BigEndian.PutUint64(buf[pos:pos+8], uint64(m.DelayedTs))
	pos += 8
	binary.BigEndian.PutUint64(buf[pos:pos+8], uint64(m.DelayedOrigID))
	pos += 8
	binary.BigEndian.PutUint32(buf[pos:pos+4], uint32(len(m.DelayedChannel)))
	pos += 4
	n, err = w.Write(buf[:pos])
	total += int64(n)
	if err != nil {
		return total, err
	}
	n, err = w.Write([]byte(m.DelayedChannel))
	total += int64(n)
	if err != nil {
		return total, err
	}

	//write ext content
	if writeExt {
		//write ext version
		buf[0] = byte(m.ExtVer)
		n, err = w.Write(buf[:1])
		total += int64(n)
		if err != nil {
			return total, err
		}

		if m.ExtVer != ext.NO_EXT_VER {
			binary.BigEndian.PutUint16(buf[:2], uint16(len(m.ExtBytes)))
			n, err = w.Write(buf[:2])
			total += int64(n)
			if err != nil {
				return total, err
			}
			n, err = w.Write(m.ExtBytes)
			total += int64(n)
			if err != nil {
				return total, err
			}
		}
	}

	n, err = w.Write(m.Body)
	total += int64(n)
	if err != nil {
		return total, err
	}

	return total, nil
}

func DecodeMessage(b []byte, ext bool) (*Message, error) {
	return decodeMessage(b, ext)
}

// note: the message body is using the origin buffer, so never modify the buffer after decode.
// decodeMessage deserializes data (as []byte) and creates a new Message
// message format:
// [x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x]...
// |       (int64)        ||    ||      (hex string encoded in ASCII)           || (binary)
// |       8-byte         ||    ||                 16-byte                      || N-byte
// ------------------------------------------------------------------------------------------...
//   nanosecond timestamp    ^^                   message ID                       message body
//                        (uint16)
//                         2-byte
//                        attempts
func decodeMessage(b []byte, isExt bool) (*Message, error) {
	var msg Message

	if len(b) < minValidMsgLength {
		return nil, fmt.Errorf("invalid message buffer size (%d)", len(b))
	}

	msg.Timestamp = int64(binary.BigEndian.Uint64(b[:8]))
	msg.Attempts = binary.BigEndian.Uint16(b[8:10])
	msg.ID = MessageID(binary.BigEndian.Uint64(b[10:18]))
	msg.TraceID = binary.BigEndian.Uint64(b[18:26])

	bodyStart := 26

	if isExt {
		extVer := ext.ExtVer(uint8(b[bodyStart]))
		switch extVer {
		case ext.TAG_EXT_VER:
			tagLen := binary.BigEndian.Uint16(b[27 : 27+2])
			msg.ExtVer = ext.TAG_EXT_VER
			msg.ExtBytes = b[29 : 29+tagLen]
			bodyStart = 29 + int(tagLen)
		case ext.NO_EXT_VER:
			msg.ExtVer = ext.NO_EXT_VER
			bodyStart = 27
		default:
			return nil, fmt.Errorf("invalid ext version %v", extVer)
		}
	}

	msg.Body = b[bodyStart:]
	return &msg, nil
}

func DecodeDelayedMessage(b []byte, isExt bool) (*Message, error) {
	var msg Message

	if len(b) < minValidMsgLength {
		return nil, fmt.Errorf("invalid message buffer size (%d)", len(b))
	}

	pos := 0
	msg.Timestamp = int64(binary.BigEndian.Uint64(b[pos:8]))
	pos += 8
	msg.Attempts = binary.BigEndian.Uint16(b[pos : pos+2])
	pos += 2
	msg.ID = MessageID(binary.BigEndian.Uint64(b[pos : pos+8]))
	pos += 8
	msg.TraceID = binary.BigEndian.Uint64(b[pos : pos+8])
	pos += 8

	if len(b) < minValidMsgLength+4+8+8+4 {
		return nil, fmt.Errorf("invalid delayed message buffer size (%d)", len(b))
	}

	msg.DelayedType = int32(binary.BigEndian.Uint32(b[pos : pos+4]))
	pos += 4
	msg.DelayedTs = int64(binary.BigEndian.Uint64(b[pos : pos+8]))
	pos += 8
	msg.DelayedOrigID = MessageID(binary.BigEndian.Uint64(b[pos : pos+8]))
	pos += 8
	nameLen := binary.BigEndian.Uint32(b[pos : pos+4])
	pos += 4

	if len(b) < minValidMsgLength+8+4+int(nameLen) {
		return nil, fmt.Errorf("invalid delayed message buffer size (%d)", len(b))
	}

	msg.DelayedChannel = string(b[pos : pos+int(nameLen)])
	pos += int(nameLen)

	if isExt {
		if len(b) <= pos {
			return nil, fmt.Errorf("invalid delayed message buffer size (%d)", len(b))
		}

		extVer := ext.ExtVer(uint8(b[pos]))
		pos++
		switch extVer {
		case ext.TAG_EXT_VER:
			tagLen := binary.BigEndian.Uint16(b[pos : pos+2])
			pos += 2
			msg.ExtVer = ext.TAG_EXT_VER
			msg.ExtBytes = b[pos : pos+int(tagLen)]
			pos += int(tagLen)
		case ext.NO_EXT_VER:
			msg.ExtVer = ext.NO_EXT_VER
		default:
			return nil, fmt.Errorf("invalid ext version %v", extVer)
		}
	}

	msg.Body = b[pos:]
	return &msg, nil
}
