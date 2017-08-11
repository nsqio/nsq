package nsqd

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/absolute8511/nsq/internal/ext"
)

const (
	MsgIDLength         = 16
	MsgTraceIDLength    = 8
	MsgJsonHeaderLength = 2
	minValidMsgLength   = MsgIDLength + 8 + 2 // Timestamp + Attempts
	maxAttempts         = 4000
	extMsgHighBits      = 0xa000
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
		m.pri, m.index, m.deferredCnt, m.Offset, m.RawMoveSize, m.queueCntIndex)
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
	Offset        BackendOffset
	RawMoveSize   BackendOffset
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

func (m *Message) WriteToClient(w io.Writer, writeExt bool, writeDetail bool) (int64, error) {
	return m.internalWriteTo(w, writeExt, writeDetail)
}

func (m *Message) WriteTo(w io.Writer, writeExt bool) (int64, error) {
	return m.internalWriteTo(w, writeExt, false)
}

func (m *Message) internalWriteTo(w io.Writer, writeExt bool, writeDetail bool) (int64, error) {
	var buf [16]byte
	var total int64

	binary.BigEndian.PutUint64(buf[:8], uint64(m.Timestamp))
	if m.Attempts > maxAttempts {
		m.Attempts = maxAttempts
	}
	combined := m.Attempts
	if writeExt {
		combined += uint16(extMsgHighBits)
	}
	binary.BigEndian.PutUint16(buf[8:10], combined)

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
		binary.BigEndian.PutUint64(buf[:8], uint64(m.Offset))
		binary.BigEndian.PutUint32(buf[8:8+4], uint32(m.RawMoveSize))
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
	if m.Attempts > maxAttempts {
		m.Attempts = maxAttempts
	}
	combined := m.Attempts
	if writeExt {
		combined += uint16(extMsgHighBits)
	}

	var buf [32]byte
	var total int64

	binary.BigEndian.PutUint64(buf[:8], uint64(m.Timestamp))
	binary.BigEndian.PutUint16(buf[8:10], combined)

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
			if len(m.ExtBytes) >= 65535 {
				return total, errors.New("extend data exceed the limit")
			}
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

	if m.DelayedType == ChannelDelayed {
		// some data with original queue info
		binary.BigEndian.PutUint64(buf[:8], uint64(m.Offset))
		binary.BigEndian.PutUint32(buf[8:8+4], uint32(m.RawMoveSize))
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
	combined := binary.BigEndian.Uint16(b[8:10])
	msg.ID = MessageID(binary.BigEndian.Uint64(b[10:18]))
	msg.TraceID = binary.BigEndian.Uint64(b[18:26])
	// we reused high 4-bits of attempts for message version to make it compatible with ext message and any other future change.
	// there some cases
	// 1. no ext topic, all high 4-bits should be 0
	// 2. ext topic, during update some old message has all high 4-bits with 0, and new message should equal 0xa for ext.
	// 3. do we need handle new message with ext but have all high 4-bits with 0?
	// 4. do we need handle some old attempts which maybe exceed the maxAttempts? (very small possible)
	// 5. if any future change, hight 4-bits can be 0xb, 0xc, 0xd, 0xe (0x1~0x9 should be reserved for future)
	var highBits uint16
	if combined > maxAttempts {
		highBits = combined & uint16(0xF000)
		msg.Attempts = combined & uint16(0x0FFF)
	} else {
		msg.Attempts = combined
	}

	bodyStart := 26
	if highBits == extMsgHighBits && !isExt {
		// may happened while upgrading and the channel read the new data while ext flag not setting
		return nil, fmt.Errorf("invalid message, has the ext high bits but not decode as ext data: %v", b)
	}

	if isExt && highBits == extMsgHighBits {
		if len(b) < 27 {
			return nil, fmt.Errorf("invalid message buffer size (%d)", len(b))
		}
		extVer := ext.ExtVer(uint8(b[bodyStart]))
		msg.ExtVer = extVer
		switch extVer {
		case ext.NO_EXT_VER:
			msg.ExtVer = ext.NO_EXT_VER
			bodyStart = 27
		default:
			if len(b) < 27+2 {
				return nil, fmt.Errorf("invalid message buffer size (%d)", len(b))
			}

			extLen := binary.BigEndian.Uint16(b[27 : 27+2])
			if len(b) < 27+2+int(extLen) {
				return nil, fmt.Errorf("invalid message buffer size (%d)", len(b))
			}
			msg.ExtBytes = b[29 : 29+extLen]
			bodyStart = 29 + int(extLen)
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
	combined := binary.BigEndian.Uint16(b[pos : pos+2])
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

	if len(b) < pos+int(nameLen) {
		return nil, fmt.Errorf("invalid delayed message buffer size (%d)", len(b))
	}

	msg.DelayedChannel = string(b[pos : pos+int(nameLen)])
	pos += int(nameLen)
	var highBits uint16
	if combined > maxAttempts {
		highBits = combined & uint16(0xF000)
		msg.Attempts = combined & uint16(0x0FFF)
	} else {
		msg.Attempts = combined
	}

	if highBits == extMsgHighBits && !isExt {
		return nil, fmt.Errorf("invalid message, has the ext high bits but not decode as ext data: %v", b)
	}

	if isExt && highBits == extMsgHighBits {
		if len(b) < pos+1 {
			return nil, fmt.Errorf("invalid delayed message buffer size (%d)", len(b))
		}

		extVer := ext.ExtVer(uint8(b[pos]))
		msg.ExtVer = extVer
		pos++
		switch extVer {
		case ext.NO_EXT_VER:
			msg.ExtVer = ext.NO_EXT_VER
		default:
			if len(b) < pos+2 {
				return nil, fmt.Errorf("invalid delayed message buffer size (%d)", len(b))
			}
			extLen := binary.BigEndian.Uint16(b[pos : pos+2])
			pos += 2
			if len(b) < pos+int(extLen) {
				return nil, fmt.Errorf("invalid delayed message buffer size (%d)", len(b))
			}
			msg.ExtBytes = b[pos : pos+int(extLen)]
			pos += int(extLen)
		}
	}

	if msg.DelayedType == ChannelDelayed {
		if len(b) < pos+8+4 {
			return nil, fmt.Errorf("invalid delayed message buffer size (%d)", len(b))
		}
		// some data with original queue info
		msg.Offset = BackendOffset(binary.BigEndian.Uint64(b[pos : pos+8]))
		pos += 8
		msg.RawMoveSize = BackendOffset(binary.BigEndian.Uint32(b[pos : pos+4]))
		pos += 4
	}
	msg.Body = b[pos:]
	return &msg, nil
}
