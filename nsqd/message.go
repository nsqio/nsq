package nsqd

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"time"
)

const MsgIDLength = 16

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

func decodeMessage(b []byte) (*Message, error) {
	var msg Message

	if len(b) < 26 {
		return nil, fmt.Errorf("invalid message buffer size (%d)", len(b))
	}

	msg.Timestamp = int64(binary.BigEndian.Uint64(b[:8]))
	msg.Attempts = binary.BigEndian.Uint16(b[8:10])

	buf := bytes.NewBuffer(b[10:])

	_, err := io.ReadFull(buf, msg.ID[:])
	if err != nil {
		return nil, err
	}

	msg.Body, err = ioutil.ReadAll(buf)
	if err != nil {
		return nil, err
	}

	return &msg, nil
}

func writeMessageToBackend(buf *bytes.Buffer, msg *Message, bq BackendQueue) error {
	buf.Reset()
	_, err := msg.WriteTo(buf)
	if err != nil {
		return err
	}
	err = bq.Put(buf.Bytes())
	if err != nil {
		return err
	}
	return nil
}
