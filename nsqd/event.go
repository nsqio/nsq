package nsqd

import (
	"encoding/binary"
	"io"
	"time"
)

type EventAttribute struct {
	ID     uint8
	Length uint8
	Value  []byte
}

const DeadlineAttributeID = 0

func NewDeadlineAttribute(v time.Time) EventAttribute {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(v.UnixNano()))
	return EventAttribute{
		ID:    DeadlineAttributeID,
		Value: buf[:],
	}
}

type Event struct {
	Attributes []EventAttribute
	Body       []byte
}

func NewEvent(body []byte, attrs ...EventAttribute) Event {
	return Event{
		Attributes: attrs,
		Body:       body,
	}
}

func (e Event) WriteTo(w io.Writer) (int64, error) {
	var buf [256]byte
	var total int64

	buf[0] = byte(len(e.Attributes))
	idx := 1
	for _, attr := range e.Attributes {
		buf[idx] = byte(attr.ID)
		idx++
		buf[idx] = byte(len(attr.Value))
		idx++
		copy(buf[idx:idx+len(attr.Value)], attr.Value)
		idx += len(attr.Value)
	}

	n, err := w.Write(buf[:idx])
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(e.Body)
	total += int64(n)
	if err != nil {
		return total, err
	}

	return total, err
}
