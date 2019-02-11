package nsqd

import (
	"encoding/binary"
	"io"

	"github.com/klauspost/crc32"
)

const EntryMagicV1 = 1

type Entry struct {
	Magic     byte
	Timestamp int64
	Deadline  int64
	Body      []byte
}

func NewEntry(body []byte, timestamp int64, deadline int64) Entry {
	return Entry{
		Magic:     EntryMagicV1,
		Timestamp: timestamp,
		Deadline:  deadline,
		Body:      body,
	}
}

func (e Entry) header() []byte {
	var buf [17]byte
	buf[0] = e.Magic
	binary.BigEndian.PutUint64(buf[1:9], uint64(e.Timestamp))
	binary.BigEndian.PutUint64(buf[9:17], uint64(e.Deadline))
	return buf[:]
}

func (e Entry) WriteTo(w io.Writer) (int64, error) {
	var total int64

	n, err := w.Write(e.header())
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

func (e Entry) CRC() uint32 {
	crc := crc32.ChecksumIEEE(e.header())
	return crc32.Update(crc, crc32.IEEETable, e.Body)
}

func (e Entry) Len() int64 {
	return int64(len(e.Body)) + 17
}

func DecodeWireEntry(data []byte) (Entry, error) {
	return Entry{
		Magic:     data[0],
		Timestamp: int64(binary.BigEndian.Uint64(data[1:9])),
		Deadline:  int64(binary.BigEndian.Uint64(data[9:17])),
		Body:      data[17:],
	}, nil
}
