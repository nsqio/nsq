package nsqd

import (
	"encoding/binary"
	"io"

	"github.com/klauspost/crc32"
)

const EntryMagicV1 = 1

type Entry struct {
	Magic    byte
	Deadline int64
	Body     []byte
}

func NewEntry(body []byte, deadline int64) Entry {
	return Entry{
		Magic:    EntryMagicV1,
		Deadline: deadline,
		Body:     body,
	}
}

func (e Entry) header() []byte {
	var buf [9]byte
	buf[0] = e.Magic
	binary.BigEndian.PutUint64(buf[1:9], uint64(e.Deadline))
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
	return int64(len(e.Body)) + 9
}

func DecodeWireEntry(data []byte) (Entry, error) {
	return Entry{
		Magic:    data[0],
		Deadline: int64(binary.BigEndian.Uint64(data[1:9])),
		Body:     data[9:],
	}, nil
}
