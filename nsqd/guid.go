package nsqd

// the core algorithm here was borrowed from:
// Blake Mizerany's `noeqd` https://github.com/bmizerany/noeqd
// and indirectly:
// Twitter's `snowflake` https://github.com/twitter/snowflake

// only minor cleanup and changes to introduce a type, combine the concept
// of workerId + datacenterId into a single identifier, and modify the
// behavior when sequences rollover for our specific implementation needs

import (
	"encoding/hex"
	"errors"
	"time"
)

const (
	workerIdBits   = uint64(10)
	sequenceBits   = uint64(12)
	workerIdShift  = sequenceBits
	timestampShift = sequenceBits + workerIdBits
	sequenceMask   = int64(-1) ^ (int64(-1) << sequenceBits)

	// Tue, 21 Mar 2006 20:50:14.000 GMT
	twepoch = int64(1288834974657)
)

var ErrTimeBackwards = errors.New("time has gone backwards")
var ErrSequenceExpired = errors.New("sequence expired")

type guid int64

type guidFactory struct {
	sequence      int64
	lastTimestamp int64
}

func (f *guidFactory) NewGUID(workerId int64) (guid, error) {
	ts := time.Now().UnixNano() / 1e6

	if ts < f.lastTimestamp {
		return 0, ErrTimeBackwards
	}

	if f.lastTimestamp == ts {
		f.sequence = (f.sequence + 1) & sequenceMask
		if f.sequence == 0 {
			return 0, ErrSequenceExpired
		}
	} else {
		f.sequence = 0
	}

	f.lastTimestamp = ts

	id := ((ts - twepoch) << timestampShift) |
		(workerId << workerIdShift) |
		f.sequence

	return guid(id), nil
}

func (g guid) Hex() MessageID {
	var h MessageID
	var b [8]byte

	b[0] = byte(g >> 56)
	b[1] = byte(g >> 48)
	b[2] = byte(g >> 40)
	b[3] = byte(g >> 32)
	b[4] = byte(g >> 24)
	b[5] = byte(g >> 16)
	b[6] = byte(g >> 8)
	b[7] = byte(g)

	hex.Encode(h[:], b[:])
	return h
}
