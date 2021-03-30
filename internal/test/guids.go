package test

import (
	"sync/atomic"
)

// GUIDFactory is an atomic sequence that can be used for MessageID's for benchmarks
// to avoid ErrSequenceExpired when creating a large number of messages
type GUIDFactory struct {
	n int64
}

func (gf *GUIDFactory) NextMessageID() int64 {
	return atomic.AddInt64(&gf.n, 1)
}
