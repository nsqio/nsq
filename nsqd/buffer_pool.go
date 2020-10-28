package nsqd

import (
	"bytes"
	"sync"
)

var bp sync.Pool

func init() {
	bp.New = func() interface{} {
		return &bytes.Buffer{}
	}
}

func bufferPoolGet() *bytes.Buffer {
	return bp.Get().(*bytes.Buffer)
}

func bufferPoolPut(b *bytes.Buffer) {
	b.Reset()
	bp.Put(b)
}
