// +build !go1.3

package nsqd

import (
	"bytes"
)

func bufferPoolGet() *bytes.Buffer {
	return &bytes.Buffer{}
}

func bufferPoolPut(b *bytes.Buffer) {}
