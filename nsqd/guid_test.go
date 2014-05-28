package nsqd

import (
	"testing"
	"unsafe"
)

func BenchmarkGUIDCopy(b *testing.B) {
	source := make([]byte, 16)
	var dest MessageID
	for i := 0; i < b.N; i++ {
		copy(dest[:], source)
	}
}

func BenchmarkGUIDUnsafe(b *testing.B) {
	source := make([]byte, 16)
	var dest MessageID
	for i := 0; i < b.N; i++ {
		dest = *(*MessageID)(unsafe.Pointer(&source[0]))
	}
	_ = dest
}

func BenchmarkGUID(b *testing.B) {
	factory := &guidFactory{}
	for i := 0; i < b.N; i++ {
		guid, err := factory.NewGUID(0)
		if err != nil {
			continue
		}
		guid.Hex()
	}
}
