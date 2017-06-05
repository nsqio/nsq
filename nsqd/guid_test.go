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
	var okays, errors, fails int64
	var previd guid
	factory := &guidFactory{}
	for i := 0; i < b.N; i++ {
		id, err := factory.NewGUID()
		if err != nil {
			errors++
		} else if id == previd {
			fails++
			b.Fail()
		} else {
			okays++
		}
		id.Hex()
	}
	b.Logf("okays=%d errors=%d bads=%d", okays, errors, fails)
}
