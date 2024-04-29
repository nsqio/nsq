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
	var previd guid
	factory := NewGUIDFactory(37)
	for i := 0; i < b.N; i++ {
		id, err := factory.NewGUID()
		if err != nil {
			b.Fatal(err)
		} else if id <= previd {
			b.Fatal("repeated or descending id")
		}
		previd = id
		id.Hex()
	}
}

func TestGUID(t *testing.T) {
	factory := NewGUIDFactory(1)
	var previd guid
	for i := 0; i < 1000; i++ {
		id, err := factory.NewGUID()
		if err != nil {
			t.Fatal(err)
		} else if id <= previd {
			t.Fatal("repeated or descending id")
		}
		previd = id
	}
}
