package main

import (
	"github.com/bitly/go-nsq"
	"testing"
	"unsafe"
)

func BenchmarkGUIDCopy(b *testing.B) {
	source := make([]byte, 16)
	var dest nsq.MessageID
	for i := 0; i < b.N; i++ {
		copy(dest[:], source)
	}
}

func BenchmarkGUIDUnsafe(b *testing.B) {
	source := make([]byte, 16)
	var dest nsq.MessageID
	for i := 0; i < b.N; i++ {
		dest = *(*nsq.MessageID)(unsafe.Pointer(&source[0]))
	}
	_ = dest
}
