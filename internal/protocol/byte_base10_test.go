package protocol

import (
	"testing"
)

var result uint64

func BenchmarkByteToBase10Valid(b *testing.B) {
	bt := []byte{'3', '1', '4', '1', '5', '9', '2', '5'}
	var n uint64
	for i := 0; i < b.N; i++ {
		n, _ = ByteToBase10(bt)
	}
	result = n
}

func BenchmarkByteToBase10Invalid(b *testing.B) {
	bt := []byte{'?', '1', '4', '1', '5', '9', '2', '5'}
	var n uint64
	for i := 0; i < b.N; i++ {
		n, _ = ByteToBase10(bt)
	}
	result = n
}
