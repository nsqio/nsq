package protocol

import (
	"math"
	"testing"

	"github.com/nsqio/nsq/internal/test"
)

var result uint64

func TestByteToBase10(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected uint64
	}{
		{"zero", "0", 0},
		{"single digit", "7", 7},
		{"multiple digits", "31415925", 31415925},
		{"leading zeros", "007", 7},
		{"all zeros", "0000", 0},
		{"empty is zero", "", 0},
		{"max uint64", "18446744073709551615", math.MaxUint64},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, err := ByteToBase10([]byte(tt.input))
			test.Nil(t, err)
			test.Equal(t, tt.expected, n)
		})
	}
}

func TestByteToBase10Invalid(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"leading non-digit", "?1415925"},
		{"trailing non-digit", "1415925?"},
		{"embedded non-digit", "1415?925"},
		{"letters", "abc"},
		{"hex digits", "1f"},
		{"leading space", " 123"},
		{"trailing space", "123 "},
		{"negative sign", "-1"},
		{"positive sign", "+1"},
		{"decimal point", "1.5"},
		{"newline", "123\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, err := ByteToBase10([]byte(tt.input))
			test.NotNil(t, err)
			// the accumulator is reset on failure so callers never see a
			// partially parsed value
			test.Equal(t, uint64(0), n)
		})
	}
}

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
