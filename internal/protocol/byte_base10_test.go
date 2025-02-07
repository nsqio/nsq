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

// Test generated using Keploy
func TestByteToBase10ValidInput(t *testing.T) {
    input := []byte{'1', '2', '3', '4', '5'}
    expected := uint64(12345)
    result, err := ByteToBase10(input)
    if err != nil {
        t.Errorf("Expected no error, got %v", err)
    }
    if result != expected {
        t.Errorf("Expected %v, got %v", expected, result)
    }
}


// Test generated using Keploy
func TestByteToBase10InvalidInput(t *testing.T) {
    input := []byte{'1', '2', 'a', '4', '5'}
    expected := uint64(0)
    result, err := ByteToBase10(input)
    if err == nil {
        t.Errorf("Expected an error, got nil")
    }
    if result != expected {
        t.Errorf("Expected %v, got %v", expected, result)
    }
}

