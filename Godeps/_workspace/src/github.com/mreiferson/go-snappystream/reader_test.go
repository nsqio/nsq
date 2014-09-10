package snappystream

import (
	"bytes"
	"crypto/rand"
	"io/ioutil"
	"testing"
)

// mkpad returns a padding block with n padding bytes (total length, n+8 bytes).
// practically useless but good enough for testing.
func mkpad(b byte, n int) []byte {
	if b == 0 {
		b = 0xfe
	}

	length := uint32(n + 4)
	lengthle := []byte{byte(length), byte(length >> 8), byte(length >> 16)}
	checksum := make([]byte, 4)
	_, err := rand.Read(checksum)
	if err != nil {
		panic(err)
	}
	padbytes := make([]byte, n)
	_, err = rand.Read(padbytes)
	if err != nil {
		panic(err)
	}

	var h []byte
	h = append(h, b)
	h = append(h, lengthle...)
	h = append(h, checksum...)
	h = append(h, padbytes...)
	return h
}

// This test checks that padding and reserved skippable blocks are ignored by
// the reader.
func TestReader_skippable(t *testing.T) {
	var buf bytes.Buffer
	// write some blocks with injected padding/skippable blocks
	w := NewWriter(&buf)
	write := func(p []byte) (int, error) {
		return w.Write(p)
	}
	writepad := func(b byte, n int) (int, error) {
		return buf.Write(mkpad(b, n))
	}
	_, err := write([]byte("hello"))
	if err != nil {
		t.Fatalf("write error: %v", err)
	}
	_, err = writepad(0xfe, 100) // normal padding
	if err != nil {
		t.Fatalf("write error: %v", err)
	}
	_, err = write([]byte(" "))
	if err != nil {
		t.Fatalf("write error: %v", err)
	}
	_, err = writepad(0xa0, 100) // reserved skippable block
	if err != nil {
		t.Fatalf("write error: %v", err)
	}
	_, err = writepad(0xfe, MaxBlockSize) // normal padding
	if err != nil {
		t.Fatalf("write error: %v", err)
	}
	_, err = write([]byte("padding"))
	if err != nil {
		t.Fatalf("write error: %v", err)
	}

	p, err := ioutil.ReadAll(NewReader(&buf, true))
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if string(p) != "hello padding" {
		t.Fatalf("read: unexpected content %q", string(p))
	}
}

// This test checks that reserved unskippable blocks are cause decoder errors.
func TestReader_unskippable(t *testing.T) {
	var buf bytes.Buffer
	// write some blocks with injected padding/skippable blocks
	w := NewWriter(&buf)
	write := func(p []byte) (int, error) {
		return w.Write(p)
	}
	writepad := func(b byte, n int) (int, error) {
		return buf.Write(mkpad(b, n))
	}
	_, err := write([]byte("unskippable"))
	if err != nil {
		t.Fatalf("write error: %v", err)
	}
	_, err = writepad(0x50, 100) // unskippable reserved block
	if err != nil {
		t.Fatalf("write error: %v", err)
	}
	_, err = write([]byte(" blocks"))
	if err != nil {
		t.Fatalf("write error: %v", err)
	}

	_, err = ioutil.ReadAll(NewReader(&buf, true))
	if err == nil {
		t.Fatalf("read success")
	}
}
