package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/nsqio/nsq/internal/test"
)

var errWrite = errors.New("write failed")

// failingWriter accepts okBytes bytes in total and then fails every write.
type failingWriter struct {
	okBytes int
	written int
}

func (w *failingWriter) Write(p []byte) (int, error) {
	remaining := w.okBytes - w.written
	if remaining <= 0 {
		return 0, errWrite
	}
	if len(p) > remaining {
		w.written += remaining
		return remaining, errWrite
	}
	w.written += len(p)
	return len(p), nil
}

func TestSendResponse(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"simple", []byte("OK")},
		{"empty", []byte("")},
		{"binary", []byte{0x00, 0xff, 0x10}},
		{"large", bytes.Repeat([]byte("a"), 1024)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			n, err := SendResponse(&buf, tt.data)

			test.Nil(t, err)
			// 4 byte big endian length header + payload
			test.Equal(t, len(tt.data)+4, n)
			test.Equal(t, len(tt.data)+4, buf.Len())

			body := buf.Bytes()
			test.Equal(t, uint32(len(tt.data)), binary.BigEndian.Uint32(body[:4]))
			test.Equal(t, tt.data, body[4:])
		})
	}
}

func TestSendResponseHeaderWriteError(t *testing.T) {
	w := &failingWriter{okBytes: 0}
	n, err := SendResponse(w, []byte("OK"))

	test.NotNil(t, err)
	test.Equal(t, 0, n)
}

func TestSendResponseBodyWriteError(t *testing.T) {
	// accept the 4 byte header, then fail on the payload
	w := &failingWriter{okBytes: 4}
	n, err := SendResponse(w, []byte("OK"))

	test.Equal(t, errWrite, err)
	test.Equal(t, 0, n)
}

func TestSendFramedResponse(t *testing.T) {
	tests := []struct {
		name      string
		frameType int32
		data      []byte
	}{
		{"response frame", 0, []byte("OK")},
		{"error frame", 1, []byte("E_INVALID invalid")},
		{"message frame", 2, []byte{0x01, 0x02, 0x03}},
		{"empty payload", 0, []byte("")},
		{"large payload", 2, bytes.Repeat([]byte("a"), 1024)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			n, err := SendFramedResponse(&buf, tt.frameType, tt.data)

			test.Nil(t, err)
			// 4 byte size + 4 byte frame type + payload
			test.Equal(t, len(tt.data)+8, n)
			test.Equal(t, len(tt.data)+8, buf.Len())

			body := buf.Bytes()
			// the advertised size covers the frame type and the payload
			test.Equal(t, uint32(len(tt.data)+4), binary.BigEndian.Uint32(body[:4]))
			test.Equal(t, uint32(tt.frameType), binary.BigEndian.Uint32(body[4:8]))
			test.Equal(t, tt.data, body[8:])
		})
	}
}

func TestSendFramedResponseSizeWriteError(t *testing.T) {
	w := &failingWriter{okBytes: 0}
	n, err := SendFramedResponse(w, 0, []byte("OK"))

	test.Equal(t, errWrite, err)
	test.Equal(t, 0, n)
}

func TestSendFramedResponseFrameTypeWriteError(t *testing.T) {
	// accept the 4 byte size header, then fail on the frame type
	w := &failingWriter{okBytes: 4}
	n, err := SendFramedResponse(w, 0, []byte("OK"))

	test.Equal(t, errWrite, err)
	test.Equal(t, 4, n)
}

func TestSendFramedResponseBodyWriteError(t *testing.T) {
	// accept size and frame type headers, then fail on the payload
	w := &failingWriter{okBytes: 8}
	n, err := SendFramedResponse(w, 0, []byte("OK"))

	test.Equal(t, errWrite, err)
	test.Equal(t, 8, n)
}
