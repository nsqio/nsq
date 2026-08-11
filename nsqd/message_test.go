package nsqd

import (
    "bytes"
    "testing"
)


// Test generated using Keploy
func TestMessage_WriteTo(t *testing.T) {
    id := MessageID{}
    body := []byte("test body")
    msg := NewMessage(id, body)
    msg.Attempts = 5

    var buf bytes.Buffer
    n, err := msg.WriteTo(&buf)
    if err != nil {
        t.Fatalf("WriteTo failed: %v", err)
    }

    expectedSize := int64(10 + len(id) + len(body)) // 10 bytes for Timestamp + Attempts, 16 bytes for ID, len(body) for Body
    if n != expectedSize {
        t.Errorf("Expected written size %v, got %v", expectedSize, n)
    }

    writtenData := buf.Bytes()
    if len(writtenData) != int(expectedSize) {
        t.Errorf("Expected buffer size %v, got %v", expectedSize, len(writtenData))
    }
}
