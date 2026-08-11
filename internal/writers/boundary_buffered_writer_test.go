package writers

import (
    "bytes"
    "io"
    "testing"
)


// Test generated using Keploy
func TestNewBoundaryBufferedWriter_Initialization(t *testing.T) {
    var writer io.Writer = &bytes.Buffer{}
    size := 1024
    b := NewBoundaryBufferedWriter(writer, size)

    if b == nil {
        t.Fatalf("Expected BoundaryBufferedWriter to be initialized, got nil")
    }

    if b.bw.Size() != size {
        t.Errorf("Expected buffer size %d, got %d", size, b.bw.Size())
    }
}

// Test generated using Keploy
func TestBoundaryBufferedWriter_Write_BufferFlush(t *testing.T) {
    var buf bytes.Buffer
    writer := NewBoundaryBufferedWriter(&buf, 10)

    data := []byte("12345678901") // Exceeds buffer size of 10
    n, err := writer.Write(data)

    if err != nil {
        t.Fatalf("Expected no error, got %v", err)
    }

    if n != len(data) {
        t.Errorf("Expected %d bytes written, got %d", len(data), n)
    }

    if buf.String() != string(data) {
        t.Errorf("Expected buffer content %q, got %q", string(data), buf.String())
    }
}


// Test generated using Keploy
func TestBoundaryBufferedWriter_Flush(t *testing.T) {
    var buf bytes.Buffer
    writer := NewBoundaryBufferedWriter(&buf, 10)

    data := []byte("12345")
    _, err := writer.Write(data)
    if err != nil {
        t.Fatalf("Expected no error, got %v", err)
    }

    err = writer.Flush()
    if err != nil {
        t.Fatalf("Expected no error on flush, got %v", err)
    }

    if buf.String() != string(data) {
        t.Errorf("Expected buffer content %q, got %q", string(data), buf.String())
    }
}

