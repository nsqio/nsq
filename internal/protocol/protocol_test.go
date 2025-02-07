package protocol

import (
    "bytes"
    "testing"
    "io"
)


// Test generated using Keploy
func TestSendResponse_HappyPath(t *testing.T) {
    data := []byte("test data")
    buf := &bytes.Buffer{}

    n, err := SendResponse(buf, data)
    if err != nil {
        t.Fatalf("Expected no error, got %v", err)
    }

    expectedLength := len(data) + 4
    if n != expectedLength {
        t.Errorf("Expected %d bytes written, got %d", expectedLength, n)
    }

    expectedOutput := append([]byte{0, 0, 0, byte(len(data))}, data...)
    if !bytes.Equal(buf.Bytes(), expectedOutput) {
        t.Errorf("Expected output %v, got %v", expectedOutput, buf.Bytes())
    }
}

// Test generated using Keploy
type FailingWriter struct{}

func (f *FailingWriter) Write(p []byte) (n int, err error) {
    return 0, io.ErrClosedPipe
}

func TestSendResponse_WriteLengthError(t *testing.T) {
    data := []byte("test data")
    writer := &FailingWriter{}

    n, err := SendResponse(writer, data)
    if err == nil {
        t.Fatalf("Expected an error, got nil")
    }

    if n != 0 {
        t.Errorf("Expected 0 bytes written, got %d", n)
    }
}


// Test generated using Keploy
func TestSendFramedResponse_HappyPath(t *testing.T) {
    data := []byte("test data")
    frameType := int32(1)
    buf := &bytes.Buffer{}

    n, err := SendFramedResponse(buf, frameType, data)
    if err != nil {
        t.Fatalf("Expected no error, got %v", err)
    }

    expectedLength := len(data) + 8
    if n != expectedLength {
        t.Errorf("Expected %d bytes written, got %d", expectedLength, n)
    }

    expectedOutput := append([]byte{0, 0, 0, byte(len(data) + 4)}, []byte{0, 0, 0, byte(frameType)}...)
    expectedOutput = append(expectedOutput, data...)
    if !bytes.Equal(buf.Bytes(), expectedOutput) {
        t.Errorf("Expected output %v, got %v", expectedOutput, buf.Bytes())
    }
}


// Test generated using Keploy
type PartialHeaderWriter struct {
    writeCount int
}

func (p *PartialHeaderWriter) Write(b []byte) (int, error) {
    if p.writeCount == 0 {
        p.writeCount++
        return len(b) / 2, nil
    }
    return 0, io.ErrClosedPipe
}

func TestSendFramedResponse_PartialFrameHeaderWrite(t *testing.T) {
    data := []byte("test data")
    frameType := int32(1)
    writer := &PartialHeaderWriter{}

    n, err := SendFramedResponse(writer, frameType, data)
    if err == nil {
        t.Fatalf("Expected an error, got nil")
    }

    if n != len(data)/2 {
        t.Errorf("Expected %d bytes written, got %d", len(data)/2, n)
    }
}


// Test generated using Keploy
type AlwaysErrorWriter struct{}

func (a *AlwaysErrorWriter) Write(p []byte) (n int, err error) {
    return 0, io.ErrClosedPipe
}

func TestSendFramedResponse_ErrorWriter(t *testing.T) {
    data := []byte("test data")
    frameType := int32(1)
    writer := &AlwaysErrorWriter{}

    n, err := SendFramedResponse(writer, frameType, data)
    if err == nil {
        t.Fatalf("Expected an error, got nil")
    }

    if n != 0 {
        t.Errorf("Expected 0 bytes written, got %d", n)
    }
}

