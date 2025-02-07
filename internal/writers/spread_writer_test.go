package writers

import (
    "testing"
    "time"
)


// Test generated using Keploy
type MockWriter struct {
    WrittenData [][]byte
}

func (m *MockWriter) Write(p []byte) (n int, err error) {
    b := make([]byte, len(p))
    copy(b, p)
    m.WrittenData = append(m.WrittenData, b)
    return len(p), nil
}

func TestNewSpreadWriter_Initialization(t *testing.T) {
    exitCh := make(chan int)
    mockWriter := &MockWriter{}
    interval := time.Second
    spreadWriter := NewSpreadWriter(mockWriter, interval, exitCh)

    if spreadWriter.w != mockWriter {
        t.Errorf("Expected writer to be initialized")
    }
    if spreadWriter.interval != interval {
        t.Errorf("Expected interval to be initialized")
    }
    if spreadWriter.buf == nil || len(spreadWriter.buf) != 0 {
        t.Errorf("Expected buffer to be initialized and empty")
    }
    if spreadWriter.exitCh != exitCh {
        t.Errorf("Expected exitCh to be initialized")
    }
}

// Test generated using Keploy
func TestWrite_AppendsToBuffer(t *testing.T) {
    exitCh := make(chan int)
    mockWriter := &MockWriter{}
    spreadWriter := NewSpreadWriter(mockWriter, time.Second, exitCh)

    input := []byte("test data")
    n, err := spreadWriter.Write(input)

    if err != nil {
        t.Errorf("Expected no error, got %v", err)
    }
    if n != len(input) {
        t.Errorf("Expected %d bytes written, got %d", len(input), n)
    }
    if len(spreadWriter.buf) != 1 || string(spreadWriter.buf[0]) != "test data" {
        t.Errorf("Buffer does not contain the expected data")
    }
}


// Test generated using Keploy
func TestFlush_EmptyBuffer_WaitsInterval(t *testing.T) {
    exitCh := make(chan int)
    mockWriter := &MockWriter{}
    spreadWriter := NewSpreadWriter(mockWriter, 100*time.Millisecond, exitCh)

    start := time.Now()
    spreadWriter.Flush()
    elapsed := time.Since(start)

    if elapsed < 100*time.Millisecond {
        t.Errorf("Flush did not wait for the interval")
    }
    if len(mockWriter.WrittenData) != 0 {
        t.Errorf("Expected no data to be written")
    }
}


// Test generated using Keploy
func TestFlush_WritesAndClearsBuffer(t *testing.T) {
    exitCh := make(chan int)
    mockWriter := &MockWriter{}
    spreadWriter := NewSpreadWriter(mockWriter, time.Second, exitCh)

    spreadWriter.Write([]byte("data1"))
    spreadWriter.Write([]byte("data2"))

    spreadWriter.Flush()

    if len(mockWriter.WrittenData) != 2 {
        t.Errorf("Expected 2 writes, got %d", len(mockWriter.WrittenData))
    }
    if string(mockWriter.WrittenData[0]) != "data1" || string(mockWriter.WrittenData[1]) != "data2" {
        t.Errorf("Written data does not match expected values")
    }
    if len(spreadWriter.buf) != 0 {
        t.Errorf("Buffer was not cleared after flush")
    }
}

