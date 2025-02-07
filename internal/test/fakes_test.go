package test

import (
    "testing"
    "net"
    "time"
    "fmt"
)


// Test generated using Keploy
func TestFakeNetConn_Read(t *testing.T) {
    expectedData := []byte("test data")
    expectedLen := len(expectedData)
    expectedErr := error(nil)

    fakeConn := FakeNetConn{
        ReadFunc: func(b []byte) (int, error) {
            copy(b, expectedData)
            return expectedLen, expectedErr
        },
    }

    buffer := make([]byte, 10)
    n, err := fakeConn.Read(buffer)

    if n != expectedLen {
        t.Errorf("Expected length %d, got %d", expectedLen, n)
    }
    if err != expectedErr {
        t.Errorf("Expected error %v, got %v", expectedErr, err)
    }
    if string(buffer[:n]) != string(expectedData) {
        t.Errorf("Expected data %s, got %s", expectedData, buffer[:n])
    }
}

// Test generated using Keploy
func TestFakeNetConn_Write(t *testing.T) {
    inputData := []byte("test data")
    expectedLen := len(inputData)
    expectedErr := error(nil)

    fakeConn := FakeNetConn{
        WriteFunc: func(b []byte) (int, error) {
            if string(b) != string(inputData) {
                t.Errorf("Expected data %s, got %s", inputData, b)
            }
            return expectedLen, expectedErr
        },
    }

    n, err := fakeConn.Write(inputData)

    if n != expectedLen {
        t.Errorf("Expected length %d, got %d", expectedLen, n)
    }
    if err != expectedErr {
        t.Errorf("Expected error %v, got %v", expectedErr, err)
    }
}


// Test generated using Keploy
func TestFakeNetConn_Close(t *testing.T) {
    expectedErr := error(nil)

    fakeConn := FakeNetConn{
        CloseFunc: func() error {
            return expectedErr
        },
    }

    err := fakeConn.Close()

    if err != expectedErr {
        t.Errorf("Expected error %v, got %v", expectedErr, err)
    }
}


// Test generated using Keploy
func TestFakeNetConn_LocalAddr(t *testing.T) {
    expectedAddr := fakeNetAddr{}

    fakeConn := FakeNetConn{
        LocalAddrFunc: func() net.Addr {
            return expectedAddr
        },
    }

    addr := fakeConn.LocalAddr()

    if addr != expectedAddr {
        t.Errorf("Expected address %v, got %v", expectedAddr, addr)
    }
}


// Test generated using Keploy
func TestFakeNetConn_RemoteAddr(t *testing.T) {
    expectedAddr := fakeNetAddr{}

    fakeConn := FakeNetConn{
        RemoteAddrFunc: func() net.Addr {
            return expectedAddr
        },
    }

    addr := fakeConn.RemoteAddr()

    if addr != expectedAddr {
        t.Errorf("Expected address %v, got %v", expectedAddr, addr)
    }
}


// Test generated using Keploy
func TestFakeNetConn_SetDeadline(t *testing.T) {
    expectedErr := error(nil)
    expectedTime := time.Now()

    fakeConn := FakeNetConn{
        SetDeadlineFunc: func(t time.Time) error {
            if !t.Equal(expectedTime) {
                return fmt.Errorf("Expected time %v, got %v", expectedTime, t)
            }
            return expectedErr
        },
    }

    // Test with valid time
    err := fakeConn.SetDeadline(expectedTime)
    if err != expectedErr {
        t.Errorf("Expected error %v, got %v", expectedErr, err)
    }

    // Test with zero time (invalid case)
    zeroTime := time.Time{}
    err = fakeConn.SetDeadline(zeroTime)
    if err == nil {
        t.Errorf("Expected an error for zero time, got nil")
    }
}


// Test generated using Keploy
func TestFakeNetConn_SetReadDeadline(t *testing.T) {
    expectedErr := error(nil)
    expectedTime := time.Now()

    fakeConn := FakeNetConn{
        SetReadDeadlineFunc: func(t time.Time) error {
            if !t.Equal(expectedTime) {
                return fmt.Errorf("Expected time %v, got %v", expectedTime, t)
            }
            return expectedErr
        },
    }

    // Test with valid time
    err := fakeConn.SetReadDeadline(expectedTime)
    if err != expectedErr {
        t.Errorf("Expected error %v, got %v", expectedErr, err)
    }

    // Test with zero time (invalid case)
    zeroTime := time.Time{}
    err = fakeConn.SetReadDeadline(zeroTime)
    if err == nil {
        t.Errorf("Expected an error for zero time, got nil")
    }
}


// Test generated using Keploy
func TestFakeNetConn_SetWriteDeadline(t *testing.T) {
    expectedErr := error(nil)
    expectedTime := time.Now()

    fakeConn := FakeNetConn{
        SetWriteDeadlineFunc: func(t time.Time) error {
            if !t.Equal(expectedTime) {
                return fmt.Errorf("Expected time %v, got %v", expectedTime, t)
            }
            return expectedErr
        },
    }

    // Test with valid time
    err := fakeConn.SetWriteDeadline(expectedTime)
    if err != expectedErr {
        t.Errorf("Expected error %v, got %v", expectedErr, err)
    }

    // Test with zero time (invalid case)
    zeroTime := time.Time{}
    err = fakeConn.SetWriteDeadline(zeroTime)
    if err == nil {
        t.Errorf("Expected an error for zero time, got nil")
    }
}

