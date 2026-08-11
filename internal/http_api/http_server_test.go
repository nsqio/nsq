package http_api

import (
    "testing"
    "github.com/nsqio/nsq/internal/lg"
    "fmt"
    "net"
    "net/http"
    "time"
)


// Test generated using Keploy
func TestLogWriter_Write(t *testing.T) {
    var loggedLevel lg.LogLevel
    var loggedFormat string
    var loggedArgs []interface{}

    mockLogFunc := func(lvl lg.LogLevel, f string, args ...interface{}) {
        loggedLevel = lvl
        loggedFormat = f
        loggedArgs = args
    }

    lw := logWriter{logf: mockLogFunc}
    message := "test message"
    n, err := lw.Write([]byte(message))

    if err != nil {
        t.Errorf("Expected no error, got %v", err)
    }

    if n != len(message) {
        t.Errorf("Expected %d bytes written, got %d", n, len(message))
    }

    if loggedLevel != lg.WARN {
        t.Errorf("Expected log level WARN, got %v", loggedLevel)
    }

    if loggedFormat != "%s" {
        t.Errorf("Expected log format '%%s', got %s", loggedFormat)
    }

    if len(loggedArgs) != 1 || loggedArgs[0].(string) != message {
        t.Errorf("Expected logged args [%s], got %v", message, loggedArgs)
    }
}

// Test generated using Keploy
func TestServe_NormalOperation(t *testing.T) {
    var logMessages []string

    mockLogFunc := func(lvl lg.LogLevel, f string, args ...interface{}) {
        logMessages = append(logMessages, fmt.Sprintf(f, args...))
    }

    listener, err := net.Listen("tcp", "localhost:0")
    if err != nil {
        t.Fatalf("Failed to create listener: %v", err)
    }
    defer listener.Close()

    handler := http.NewServeMux()

    // Run Serve in a separate goroutine because it blocks
    done := make(chan error)
    go func() {
        err := Serve(listener, handler, "test_proto", mockLogFunc)
        done <- err
    }()

    // Wait a bit to let the server start
    time.Sleep(100 * time.Millisecond)

    // Make a simple HTTP request to ensure the server is running
    resp, err := http.Get(fmt.Sprintf("http://%s", listener.Addr().String()))
    if err != nil {
        t.Errorf("Failed to connect to server: %v", err)
    } else {
        resp.Body.Close()
    }

    // Close the listener to stop the server
    listener.Close()

    // Wait for Serve to finish
    err = <-done
    if err != nil {
        t.Errorf("Serve returned error: %v", err)
    }

    // Check that appropriate log messages were logged
    expectedStartMessage := fmt.Sprintf("test_proto: listening on %s", listener.Addr())
    expectedCloseMessage := fmt.Sprintf("test_proto: closing %s", listener.Addr())
    if len(logMessages) < 2 {
        t.Errorf("Expected at least 2 log messages, got %d", len(logMessages))
    } else {
        if logMessages[0] != expectedStartMessage {
            t.Errorf("Expected log message '%s', got '%s'", expectedStartMessage, logMessages[0])
        }
        if logMessages[len(logMessages)-1] != expectedCloseMessage {
            t.Errorf("Expected log message '%s', got '%s'", expectedCloseMessage, logMessages[len(logMessages)-1])
        }
    }
}


// Test generated using Keploy
func TestServe_ServerError(t *testing.T) {
    var logMessages []string

    mockLogFunc := func(lvl lg.LogLevel, f string, args ...interface{}) {
        logMessages = append(logMessages, fmt.Sprintf(f, args...))
    }

    handler := http.NewServeMux()

    // Create a faulty listener that returns an error on Accept
    faultyListener := &FaultyListener{}

    err := Serve(faultyListener, handler, "test_proto", mockLogFunc)
    if err == nil {
        t.Errorf("Expected error from Serve, got nil")
    } else if err.Error() != "http.Serve() error - forced error" {
        t.Errorf("Unexpected error message: %v", err)
    }
}

// FaultyListener is a net.Listener that returns an error on Accept
type FaultyListener struct{}

func (fl *FaultyListener) Accept() (net.Conn, error) {
    return nil, fmt.Errorf("forced error")
}

func (fl *FaultyListener) Close() error {
    return nil
}

func (fl *FaultyListener) Addr() net.Addr {
    return &net.TCPAddr{
        IP:   net.ParseIP("127.0.0.1"),
        Port: 0,
    }
}

