package test

import (
    "testing"
)


// Test generated using Keploy
// Mock implementation of tbLog
type mockTbLog struct {
    loggedMessages []string
}

func (m *mockTbLog) Log(args ...interface{}) {
    for _, arg := range args {
        if msg, ok := arg.(string); ok {
            m.loggedMessages = append(m.loggedMessages, msg)
        }
    }
}

func TestNewTestLogger_OutputCallsLog(t *testing.T) {
    mockLog := &mockTbLog{}
    logger := NewTestLogger(mockLog)

    testMessage := "test log message"
    err := logger.Output(1, testMessage)
    if err != nil {
        t.Errorf("Expected no error, got %v", err)
    }

    if len(mockLog.loggedMessages) != 1 {
        t.Errorf("Expected 1 logged message, got %d", len(mockLog.loggedMessages))
    }

    if mockLog.loggedMessages[0] != testMessage {
        t.Errorf("Expected logged message to be '%s', got '%s'", testMessage, mockLog.loggedMessages[0])
    }
}
