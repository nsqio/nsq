package main

import (
    "os"
    "testing"
    "github.com/nsqio/go-nsq"
    "bytes"
)


// Test generated using Keploy
func TestMain_VersionFlag(t *testing.T) {
    os.Args = []string{"cmd", "--version"}
    exitCode := 0
    exitFunc := func(code int) {
        exitCode = code
    }
    appExit = exitFunc
    defer func() { appExit = os.Exit }()
    
    main()
    
    if exitCode != 0 {
        t.Errorf("Expected exit code 0, got %d", exitCode)
    }
}

// Test generated using Keploy
func TestTailHandler_HandleMessage_Exit(t *testing.T) {
    handler := &TailHandler{
        topicName:     "test_topic",
        totalMessages: 1,
        messagesShown: 0,
    }
    
    message := &nsq.Message{
        Body: []byte("test message"),
    }
    
    exitCode := 0
    exitFunc := func(code int) {
        exitCode = code
    }
    appExit = exitFunc
    defer func() { appExit = os.Exit }()
    
    handler.HandleMessage(message)
    
    if exitCode != 0 {
        t.Errorf("Expected exit code 0, got %d", exitCode)
    }
}


// Test generated using Keploy
func TestTailHandler_HandleMessage_PrintTopic(t *testing.T) {
    // Set printTopic flag to true
    originalPrintTopic := *printTopic
    *printTopic = true
    defer func() { *printTopic = originalPrintTopic }()

    // Replace os.Stdout with a pipe
    r, w, err := os.Pipe()
    if err != nil {
        t.Fatalf("Failed to create pipe: %v", err)
    }
    originalStdout := os.Stdout
    os.Stdout = w
    defer func() {
        os.Stdout = originalStdout
        w.Close()
        r.Close()
    }()

    handler := &TailHandler{
        topicName:     "test_topic",
        totalMessages: 0,
        messagesShown: 0,
    }

    message := &nsq.Message{
        Body: []byte("test message"),
    }

    err = handler.HandleMessage(message)
    if err != nil {
        t.Errorf("Expected no error, got %v", err)
    }

    // Close writer to allow reading
    w.Close()

    // Read output
    var buf bytes.Buffer
    _, err = buf.ReadFrom(r)
    if err != nil {
        t.Fatalf("Failed to read from pipe: %v", err)
    }

    expectedOutput := "test_topic | test message\n"
    if buf.String() != expectedOutput {
        t.Errorf("Expected output %q, got %q", expectedOutput, buf.String())
    }
}

