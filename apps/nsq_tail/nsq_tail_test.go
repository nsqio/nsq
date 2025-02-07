package main

import (
    "os"
    "testing"
    "github.com/nsqio/go-nsq"
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

