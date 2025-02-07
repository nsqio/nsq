package main

import (
    "os"
    "testing"
    "time"
    "github.com/nsqio/go-nsq"
    "github.com/nsqio/nsq/internal/lg"
)


// Test generated using Keploy
func TestNewTopicDiscoverer_Initialization(t *testing.T) {
    logf := func(lvl lg.LogLevel, f string, args ...interface{}) {}
    opts := &Options{
        HTTPClientConnectTimeout: 2 * time.Second,
        HTTPClientRequestTimeout: 5 * time.Second,
    }
    cfg := nsq.NewConfig()
    hupChan := make(chan os.Signal, 1)
    termChan := make(chan os.Signal, 1)

    td := newTopicDiscoverer(logf, opts, cfg, hupChan, termChan)

    if td.logf == nil {
        t.Errorf("Expected logf to be initialized, got nil")
    }
    if td.opts != opts {
        t.Errorf("Expected opts to be %v, got %v", opts, td.opts)
    }
    if td.cfg != cfg {
        t.Errorf("Expected cfg to be %v, got %v", cfg, td.cfg)
    }
    if td.hupChan != hupChan {
        t.Errorf("Expected hupChan to be %v, got %v", hupChan, td.hupChan)
    }
    if td.termChan != termChan {
        t.Errorf("Expected termChan to be %v, got %v", termChan, td.termChan)
    }
    if td.topics == nil {
        t.Errorf("Expected topics map to be initialized, got nil")
    }
}

// Test generated using Keploy
func TestUpdateTopics_SkipExistingTopics(t *testing.T) {
    logf := func(lvl lg.LogLevel, f string, args ...interface{}) {}
    opts := &Options{}
    cfg := nsq.NewConfig()
    hupChan := make(chan os.Signal, 1)
    termChan := make(chan os.Signal, 1)

    td := newTopicDiscoverer(logf, opts, cfg, hupChan, termChan)
    td.topics["existing_topic"] = &FileLogger{}

    td.updateTopics([]string{"existing_topic"})

    if len(td.topics) != 1 {
        t.Errorf("Expected topics map to have 1 entry, got %d", len(td.topics))
    }
}


// Test generated using Keploy
func TestIsTopicAllowed_InvalidPattern(t *testing.T) {
    logf := func(lvl lg.LogLevel, f string, args ...interface{}) {}
    opts := &Options{TopicPattern: "^allowed.*"}
    cfg := nsq.NewConfig()
    hupChan := make(chan os.Signal, 1)
    termChan := make(chan os.Signal, 1)

    td := newTopicDiscoverer(logf, opts, cfg, hupChan, termChan)

    allowed := td.isTopicAllowed("disallowed_topic")
    if allowed {
        t.Errorf("Expected isTopicAllowed to return false, got true")
    }
}


// Test generated using Keploy
func TestRun_HupChanSignal(t *testing.T) {
    logf := func(lvl lg.LogLevel, f string, args ...interface{}) {}
    opts := &Options{}
    cfg := nsq.NewConfig()
    hupChan := make(chan os.Signal, 1)
    termChan := make(chan os.Signal, 1)

    td := newTopicDiscoverer(logf, opts, cfg, hupChan, termChan)
    mockFileLogger := &FileLogger{
        hupChan: make(chan bool, 1),
    }
    td.topics["test_topic"] = mockFileLogger

    hupChan <- os.Interrupt

    go func() {
        td.run()
    }()

    select {
    case <-mockFileLogger.hupChan:
        // Success
    case <-time.After(1 * time.Second):
        t.Errorf("Expected hupChan signal to be sent to FileLogger")
    }
}


// Test generated using Keploy
func TestIsTopicAllowed_InvalidRegex(t *testing.T) {
    logf := func(lvl lg.LogLevel, f string, args ...interface{}) {}
    opts := &Options{TopicPattern: "["} // Invalid regex pattern
    cfg := nsq.NewConfig()
    hupChan := make(chan os.Signal, 1)
    termChan := make(chan os.Signal, 1)

    td := newTopicDiscoverer(logf, opts, cfg, hupChan, termChan)

    allowed := td.isTopicAllowed("any_topic")
    if allowed {
        t.Errorf("Expected isTopicAllowed to return false for invalid regex pattern, got true")
    }
}


// Test generated using Keploy
func TestRun_TermChanSignal(t *testing.T) {
    logf := func(lvl lg.LogLevel, f string, args ...interface{}) {}
    opts := &Options{}
    cfg := nsq.NewConfig()
    hupChan := make(chan os.Signal, 1)
    termChan := make(chan os.Signal, 1)

    td := newTopicDiscoverer(logf, opts, cfg, hupChan, termChan)

    done := make(chan bool)
    go func() {
        td.run()
        done <- true
    }()

    termChan <- os.Interrupt

    select {
    case <-done:
        // Success
    case <-time.After(1 * time.Second):
        t.Errorf("Expected run to terminate on termChan signal")
    }
}

