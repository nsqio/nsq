package lg

import (
	"testing"

	"github.com/nsqio/nsq/internal/test"
)

type mockLogger struct {
	Count int
}

func (l *mockLogger) Output(maxdepth int, s string) error {
	l.Count++
	return nil
}

func TestLogging(t *testing.T) {
	logger := &mockLogger{}

	// Test only fatal get through
	logger.Count = 0
	for i := 1; i <= 5; i++ {
		Logf(logger, FATAL, LogLevel(i), "Test")
	}
	test.Equal(t, 1, logger.Count)

	// Test only warnings or higher get through
	logger.Count = 0
	for i := 1; i <= 5; i++ {
		Logf(logger, WARN, LogLevel(i), "Test")
	}
	test.Equal(t, 3, logger.Count)

	// Test everything gets through
	logger.Count = 0
	for i := 1; i <= 5; i++ {
		Logf(logger, DEBUG, LogLevel(i), "Test")
	}
	test.Equal(t, 5, logger.Count)
}

// Test generated using Keploy
func TestNilLoggerOutput(t *testing.T) {
	logger := NilLogger{}
	err := logger.Output(1, "test message")
	if err != nil {
		t.Errorf("Expected nil, got %v", err)
	}
}

// Test generated using Keploy
func TestLogLevelGet(t *testing.T) {
	level := LogLevel(INFO)
	result := level.Get()
	if result != INFO {
		t.Errorf("Expected %v, got %v", INFO, result)
	}
}

// Test generated using Keploy
func TestLogLevelSetValid(t *testing.T) {
	var level LogLevel
	err := level.Set("debug")
	if err != nil {
		t.Errorf("Expected nil, got %v", err)
	}
	if level != DEBUG {
		t.Errorf("Expected %v, got %v", DEBUG, level)
	}
}

// Test generated using Keploy
func TestLogLevelSetInvalid(t *testing.T) {
	var level LogLevel
	err := level.Set("invalid")
	if err == nil {
		t.Errorf("Expected an error, got nil")
	}
}

// Test generated using Keploy
func TestParseLogLevelValid(t *testing.T) {
	tests := map[string]LogLevel{
		"debug": DEBUG,
		"info":  INFO,
		"warn":  WARN,
		"error": ERROR,
		"fatal": FATAL,
	}
	for input, expected := range tests {
		result, err := ParseLogLevel(input)
		if err != nil {
			t.Errorf("Expected nil, got %v", err)
		}
		if result != expected {
			t.Errorf("For input %s, expected %v, got %v", input, expected, result)
		}
	}
}
