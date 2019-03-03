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
