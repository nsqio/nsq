package lg

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/nsqio/nsq/internal/test"
)

type jsonRecord struct {
	Time   string `json:"time"`
	Level  string `json:"level"`
	Msg    string `json:"msg"`
	Prefix string `json:"prefix"`
}

func decodeJSONLog(t *testing.T, b []byte) jsonRecord {
	t.Helper()
	var r jsonRecord
	err := json.Unmarshal(b, &r)
	test.Nil(t, err)
	return r
}

func TestJSONLogging(t *testing.T) {
	// levels are named in JSON exactly as they are named in plain text
	for i := 1; i <= 5; i++ {
		lvl := LogLevel(i)
		var buf bytes.Buffer
		Logf(NewJSONLogger(&buf, "[nsqd] "), DEBUG, lvl, "TOPIC(%s): created", "mytopic")

		test.Equal(t, 1, strings.Count(buf.String(), "\n"))
		r := decodeJSONLog(t, buf.Bytes())
		test.Equal(t, lvl.String(), r.Level)
		test.Equal(t, "TOPIC(mytopic): created", r.Msg)
		test.Equal(t, "[nsqd]", r.Prefix)

		_, err := time.Parse(time.RFC3339Nano, r.Time)
		test.Nil(t, err)
	}
}

func TestJSONLoggingLevelFiltering(t *testing.T) {
	var buf bytes.Buffer
	logger := NewJSONLogger(&buf, "")
	for i := 1; i <= 5; i++ {
		Logf(logger, WARN, LogLevel(i), "Test")
	}
	test.Equal(t, 3, strings.Count(buf.String(), "\n"))

	// an empty prefix is not written
	test.Equal(t, false, strings.Contains(buf.String(), "prefix"))
}

func TestJSONLoggingOutput(t *testing.T) {
	// a caller that is not level aware still gets the level right, it is
	// recovered from the prefix Logf would have written
	var buf bytes.Buffer
	logger := NewJSONLogger(&buf, "")
	err := logger.Output(2, "WARNING: TOPIC(mytopic): closing")
	test.Nil(t, err)
	r := decodeJSONLog(t, buf.Bytes())
	test.Equal(t, "WARNING", r.Level)
	test.Equal(t, "TOPIC(mytopic): closing", r.Msg)

	buf.Reset()
	err = logger.Output(2, "NSQ: bye")
	test.Nil(t, err)
	r = decodeJSONLog(t, buf.Bytes())
	test.Equal(t, "INFO", r.Level)
	test.Equal(t, "NSQ: bye", r.Msg)
}

func TestNewLoggerText(t *testing.T) {
	// the default output is unchanged, e.g.
	// [nsqd] 2026/09/04 02:29:03.301342 INFO: TOPIC(mytopic): created
	var buf bytes.Buffer
	Logf(NewLogger(&buf, "[nsqd] ", false), DEBUG, INFO, "TOPIC(%s): created", "mytopic")

	line := buf.String()
	test.Equal(t, true, strings.HasPrefix(line, "[nsqd] "))
	test.Equal(t, true, strings.HasSuffix(line, " INFO: TOPIC(mytopic): created\n"))
}
