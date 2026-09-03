package lg

import (
	"context"
	"io"
	"log/slog"
	"strings"
)

// levelFatal extends the log/slog levels with nsq's FATAL, which sits above
// slog.LevelError.
const levelFatal = slog.LevelError + 4

// jsonLogger writes each log message as a single line of JSON, using
// log/slog's JSONHandler.
type jsonLogger struct {
	logger *slog.Logger
}

// NewJSONLogger returns a Logger that writes JSON to w. Each message is
// written as an object with "time", "level" and "msg" keys, plus a "prefix"
// key when prefix is not empty. Levels are named as LogLevel.String() names
// them, so that JSON and plain text output agree.
func NewJSONLogger(w io.Writer, prefix string) Logger {
	logger := slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{
		// Logf has already applied the configured level, everything that
		// reaches the handler should be written
		Level:       slog.LevelDebug,
		ReplaceAttr: replaceLevel,
	}))
	if prefix = strings.TrimSpace(prefix); prefix != "" {
		logger = logger.With(slog.String("prefix", prefix))
	}
	return &jsonLogger{logger: logger}
}

func (l *jsonLogger) log(lvl LogLevel, s string) error {
	l.logger.Log(context.Background(), slogLevel(lvl), s)
	return nil
}

// Output implements Logger, recovering the level from the "LEVEL: " prefix
// that Logf writes ahead of every message.
func (l *jsonLogger) Output(maxdepth int, s string) error {
	lvl := INFO
	if i := strings.Index(s, ": "); i > 0 {
		if parsed, ok := parseLevelName(s[:i]); ok {
			lvl, s = parsed, s[i+2:]
		}
	}
	return l.log(lvl, s)
}

func slogLevel(lvl LogLevel) slog.Level {
	switch lvl {
	case DEBUG:
		return slog.LevelDebug
	case WARN:
		return slog.LevelWarn
	case ERROR:
		return slog.LevelError
	case FATAL:
		return levelFatal
	}
	return slog.LevelInfo
}

func logLevel(lvl slog.Level) LogLevel {
	switch {
	case lvl <= slog.LevelDebug:
		return DEBUG
	case lvl <= slog.LevelInfo:
		return INFO
	case lvl <= slog.LevelWarn:
		return WARN
	case lvl <= slog.LevelError:
		return ERROR
	}
	return FATAL
}

// parseLevelName is the inverse of LogLevel.String()
func parseLevelName(s string) (LogLevel, bool) {
	for lvl := DEBUG; lvl <= FATAL; lvl++ {
		if lvl.String() == s {
			return lvl, true
		}
	}
	return INFO, false
}

// replaceLevel renders the level with the name nsq uses elsewhere, so that
// WARN reads "WARNING" and FATAL reads "FATAL" rather than "ERROR+4"
func replaceLevel(groups []string, a slog.Attr) slog.Attr {
	if len(groups) != 0 || a.Key != slog.LevelKey {
		return a
	}
	lvl, ok := a.Value.Any().(slog.Level)
	if !ok {
		return a
	}
	name := logLevel(lvl)
	a.Value = slog.StringValue(name.String())
	return a
}
