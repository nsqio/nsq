package nsqd

import (
	"fmt"
	"strings"
)

type Logger interface {
	Output(maxdepth int, s string) error
}

const (
	LOG_DEBUG = 1
	LOG_INFO  = 2
	LOG_WARN  = 3
	LOG_ERROR = 4
	LOG_FATAL = 5
)

func (n *NSQD) logLevelFromString(level string) int {
	// check log-level is valid and translate to int
	switch strings.ToLower(level) {
	case "debug":
		return LOG_DEBUG
	case "info":
		return LOG_INFO
	case "warn":
		return LOG_WARN
	case "error":
		return LOG_ERROR
	case "fatal":
		return LOG_FATAL
	default:
		return -1
	}
}

func (n *NSQD) logf(level int, f string, args ...interface{}) {
	levelString := "INFO"
	switch level {
	case LOG_DEBUG:
		levelString = "DEBUG"
	case LOG_INFO:
		levelString = "INFO"
	case LOG_WARN:
		levelString = "WARNING"
	case LOG_ERROR:
		levelString = "ERROR"
	case LOG_FATAL:
		levelString = "FATAL"
	}

	opts := n.getOpts()
	if level >= opts.logLevel || opts.Verbose {
		n.getOpts().Logger.Output(2, fmt.Sprintf(levelString+": "+f, args...))
	}
}
