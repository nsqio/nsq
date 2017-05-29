// short for "log"
package lg

import (
	"fmt"
	"strings"
)

type LogLevel int

const (
	DEBUG = LogLevel(1)
	INFO  = LogLevel(2)
	WARN  = LogLevel(3)
	ERROR = LogLevel(4)
	FATAL = LogLevel(5)
)

type AppLogFunc func(lvl LogLevel, f string, args ...interface{})

type Logger interface {
	Output(maxdepth int, s string) error
}

type NilLogger struct{}

func (l NilLogger) Output(maxdepth int, s string) error {
	return nil
}

func (l LogLevel) String() string {
	switch l {
	case 1:
		return "DEBUG"
	case 2:
		return "INFO"
	case 3:
		return "WARNING"
	case 4:
		return "ERROR"
	case 5:
		return "FATAL"
	}
	panic("invalid LogLevel")
}

func ParseLogLevel(levelstr string, verbose bool) (LogLevel, error) {
	lvl := INFO

	switch strings.ToLower(levelstr) {
	case "debug":
		lvl = DEBUG
	case "info":
		lvl = INFO
	case "warn":
		lvl = WARN
	case "error":
		lvl = ERROR
	case "fatal":
		lvl = FATAL
	default:
		return lvl, fmt.Errorf("invalid log-level '%s'", levelstr)
	}
	if verbose {
		lvl = DEBUG
	}
	return lvl, nil
}

func Logf(logger Logger, cfgLevel LogLevel, msgLevel LogLevel, f string, args ...interface{}) {
	if cfgLevel > msgLevel {
		return
	}
	logger.Output(3, fmt.Sprintf(msgLevel.String()+": "+f, args...))
}
