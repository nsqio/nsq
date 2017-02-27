package levellogger

import (
	"fmt"
	"github.com/absolute8511/glog"
	"log"
	"os"
	"sync/atomic"
)

type Logger interface {
	Output(maxdepth int, s string) error
	OutputErr(maxdepth int, s string) error
	OutputWarning(maxdepth int, s string) error
}

type SimpleLogger struct {
	l *log.Logger
}

func NewSimpleLog() *SimpleLogger {
	return &SimpleLogger{
		l: log.New(os.Stderr, "", log.LstdFlags|log.Lmicroseconds|log.Lshortfile),
	}
}

func (self *SimpleLogger) Output(maxdepth int, s string) error {
	if self.l != nil {
		self.l.Output(maxdepth, s)
	}
	return nil
}

func (self *SimpleLogger) OutputErr(maxdepth int, s string) error {
	if self.l != nil {
		self.l.Output(maxdepth, s)
	}
	return nil
}

func (self *SimpleLogger) OutputWarning(maxdepth int, s string) error {
	if self.l != nil {
		self.l.Output(maxdepth, s)
	}
	return nil
}

type GLogger struct {
}

func (self *GLogger) Output(maxdepth int, s string) error {
	glog.InfoDepth(maxdepth, s)
	return nil
}

func (self *GLogger) OutputErr(maxdepth int, s string) error {
	glog.ErrorDepth(maxdepth, s)
	return nil
}

func (self *GLogger) OutputWarning(maxdepth int, s string) error {
	glog.WarningDepth(maxdepth, s)
	return nil
}

const (
	LOG_ERR int32 = iota
	LOG_WARN
	LOG_INFO
	LOG_DEBUG
	LOG_DETAIL
)

type LevelLogger struct {
	Logger Logger
	level  int32
}

func NewLevelLogger(level int32, l Logger) *LevelLogger {
	return &LevelLogger{
		Logger: l,
		level:  level,
	}
}

func (self *LevelLogger) SetLevel(l int32) {
	atomic.StoreInt32(&self.level, l)
}

func (self *LevelLogger) Level() int32 {
	return atomic.LoadInt32(&self.level)
}

func (self *LevelLogger) Logf(f string, args ...interface{}) {
	if self.Logger != nil && self.Level() >= LOG_INFO {
		self.Logger.Output(2, fmt.Sprintf(f, args...))
	}
}

func (self *LevelLogger) LogDebugf(f string, args ...interface{}) {
	if self.Logger != nil && self.Level() >= LOG_DEBUG {
		self.Logger.Output(2, fmt.Sprintf(f, args...))
	}
}

func (self *LevelLogger) LogErrorf(f string, args ...interface{}) {
	if self.Logger != nil {
		self.Logger.OutputErr(2, fmt.Sprintf(f, args...))
	}
}

func (self *LevelLogger) LogWarningf(f string, args ...interface{}) {
	if self.Logger != nil && self.Level() >= LOG_WARN {
		self.Logger.OutputWarning(2, fmt.Sprintf(f, args...))
	}
}

func (self *LevelLogger) Infof(f string, args ...interface{}) {
	if self.Logger != nil && self.Level() >= LOG_INFO {
		self.Logger.Output(2, fmt.Sprintf(f, args...))
	}
}

func (self *LevelLogger) Debugf(f string, args ...interface{}) {
	if self.Logger != nil && self.Level() >= LOG_DEBUG {
		self.Logger.Output(2, fmt.Sprintf(f, args...))
	}
}

func (self *LevelLogger) Errorf(f string, args ...interface{}) {
	if self.Logger != nil {
		self.Logger.OutputErr(2, fmt.Sprintf(f, args...))
	}
}

func (self *LevelLogger) Warningf(f string, args ...interface{}) {
	if self.Logger != nil && self.Level() >= LOG_WARN {
		self.Logger.OutputWarning(2, fmt.Sprintf(f, args...))
	}
}

func (self *LevelLogger) Warningln(f string) {
	if self.Logger != nil && self.Level() >= LOG_WARN {
		self.Logger.OutputWarning(2, f)
	}
}
