package levellogger

import (
	"fmt"
	"github.com/absolute8511/glog"
	"sync/atomic"
)

type Logger interface {
	Level() int32
	SetLevel(int32)
	Logf(f string, args ...interface{})
	LogDebugf(f string, args ...interface{})
	LogWarningf(f string, args ...interface{})
	LogErrorf(f string, args ...interface{})
	Output(maxdepth int, s string) error
	OutputErr(maxdepth int, s string) error
	OutputWarning(maxdepth int, s string) error
}

type GLogger struct {
	level int32
}

func NewGLogger(l int32) *GLogger {
	return &GLogger{l}
}

func (self *GLogger) SetLevel(l int32) {
	atomic.StoreInt32(&self.level, l)
}

func (self *GLogger) Level() int32 {
	return self.level
}

func (self *GLogger) Logf(f string, args ...interface{}) {
	if self.level > 0 {
		self.Output(2, fmt.Sprintf(f, args...))
	}
}

func (self *GLogger) LogDebugf(f string, args ...interface{}) {
	if self.level > 1 {
		self.Output(2, fmt.Sprintf(f, args...))
	}
}

func (self *GLogger) LogErrorf(f string, args ...interface{}) {
	self.OutputErr(2, fmt.Sprintf(f, args...))
}

func (self *GLogger) LogWarningf(f string, args ...interface{}) {
	self.OutputWarning(2, fmt.Sprintf(f, args...))
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
