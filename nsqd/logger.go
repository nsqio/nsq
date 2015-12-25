package nsqd

import (
	"github.com/golang/glog"
	"sync/atomic"
)

type logger interface {
	Level() int32
	SetLevel(int32)
	Output(maxdepth int, s string) error
	OutputErr(maxdepth int, s string) error
	OutputWarning(maxdepth int, s string) error
}

type GLogger struct {
	level int32
}

func (self *GLogger) SetLevel(l int32) {
	atomic.StoreInt32(&self.level, l)
}

func (self *GLogger) Level() int32 {
	return atomic.LoadInt32(&self.level)
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
