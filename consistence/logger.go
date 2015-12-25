package consistence

import (
	"fmt"
	"github.com/absolute8511/nsq/internal/levellogger"
)

var coordLog = &coordLogT{
	levellogger.NewGLogger(1),
}

type coordLogT struct {
	logger levellogger.Logger
}

func (l *coordLogT) Infof(f string, args ...interface{}) {
	if l.logger == nil {
		return
	}
	if l.logger.Level() > 0 {
		l.logger.Output(2, fmt.Sprintf(f, args...))
	}
}

func (l *coordLogT) Debugf(f string, args ...interface{}) {
	if l.logger == nil {
		return
	}
	if l.logger.Level() > 1 {
		l.logger.Output(2, fmt.Sprintf(f, args...))
	}
}

func (l *coordLogT) Errorf(f string, args ...interface{}) {
	if l.logger == nil {
		return
	}
	l.logger.OutputErr(2, fmt.Sprintf(f, args...))
}

func (l *coordLogT) Warningf(f string, args ...interface{}) {
	if l.logger == nil {
		return
	}
	l.logger.OutputWarning(2, fmt.Sprintf(f, args...))
}

func (l *coordLogT) Warningln(f string) {
	if l.logger == nil {
		return
	}
	l.logger.OutputWarning(2, f)
}
