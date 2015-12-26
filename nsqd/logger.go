package nsqd

import (
	"fmt"
	"github.com/absolute8511/nsq/internal/levellogger"
)

type nsqLogT struct {
	l levellogger.Logger
}

func NewNsqLogT(l levellogger.Logger) *nsqLogT {
	return &nsqLogT{l}
}

func (l *nsqLogT) SetLogLevel(level int32) {
	if l.l == nil {
		return
	}
	l.l.SetLevel(level)
}

func (l *nsqLogT) logf(f string, args ...interface{}) {
	if l.l == nil {
		return
	}
	l.l.Output(2, fmt.Sprintf(f, args...))
}

func (l *nsqLogT) logDebugf(f string, args ...interface{}) {
	if l.l == nil {
		return
	}
	if l.l.Level() > 1 {
		l.l.Output(2, fmt.Sprintf(f, args...))
	}
}

func (l *nsqLogT) logErrorf(f string, args ...interface{}) {
	if l.l == nil {
		return
	}
	l.l.OutputErr(2, fmt.Sprintf(f, args...))
}

func (l *nsqLogT) logWarningf(f string, args ...interface{}) {
	if l.l == nil {
		return
	}
	l.l.OutputWarning(2, fmt.Sprintf(f, args...))
}

var nsqLog = &nsqLogT{l: levellogger.NewGLogger(1)}
