package test

import (
	"github.com/nsqio/nsq/internal/app"
)

type tbLog interface {
	Log(...interface{})
}

type testLogger struct {
	tbLog
}

func (tl *testLogger) Output(maxdepth int, s string) error {
	tl.Log(s)
	return nil
}

func NewTestLogger(tbl tbLog) app.Logger {
	return &testLogger{tbl}
}
