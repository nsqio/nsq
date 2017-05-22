package test

type Logger interface {
	Output(maxdepth int, s string) error
}

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

func NewTestLogger(tbl tbLog) Logger {
	return &testLogger{tbl}
}
