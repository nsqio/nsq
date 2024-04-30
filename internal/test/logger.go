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

// NilLogger is a Logger that produces no output. It may be useful in
// benchmarks, where otherwise the output would be distracting. The benchmarking
// package restricts output to 10 lines anyway, so logging via the testing.B is
// not very useful.
type NilLogger struct{}

func (l NilLogger) Output(maxdepth int, s string) error {
	return nil
}
