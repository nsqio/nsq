package test

import ()

type TbLog interface {
	Log(...interface{})
}

type TestLogger struct {
	TbLog
	Level int32
}

func (tl *TestLogger) Output(maxdepth int, s string) error {
	tl.Log(s)
	return nil
}

func (tl *TestLogger) OutputErr(maxdepth int, s string) error {
	tl.Log(s)
	return nil
}

func (tl *TestLogger) OutputWarning(maxdepth int, s string) error {
	tl.Log(s)
	return nil
}
