package nsq

import (
	"io"
)

type StatefulReadWriter interface {
	io.ReadWriter
	GetState(key string) (interface{}, bool)
	SetState(key string, val interface{})
	String() string
}

type Protocol interface {
	IOLoop(client StatefulReadWriter) error
	Execute(client StatefulReadWriter, params ...string) ([]byte, error)
}

type ClientError struct {
	errStr string
}

func (e ClientError) Error() string {
	return e.errStr
}

var ClientErrBadProtocol = ClientError{"E_BAD_PROTOCOL"}
