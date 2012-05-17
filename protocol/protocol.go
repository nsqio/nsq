package protocol

import (
	"io"
)

type Client interface {
	Write(data []byte) error
	WriteError(err error) error
	GetConnection() io.ReadWriteCloser
	GetState() int
	SetState(state int)
	String() string
}

type Protocol interface {
	IOLoop(client Client) error
	Execute(client Client, params ...string) ([]byte, error)
}

var Protocols = map[int32]Protocol{}

type ClientError struct {
	errStr string
}

func (e ClientError) Error() string {
	return e.errStr
}

var ClientErrBadProtocol = ClientError{"E_BAD_PROTOCOL"}
