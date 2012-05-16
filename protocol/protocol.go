package protocol

import (
	"net"
)

type Client interface {
	Write(data []byte) error
	WriteError(err error) error
	GetConnection() net.Conn
	GetState() int
	SetState(state int)
	String() string
}

type Protocol interface {
	IOLoop(client Client) error
}

var Protocols = map[int32]Protocol{}

type ClientError struct {
	errStr string
}

func (e ClientError) Error() string {
	return e.errStr
}

var ClientErrBadProtocol = ClientError{"E_BAD_PROTOCOL"}
