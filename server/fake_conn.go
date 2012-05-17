package server

import (
	"io"
)

type FakeConn struct {
	io.ReadWriter
}

func (c *FakeConn) Close() error {
	return nil
}
