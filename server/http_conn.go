package server

import (
	"io"
)

type HTTPConn struct {
	io.ReadWriter
}

func (c *HTTPConn) Close() error {
	return nil
}
