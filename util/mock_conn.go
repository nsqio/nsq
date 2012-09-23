package util

import (
	"io"
	"net"
	"time"
)

type MockConn struct {
	io.ReadWriter
}

func (c MockConn) Close() error { return nil }
func (c MockConn) LocalAddr() net.Addr { return &net.TCPAddr{net.IPv4zero, 0 } }
func (c MockConn) RemoteAddr() net.Addr { return &net.TCPAddr{net.IPv4zero, 0} }
func (c MockConn) SetDeadline(t time.Time) error { return nil }
func (c MockConn) SetReadDeadline(t time.Time) error { return nil }
func (c MockConn) SetWriteDeadline(t time.Time) error { return nil }
