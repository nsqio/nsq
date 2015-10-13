package test

import (
	"net"
	"time"
)

type FakeNetConn struct {
	ReadFunc             func([]byte) (int, error)
	WriteFunc            func([]byte) (int, error)
	CloseFunc            func() error
	LocalAddrFunc        func() net.Addr
	RemoteAddrFunc       func() net.Addr
	SetDeadlineFunc      func(time.Time) error
	SetReadDeadlineFunc  func(time.Time) error
	SetWriteDeadlineFunc func(time.Time) error
}

func (f FakeNetConn) Read(b []byte) (int, error)         { return f.ReadFunc(b) }
func (f FakeNetConn) Write(b []byte) (int, error)        { return f.WriteFunc(b) }
func (f FakeNetConn) Close() error                       { return f.CloseFunc() }
func (f FakeNetConn) LocalAddr() net.Addr                { return f.LocalAddrFunc() }
func (f FakeNetConn) RemoteAddr() net.Addr               { return f.RemoteAddrFunc() }
func (f FakeNetConn) SetDeadline(t time.Time) error      { return f.SetDeadlineFunc(t) }
func (f FakeNetConn) SetReadDeadline(t time.Time) error  { return f.SetReadDeadlineFunc(t) }
func (f FakeNetConn) SetWriteDeadline(t time.Time) error { return f.SetWriteDeadlineFunc(t) }

type fakeNetAddr struct{}

func (fakeNetAddr) Network() string { return "" }
func (fakeNetAddr) String() string  { return "" }

func NewFakeNetConn() FakeNetConn {
	netAddr := fakeNetAddr{}
	return FakeNetConn{
		ReadFunc:             func(b []byte) (int, error) { return 0, nil },
		WriteFunc:            func(b []byte) (int, error) { return len(b), nil },
		CloseFunc:            func() error { return nil },
		LocalAddrFunc:        func() net.Addr { return netAddr },
		RemoteAddrFunc:       func() net.Addr { return netAddr },
		SetDeadlineFunc:      func(time.Time) error { return nil },
		SetWriteDeadlineFunc: func(time.Time) error { return nil },
		SetReadDeadlineFunc:  func(time.Time) error { return nil },
	}
}
