package main

import (
	"net"
	"time"
)

type ClientV1 struct {
	net.Conn
	State       int32
	ConnectTime time.Time
}

func NewClientV1(conn net.Conn) *ClientV1 {
	return &ClientV1{
		net.Conn:    conn,
		ConnectTime: time.Now(),
	}
}

func (c *ClientV1) Stats() ClientStats {
	return ClientStats{
		version:     "V1",
		name:        c.RemoteAddr().String(),
		address:     c.RemoteAddr().String(),
		state:       c.State,
		connectTime: c.ConnectTime,
	}
}

func (c *ClientV1) TimedOutMessage() {}

func (c *ClientV1) Exit() {}
