package main

import (
	"net"
)

type ClientStats struct {
	version string
	address string
	name    string
	state   int
}

type ClientV1 struct {
	net.Conn
	State    int
	Producer *Producer
}

func NewClientV1(conn net.Conn) *ClientV1 {
	return &ClientV1{
		net.Conn: conn,
	}
}

func (c *ClientV1) String() string {
	return c.RemoteAddr().String()
}

func (c *ClientV1) Stats() ClientStats {
	return ClientStats{
		version: "V1",
		name:    c.RemoteAddr().String(),
		state:   c.State,
	}
}

func (c *ClientV1) TimedOutMessage() {}
