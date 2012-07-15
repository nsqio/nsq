package main

import (
	"../nsq"
)

type ClientStats struct {
	version string
	name    string
	state   int
}

type ServerClientV1 struct {
	*nsq.ServerClient
	State int
}

func NewServerClientV1(client *nsq.ServerClient) *ServerClientV1 {
	return &ServerClientV1{client, 0}
}

func (c *ServerClientV1) Stats() ClientStats {
	return ClientStats{
		version: "V1",
		name:    c.String(),
		state:   c.State,
	}
}

func (c *ServerClientV1) TimedOutMessage() {}
