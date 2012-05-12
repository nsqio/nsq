package main

type Protocol interface {
	IOLoop(client *Client) error
}

var protocols = map[int32]Protocol{}
