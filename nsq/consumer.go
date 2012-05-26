package nsq

import (
	"strconv"
)

type Consumer struct {
	*ProtocolClient
}

func NewConsumer() *Consumer {
	return &Consumer{&ProtocolClient{}}
}

func (c *ProtocolClient) Subscribe(topic string, channel string) *ProtocolCommand {
	var params = [][]byte{[]byte(topic), []byte(channel)}
	return &ProtocolCommand{[]byte("SUB"), params}
}

func (c *ProtocolClient) Ready(count int) *ProtocolCommand {
	var params = [][]byte{[]byte(strconv.Itoa(count))}
	return &ProtocolCommand{[]byte("RDY"), params}
}

func (c *ProtocolClient) Finish(uuid string) *ProtocolCommand {
	var params = [][]byte{[]byte(uuid)}
	return &ProtocolCommand{[]byte("FIN"), params}
}

func (c *ProtocolClient) Requeue(uuid string) *ProtocolCommand {
	var params = [][]byte{[]byte(uuid)}
	return &ProtocolCommand{[]byte("REQ"), params}
}
