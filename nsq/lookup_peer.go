package nsq

import ()

type LookupPeer struct {
	*ProtocolClient
}

func NewLookupPeer() *LookupPeer {
	return &LookupPeer{&ProtocolClient{}}
}

func (c *LookupPeer) Announce(topic string, address string, port string) *ProtocolCommand {
	var params = [][]byte{[]byte(topic), []byte(address), []byte(port)}
	return &ProtocolCommand{[]byte("ANNOUNCE"), params}
}
