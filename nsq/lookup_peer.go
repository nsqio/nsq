package nsq

import (
	"net"
)

type LookupPeer struct {
	*ProtocolClient
}

func NewLookupPeer(tcpAddr *net.TCPAddr) *LookupPeer {
	return &LookupPeer{&ProtocolClient{tcpAddr: tcpAddr}}
}

func (c *LookupPeer) Announce(topic string, address string, port string) *ProtocolCommand {
	var params = [][]byte{[]byte(topic), []byte(address), []byte(port)}
	return &ProtocolCommand{[]byte("ANNOUNCE"), params}
}

func (c *LookupPeer) Ping() *ProtocolCommand {
	return &ProtocolCommand{[]byte("PING"), make([][]byte, 0)}
}
