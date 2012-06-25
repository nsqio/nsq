package nsq

import (
	"net"
	"strconv"
)

type LookupPeer struct {
	*ProtocolClient
}

func NewLookupPeer(tcpAddr *net.TCPAddr) *LookupPeer {
	return &LookupPeer{&ProtocolClient{tcpAddr: tcpAddr}}
}

func (c *LookupPeer) Announce(topic string, channel string, port int) *ProtocolCommand {
	var params = [][]byte{[]byte(topic), []byte(channel), []byte(strconv.Itoa(port))}
	return &ProtocolCommand{[]byte("ANNOUNCE"), params}
}

func (c *LookupPeer) Ping() *ProtocolCommand {
	return &ProtocolCommand{[]byte("PING"), make([][]byte, 0)}
}
