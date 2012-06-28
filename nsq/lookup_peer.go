package nsq

import (
	"net"
	"strconv"
)

// LookupPeer is a low-level type for connecting/reading/writing to nsqlookupd
type LookupPeer struct {
	*ProtocolClient
}

// NewLookupPeer creates a new LookupPeer, initializes 
// the underlying ProtocolClient, and returns a pointer
func NewLookupPeer(tcpAddr *net.TCPAddr) *LookupPeer {
	return &LookupPeer{&ProtocolClient{tcpAddr: tcpAddr}}
}

// Announce creates a new ProtocolCommand to announce the existence of
// a given topic and/or channel.
// NOTE: if channel == "." then it is considered n/a
func (c *LookupPeer) Announce(topic string, channel string, port int) *ProtocolCommand {
	var params = [][]byte{[]byte(topic), []byte(channel), []byte(strconv.Itoa(port))}
	return &ProtocolCommand{[]byte("ANNOUNCE"), params}
}

// Ping creates a new ProtocolCommand to keep-alive the state of all the 
// announced topic/channels for a given client
func (c *LookupPeer) Ping() *ProtocolCommand {
	return &ProtocolCommand{[]byte("PING"), make([][]byte, 0)}
}
