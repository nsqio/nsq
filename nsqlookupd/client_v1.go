package nsqlookupd

import (
	"net"

	"github.com/nsqio/nsq/internal/registrationdb"
)

type ClientV1 struct {
	net.Conn
	peerInfo *registrationdb.PeerInfo
}

func NewClientV1(conn net.Conn) *ClientV1 {
	return &ClientV1{
		Conn: conn,
	}
}

func (c *ClientV1) String() string {
	return c.RemoteAddr().String()
}
