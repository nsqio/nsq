package nsqd

import (
	"io"
	"net"
	"sync"

	"github.com/nsqio/nsq/internal/protocol"
)

const (
	typeConsumer = iota
	typeProducer
)

type Client interface {
	Type() int
	Stats(string) ClientStats
}

type tcpServer struct {
	nsqd *NSQD
	// conns holds fully handshaked clients, keyed by remote addr
	conns sync.Map
	// pendingConns holds raw connections that have been accepted but have
	// not yet completed the protocol handshake (read their 4-byte magic),
	// keyed by remote addr. Close() must be able to reach these too, since
	// a connection can sit here indefinitely if the client never sends
	// data.
	pendingConns sync.Map
}

func (p *tcpServer) Handle(conn net.Conn) {
	p.nsqd.logf(LOG_INFO, "TCP: new client(%s)", conn.RemoteAddr())

	p.pendingConns.Store(conn.RemoteAddr(), conn)

	// The client should initialize itself by sending a 4 byte sequence indicating
	// the version of the protocol that it intends to communicate, this will allow us
	// to gracefully upgrade the protocol away from text/line oriented to whatever...
	buf := make([]byte, 4)
	_, err := io.ReadFull(conn, buf)
	if err != nil {
		p.pendingConns.Delete(conn.RemoteAddr())
		p.nsqd.logf(LOG_ERROR, "failed to read protocol version - %s", err)
		_ = conn.Close()
		return
	}
	protocolMagic := string(buf)

	p.nsqd.logf(LOG_INFO, "CLIENT(%s): desired protocol magic '%s'",
		conn.RemoteAddr(), protocolMagic)

	var prot protocol.Protocol
	switch protocolMagic {
	case "  V2":
		prot = &protocolV2{nsqd: p.nsqd}
	default:
		p.pendingConns.Delete(conn.RemoteAddr())
		_, _ = protocol.SendFramedResponse(conn, frameTypeError, []byte("E_BAD_PROTOCOL"))
		_ = conn.Close()
		p.nsqd.logf(LOG_ERROR, "client(%s) bad protocol magic '%s'",
			conn.RemoteAddr(), protocolMagic)
		return
	}

	client := prot.NewClient(conn)
	p.conns.Store(conn.RemoteAddr(), client)
	p.pendingConns.Delete(conn.RemoteAddr())

	err = prot.IOLoop(client)
	if err != nil {
		p.nsqd.logf(LOG_ERROR, "client(%s) - %s", conn.RemoteAddr(), err)
	}

	p.conns.Delete(conn.RemoteAddr())
	_ = client.Close()
}

func (p *tcpServer) Close() {
	p.conns.Range(func(k, v interface{}) bool {
		_ = v.(protocol.Client).Close()
		return true
	})
	p.pendingConns.Range(func(k, v interface{}) bool {
		_ = v.(net.Conn).Close()
		return true
	})
}
