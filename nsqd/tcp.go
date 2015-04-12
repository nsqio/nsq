package nsqd

import (
	"io"
	"net"

	"github.com/nsqio/nsq/internal/protocol"
)

type tcpServer struct {
	ctx *context
}

func (p *tcpServer) Handle(clientConn net.Conn) {
	p.ctx.nsqd.logf("TCP: new client(%s)", clientConn.RemoteAddr())

	// The client should initialize itself by sending a 4 byte sequence indicating
	// the version of the protocol that it intends to communicate, this will allow us
	// to gracefully upgrade the protocol away from text/line oriented to whatever...
	buf := make([]byte, 4)
	_, err := io.ReadFull(clientConn, buf)
	if err != nil {
		p.ctx.nsqd.logf("ERROR: failed to read protocol version - %s", err)
		return
	}
	protocolMagic := string(buf)

	p.ctx.nsqd.logf("CLIENT(%s): desired protocol magic '%s'",
		clientConn.RemoteAddr(), protocolMagic)

	var prot protocol.Protocol
	switch protocolMagic {
	case "  V2":
		prot = &protocolV2{ctx: p.ctx}
	default:
		protocol.SendFramedResponse(clientConn, frameTypeError, []byte("E_BAD_PROTOCOL"))
		clientConn.Close()
		p.ctx.nsqd.logf("ERROR: client(%s) bad protocol magic '%s'",
			clientConn.RemoteAddr(), protocolMagic)
		return
	}

	err = prot.IOLoop(clientConn)
	if err != nil {
		p.ctx.nsqd.logf("ERROR: client(%s) - %s", clientConn.RemoteAddr(), err)
		return
	}
}
