package nsqd

import (
	"fmt"
	"io"
	"net"

	"github.com/bitly/nsq/util"
)

type tcpServer struct {
	context *context
}

func (p *tcpServer) Handle(clientConn net.Conn) {
	p.context.l.Output(2, fmt.Sprintf("TCP: new client(%s)", clientConn.RemoteAddr()))

	// The client should initialize itself by sending a 4 byte sequence indicating
	// the version of the protocol that it intends to communicate, this will allow us
	// to gracefully upgrade the protocol away from text/line oriented to whatever...
	buf := make([]byte, 4)
	_, err := io.ReadFull(clientConn, buf)
	if err != nil {
		p.context.l.Output(2, fmt.Sprintf("ERROR: failed to read protocol version - %s", err))
		return
	}
	protocolMagic := string(buf)

	p.context.l.Output(2, fmt.Sprintf(
		"CLIENT(%s): desired protocol magic '%s'", clientConn.RemoteAddr(), protocolMagic))

	var prot util.Protocol
	switch protocolMagic {
	case "  V2":
		prot = &protocolV2{context: p.context}
	default:
		util.SendFramedResponse(clientConn, frameTypeError, []byte("E_BAD_PROTOCOL"))
		clientConn.Close()
		p.context.l.Output(2, fmt.Sprintf(
			"ERROR: client(%s) bad protocol magic '%s'", clientConn.RemoteAddr(), protocolMagic))
		return
	}

	err = prot.IOLoop(clientConn)
	if err != nil {
		p.context.l.Output(2, fmt.Sprintf("ERROR: client(%s) - %s", clientConn.RemoteAddr(), err))
		return
	}
}
