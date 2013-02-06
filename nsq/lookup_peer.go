package nsq

import (
	"log"
	"net"
	"time"
)

// LookupPeer is a low-level type for connecting/reading/writing to nsqlookupd
//
// A LookupPeer instance is designed to connect lazily to nsqlookupd and reconnect
// gracefully (i.e. it is all handled by the library).  Clients can simply use the
// Command interface to perform a round-trip.
type LookupPeer struct {
	addr            string
	conn            net.Conn
	state           int32
	connectCallback func(*LookupPeer)
	Info            PeerInfo
}

// PeerInfo contains metadata for a LookupPeer instance (and is JSON marshalable)
type PeerInfo struct {
	TcpPort          int    `json:"tcp_port"`
	HttpPort         int    `json:"http_port"`
	Version          string `json:"version"`
	Address          string `json:"address"` //TODO: remove for 1.0
	BroadcastAddress string `json:"broadcast_address"`
}

// NewLookupPeer creates a new LookupPeer instance connecting to the supplied address.
//
// The supplied connectCallback will be called *every* time the instance connects.
func NewLookupPeer(addr string, connectCallback func(*LookupPeer)) *LookupPeer {
	return &LookupPeer{
		addr:            addr,
		state:           StateDisconnected,
		connectCallback: connectCallback,
	}
}

// Connect will Dial the specified address, with timeouts
func (lp *LookupPeer) Connect() error {
	log.Printf("LOOKUP connecting to %s", lp.addr)
	conn, err := net.DialTimeout("tcp", lp.addr, time.Second)
	if err != nil {
		return err
	}
	lp.conn = conn
	return nil
}

// String returns the specified address
func (lp *LookupPeer) String() string {
	return lp.addr
}

// Read implements the io.Reader interface, adding deadlines
func (lp *LookupPeer) Read(data []byte) (int, error) {
	lp.conn.SetReadDeadline(time.Now().Add(time.Second))
	return lp.conn.Read(data)
}

// Write implements the io.Writer interface, adding deadlines
func (lp *LookupPeer) Write(data []byte) (int, error) {
	lp.conn.SetWriteDeadline(time.Now().Add(time.Second))
	return lp.conn.Write(data)
}

// Close implements the io.Closer interface
func (lp *LookupPeer) Close() error {
	lp.state = StateDisconnected
	return lp.conn.Close()
}

// Command performs a round-trip for the specified Command.
//
// It will lazily connect to nsqlookupd and gracefully handle
// reconnecting in the event of a failure.
//
// It returns the response from nsqlookupd as []byte
func (lp *LookupPeer) Command(cmd *Command) ([]byte, error) {
	initialState := lp.state
	if lp.state != StateConnected {
		err := lp.Connect()
		if err != nil {
			return nil, err
		}
		lp.state = StateConnected
		lp.Write(MagicV1)
		if initialState == StateDisconnected {
			lp.connectCallback(lp)
		}
	}
	if cmd == nil {
		return nil, nil
	}
	err := cmd.Write(lp)
	if err != nil {
		lp.Close()
		return nil, err
	}
	resp, err := ReadResponse(lp)
	if err != nil {
		lp.Close()
		return nil, err
	}
	return resp, nil
}
