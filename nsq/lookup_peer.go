package nsq

import (
	"log"
	"net"
	"time"
)

// LookupPeer is a low-level type for connecting/reading/writing to nsqlookupd
type LookupPeer struct {
	addr            string
	conn            net.Conn
	state           int32
	connectCallback func(*LookupPeer)
	PeerInfo        PeerInfo
}

type PeerInfo struct {
	TcpPort  int    `json:"tcp_port"`
	HttpPort int    `json:"http_port"`
	Version  string `json:"version"`
	Address  string `json:"address"`
}

// NewLookupPeer creates a new LookupPeer instance
func NewLookupPeer(addr string, connectCallback func(*LookupPeer)) *LookupPeer {
	return &LookupPeer{
		addr:            addr,
		state:           StateDisconnected,
		connectCallback: connectCallback,
	}
}

func (lp *LookupPeer) Connect() error {
	log.Printf("LOOKUP connecting to %s", lp.addr)
	conn, err := net.DialTimeout("tcp", lp.addr, time.Second)
	if err != nil {
		return err
	}
	lp.conn = conn
	return nil
}

func (lp *LookupPeer) String() string {
	return lp.addr
}

func (lp *LookupPeer) Read(data []byte) (int, error) {
	lp.conn.SetReadDeadline(time.Now().Add(time.Second))
	return lp.conn.Read(data)
}

func (lp *LookupPeer) Write(data []byte) (int, error) {
	lp.conn.SetWriteDeadline(time.Now().Add(time.Second))
	return lp.conn.Write(data)
}

func (lp *LookupPeer) Close() error {
	return lp.conn.Close()
}

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
		lp.state = StateDisconnected
		return nil, err
	}
	resp, err := ReadResponse(lp)
	if err != nil {
		lp.Close()
		lp.state = StateDisconnected
		return nil, err
	}
	return resp, nil
}
