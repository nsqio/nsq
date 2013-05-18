package main

import (
	"bytes"
	"container/list"
	"encoding/json"
	"errors"
	"github.com/bitly/go-nsq"
	"net"
	"sync/atomic"
	"time"
)

// PeerInfo contains metadata for a LookupPeer instance (and is JSON marshalable)
type PeerInfo struct {
	TcpPort          int    `json:"tcp_port"`
	HttpPort         int    `json:"http_port"`
	Version          string `json:"version"`
	Address          string `json:"address"` //TODO: remove for 1.0
	BroadcastAddress string `json:"broadcast_address"`
}

// LookupPeer is a low-level type for connecting/reading/writing to nsqlookupd
//
// A LookupPeer instance is designed to connect lazily to nsqlookupd and reconnect
// gracefully (i.e. it is all handled by the library).  Clients can simply use the
// Command interface to perform a round-trip.
type LookupPeer struct {
	Info            PeerInfo
	identifyData    interface{}
	heartbeatChan   chan *LookupPeer
	messageChan     chan *nsq.Message
	addr            string
	conn            *peerConnection
	state           int32
	connectCallback func(*LookupPeer) error
}

type peerConnection struct {
	net.Conn
	exitChan        chan int
	dataChan        chan []byte
	transactionChan chan *lookupTransaction
	transactions    *list.List
}

type lookupTransaction struct {
	doneChan  chan int
	cmd       *nsq.Command
	frameType int32
	data      []byte
	err       error
}

// NewLookupPeer creates a new LookupPeer instance connecting to the supplied address.
//
// The supplied connectCallback will be called *every* time the instance connects.
func NewLookupPeer(addr string, heartbeatChan chan *LookupPeer, messageChan chan *nsq.Message,
	identifyData interface{},
	connectCallback func(*LookupPeer) error) *LookupPeer {
	lp := &LookupPeer{
		identifyData:    identifyData,
		heartbeatChan:   heartbeatChan,
		messageChan:     messageChan,
		addr:            addr,
		state:           nsq.StateDisconnected,
		connectCallback: connectCallback,
	}
	return lp
}

// Connect will Dial the specified address, with timeouts
func (lp *LookupPeer) Connect() error {
	if !atomic.CompareAndSwapInt32(&lp.state, nsq.StateDisconnected, nsq.StateConnecting) {
		return nil
	}

	conn, err := net.DialTimeout("tcp", lp.addr, time.Second)
	if err != nil {
		return err
	}

	lp.conn = &peerConnection{
		Conn:            conn,
		exitChan:        make(chan int),
		dataChan:        make(chan []byte),
		transactionChan: make(chan *lookupTransaction),
		transactions:    list.New(),
	}
	lp.conn.Write(nsq.MagicV2)

	go lp.conn.readLoop(lp)
	go lp.conn.router(lp)

	atomic.StoreInt32(&lp.state, nsq.StateConnected)

	cmd, err := nsq.Identify(lp.identifyData)
	if err != nil {
		lp.Disconnect()
		return err
	}

	_, resp, err := lp.Command(cmd)
	if err != nil {
		lp.Disconnect()
		return err
	}

	err = json.Unmarshal(resp, &lp.Info)
	if err != nil {
		lp.Disconnect()
		return err
	}

	err = lp.connectCallback(lp)
	if err != nil {
		lp.Disconnect()
		return err
	}

	return nil
}

func (lp *LookupPeer) Disconnect() error {
	lp.conn.Close()
	close(lp.conn.exitChan)
	atomic.StoreInt32(&lp.state, nsq.StateDisconnected)
	return nil
}

// String returns the specified address
func (lp *LookupPeer) String() string {
	return lp.addr
}

// Command performs a round-trip for the specified Command.
//
// It will lazily connect to nsqlookupd and gracefully handle
// reconnecting in the event of a failure.
//
// It returns the response from nsqlookupd
func (lp *LookupPeer) Command(cmd *nsq.Command) (int32, []byte, error) {
	err := lp.Connect()
	if err != nil || atomic.LoadInt32(&lp.state) != nsq.StateConnected {
		return -1, nil, nil
	}

	t := &lookupTransaction{
		frameType: -1,
		doneChan:  make(chan int),
		cmd:       cmd,
	}
	lp.conn.transactionChan <- t
	<-t.doneChan

	if t.frameType == nsq.FrameTypeError {
		t.err = errors.New(string(t.data))
	}

	return t.frameType, t.data, t.err
}

func (c *peerConnection) readLoop(lp *LookupPeer) {
	for {
		c.SetReadDeadline(time.Now().Add(nsq.DefaultClientTimeout))
		data, err := nsq.ReadResponse(c)
		if err != nil {
			lp.Disconnect()
			return
		}

		select {
		case c.dataChan <- data:
		case <-c.exitChan:
			return
		}
	}
}

func (c *peerConnection) router(lp *LookupPeer) {
	for {
		select {
		case t := <-c.transactionChan:
			c.SetWriteDeadline(time.Now().Add(time.Second))
			err := t.cmd.Write(c)
			if err != nil {
				lp.Disconnect()
				t.err = err
				t.doneChan <- 1
				continue
			}
			c.transactions.PushBack(t)
		case buf := <-c.dataChan:
			frameType, data := nsq.UnpackResponse(buf)
			if frameType == nsq.FrameTypeResponse && bytes.Equal(data, []byte("_heartbeat_")) {
				select {
				case lp.heartbeatChan <- lp:
				default:
				}
				continue
			}

			el := c.transactions.Front()
			c.transactions.Remove(el)
			t := el.Value.(*lookupTransaction)
			t.frameType = frameType
			t.data = data
			t.doneChan <- 1
		case <-c.exitChan:
			goto exit
		}
	}

exit:
	// flush all pending transactions
	for e := c.transactions.Front(); e != nil; e = e.Next() {
		transaction := e.Value.(*lookupTransaction)
		transaction.frameType = -1
		transaction.data = nil
		transaction.doneChan <- 1
	}
}
