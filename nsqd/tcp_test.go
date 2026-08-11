package nsqd

import (
	"net"
	"testing"
	"testing/synctest"

	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/internal/test"
)

type fakeAddr struct{}

func (fakeAddr) Network() string { return "fake" }
func (fakeAddr) String() string  { return "fake" }

// fakeListener is a net.Listener backed by net.Pipe instead of real sockets,
// so it (and everything it hands out) stays inside a synctest bubble.
type fakeListener struct {
	conns  chan net.Conn
	closed chan struct{}
}

func newFakeListener() *fakeListener {
	return &fakeListener{
		conns:  make(chan net.Conn),
		closed: make(chan struct{}),
	}
}

// dial simulates an inbound connection, handing the server side to Accept()
// and returning the client side to the caller.
func (l *fakeListener) dial() net.Conn {
	client, server := net.Pipe()
	l.conns <- server
	return client
}

func (l *fakeListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *fakeListener) Close() error {
	close(l.closed)
	return nil
}

func (l *fakeListener) Addr() net.Addr { return fakeAddr{} }

// TestTCPServerShutdownHangsOnSlowClient reproduces a bug where a TCP client
// that connects but never sends the 4-byte protocol magic prevents nsqd's
// TCP server from ever shutting down.
//
// protocol.TCPServer's WaitGroup counts a connection as soon as it is
// Accept()'d, but tcpServer.Handle() only registers the connection in the
// `conns` map (the thing Close() iterates to force-close outstanding
// connections) after it successfully reads the 4-byte protocol magic via
// io.ReadFull. No read deadline is set on the connection before that read,
// so a client that never sends data leaves the connection stuck in
// io.ReadFull forever, invisible to Close(). protocol.TCPServer's
// wg.Wait() - which nsqd.Exit() blocks on via n.waitGroup.Wait() - then
// never returns.
//
// Using testing/synctest lets this be shown deterministically: net.Pipe
// conns block on plain channels, so once every goroutine is durably
// blocked, synctest.Wait returns immediately instead of racing a wall-clock
// timeout, and we can assert TCPServer is still stuck rather than merely
// inferring it from a timeout firing.
func TestTCPServerShutdownHangsOnSlowClient(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		opts := NewOptions()
		opts.Logger = test.NewTestLogger(t)
		opts.DataPath = t.TempDir()
		n, err := New(opts)
		test.Nil(t, err)
		_ = n.tcpListener.Close()
		if n.httpListener != nil {
			_ = n.httpListener.Close()
		}
		defer func() { _ = n.dl.Unlock() }()

		ln := newFakeListener()
		serverDone := make(chan error, 1)
		go func() {
			serverDone <- protocol.TCPServer(ln, n.tcpServer, n.logf)
		}()

		// connect but deliberately never send the 4-byte protocol magic
		client := ln.dial()
		defer func() { _ = client.Close() }()

		// wait for Handle() to accept the connection and durably block
		// inside io.ReadFull, and for TCPServer's accept loop to durably
		// block waiting for the next connection or a close
		synctest.Wait()

		// simulate nsqd.Exit(): stop new accepts, then close all *known*
		// connections
		_ = ln.Close()
		n.tcpServer.Close()

		// let TCPServer's accept loop unblock, break out, and reach wg.Wait()
		synctest.Wait()

		select {
		case err := <-serverDone:
			if err != nil {
				t.Fatalf("TCPServer returned an unexpected error: %v", err)
			}
		default:
			t.Fatal("TCPServer is still blocked in wg.Wait(): the stalled " +
				"connection was never registered in tcpServer.conns, so " +
				"Close() could not close it (see tcp_server.go / tcp.go)")
		}
	})
}
