package protocol

import (
	"net"
	"testing"
	"time"

	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/test"
)

const testTimeout = 5 * time.Second

func nopLogf(lvl lg.LogLevel, f string, args ...interface{}) {}

// recordingHandler reports every accepted connection on a channel and then
// closes it, so a test can observe how many connections were dispatched.
type recordingHandler struct {
	conns chan net.Conn
}

func (h *recordingHandler) Handle(conn net.Conn) {
	h.conns <- conn
	conn.Close()
}

func newTestListener(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	test.Nil(t, err)
	return listener
}

func TestTCPServerDispatchesConnections(t *testing.T) {
	listener := newTestListener(t)
	handler := &recordingHandler{conns: make(chan net.Conn, 8)}

	done := make(chan error, 1)
	go func() {
		done <- TCPServer(listener, handler, nopLogf)
	}()

	const numConns = 3
	for i := 0; i < numConns; i++ {
		conn, err := net.Dial("tcp", listener.Addr().String())
		test.Nil(t, err)
		conn.Close()
	}

	for i := 0; i < numConns; i++ {
		select {
		case <-handler.conns:
		case <-time.After(testTimeout):
			t.Fatalf("timed out waiting for connection %d to be handled", i+1)
		}
	}

	// closing the listener ends the accept loop and TCPServer returns cleanly
	listener.Close()
	select {
	case err := <-done:
		test.Nil(t, err)
	case <-time.After(testTimeout):
		t.Fatal("TCPServer did not return after the listener was closed")
	}
}

func TestTCPServerReturnsOnClosedListener(t *testing.T) {
	listener := newTestListener(t)
	handler := &recordingHandler{conns: make(chan net.Conn, 1)}

	done := make(chan error, 1)
	go func() {
		done <- TCPServer(listener, handler, nopLogf)
	}()

	listener.Close()

	select {
	case err := <-done:
		// a closed listener is a normal shutdown, not an error
		test.Nil(t, err)
	case <-time.After(testTimeout):
		t.Fatal("TCPServer did not return after the listener was closed")
	}
}

func TestTCPServerWaitsForHandlers(t *testing.T) {
	listener := newTestListener(t)

	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	handlerFinished := make(chan struct{})

	done := make(chan error, 1)
	go func() {
		done <- TCPServer(listener, &blockingHandler{
			started:  handlerStarted,
			release:  releaseHandler,
			finished: handlerFinished,
		}, nopLogf)
	}()

	conn, err := net.Dial("tcp", listener.Addr().String())
	test.Nil(t, err)
	defer conn.Close()

	select {
	case <-handlerStarted:
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for the handler to start")
	}

	listener.Close()

	// TCPServer must not return while a handler is still running
	select {
	case <-done:
		t.Fatal("TCPServer returned before the in-flight handler completed")
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseHandler)

	select {
	case <-handlerFinished:
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for the handler to finish")
	}

	select {
	case err := <-done:
		test.Nil(t, err)
	case <-time.After(testTimeout):
		t.Fatal("TCPServer did not return after the handler completed")
	}
}

// blockingHandler holds a connection open until it is explicitly released,
// which lets a test observe TCPServer waiting on in-flight handlers.
type blockingHandler struct {
	started  chan struct{}
	release  chan struct{}
	finished chan struct{}
}

func (h *blockingHandler) Handle(conn net.Conn) {
	close(h.started)
	<-h.release
	conn.Close()
	close(h.finished)
}
