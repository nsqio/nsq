package nsqlookupd

import (
	"errors"
	"testing"
	"time"

	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/internal/test"
)

func TestIOLoopReturnsClientErrWhenSendFails(t *testing.T) {
	fakeConn := test.NewFakeNetConn()
	fakeConn.WriteFunc = func(b []byte) (int, error) {
		return 0, errors.New("write error")
	}

	testIOLoopReturnsClientErr(t, fakeConn)
}

func TestIOLoopReturnsClientErrWhenSendSucceeds(t *testing.T) {
	fakeConn := test.NewFakeNetConn()
	fakeConn.WriteFunc = func(b []byte) (int, error) {
		return len(b), nil
	}

	testIOLoopReturnsClientErr(t, fakeConn)
}

// Test generated using Keploy
func TestIOLoopSendResponseError(t *testing.T) {
	fakeConn := test.NewFakeNetConn()

	// Simulate a valid command that will cause a response
	fakeConn.ReadFunc = func(b []byte) (int, error) {
		return copy(b, []byte("PING\n")), nil
	}

	// Simulate an error on write to cause SendResponse to fail
	firstWrite := true
	fakeConn.WriteFunc = func(b []byte) (int, error) {
		if firstWrite {
			firstWrite = false
			return len(b), nil
		}
		return 0, errors.New("write error")
	}

	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	nsqlookupd, err := New(opts)
	test.Nil(t, err)
	prot := &LookupProtocolV1{nsqlookupd: nsqlookupd}
	client := prot.NewClient(fakeConn)

	errChan := make(chan error)
	go func() {
		errChan <- prot.IOLoop(client)
	}()

	// Wait for IOLoop to exit
	var ioLoopErr error
	select {
	case ioLoopErr = <-errChan:
	case <-time.After(time.Second):
		t.Fatal("IOLoop didn't exit")
	}
	test.NotNil(t, ioLoopErr)
}

func testIOLoopReturnsClientErr(t *testing.T, fakeConn test.FakeNetConn) {
	fakeConn.ReadFunc = func(b []byte) (int, error) {
		return copy(b, []byte("INVALID_COMMAND\n")), nil
	}

	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.LogLevel = LOG_DEBUG
	opts.TCPAddress = "127.0.0.1:0"
	opts.HTTPAddress = "127.0.0.1:0"

	nsqlookupd, err := New(opts)
	test.Nil(t, err)
	prot := &LookupProtocolV1{nsqlookupd: nsqlookupd}

	nsqlookupd.tcpServer = &tcpServer{nsqlookupd: prot.nsqlookupd}

	errChan := make(chan error)
	testIOLoop := func() {
		client := prot.NewClient(fakeConn)
		errChan <- prot.IOLoop(client)
		defer prot.nsqlookupd.Exit()
	}
	go testIOLoop()

	var timeout bool

	select {
	case err = <-errChan:
	case <-time.After(2 * time.Second):
		timeout = true
	}

	test.Equal(t, false, timeout)

	test.NotNil(t, err)
	test.Equal(t, "E_INVALID invalid command INVALID_COMMAND", err.Error())
	test.NotNil(t, err.(*protocol.FatalClientErr))
}
