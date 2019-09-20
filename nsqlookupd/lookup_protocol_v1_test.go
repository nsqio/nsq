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

func testIOLoopReturnsClientErr(t *testing.T, fakeConn test.FakeNetConn) {
	fakeConn.ReadFunc = func(b []byte) (int, error) {
		return copy(b, []byte("INVALID_COMMAND\n")), nil
	}

	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.LogLevel = LOG_DEBUG

	nsqlookupd, err := New(opts)
	test.Nil(t, err)
	prot := &LookupProtocolV1{ctx: &Context{nsqlookupd: nsqlookupd}}

	nsqlookupd.tcpServer = &tcpServer{ctx: prot.ctx}

	errChan := make(chan error)
	testIOLoop := func() {
		errChan <- prot.IOLoop(fakeConn)
		defer prot.ctx.nsqlookupd.Exit()
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
