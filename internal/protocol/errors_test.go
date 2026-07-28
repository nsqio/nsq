package protocol

import (
	"errors"
	"testing"

	"github.com/nsqio/nsq/internal/test"
)

func TestClientErr(t *testing.T) {
	parent := errors.New("underlying failure")
	err := NewClientErr(parent, "E_INVALID", "invalid message body")

	test.Equal(t, "E_INVALID invalid message body", err.Error())
	test.Equal(t, parent, err.Parent())
	test.Equal(t, "E_INVALID", err.Code)
	test.Equal(t, "invalid message body", err.Desc)
	test.Equal(t, parent, err.ParentErr)
}

func TestClientErrNilParent(t *testing.T) {
	err := NewClientErr(nil, "E_BAD_TOPIC", "topic name is not valid")

	test.Equal(t, "E_BAD_TOPIC topic name is not valid", err.Error())
	test.Nil(t, err.Parent())
}

func TestClientErrEmptyDesc(t *testing.T) {
	// the machine readable form always joins code and description with a
	// single space, even when the description is empty
	err := NewClientErr(nil, "E_INVALID", "")
	test.Equal(t, "E_INVALID ", err.Error())
}

func TestFatalClientErr(t *testing.T) {
	parent := errors.New("underlying failure")
	err := NewFatalClientErr(parent, "E_BAD_MESSAGE", "message too big")

	test.Equal(t, "E_BAD_MESSAGE message too big", err.Error())
	test.Equal(t, parent, err.Parent())
	test.Equal(t, "E_BAD_MESSAGE", err.Code)
	test.Equal(t, "message too big", err.Desc)
	test.Equal(t, parent, err.ParentErr)
}

func TestFatalClientErrNilParent(t *testing.T) {
	err := NewFatalClientErr(nil, "E_BAD_BODY", "body too big")

	test.Equal(t, "E_BAD_BODY body too big", err.Error())
	test.Nil(t, err.Parent())
}

// both error types are consumed through the error and ChildErr interfaces,
// so pin that they satisfy each
func TestClientErrsImplementInterfaces(t *testing.T) {
	var err error = NewClientErr(nil, "E_INVALID", "invalid")
	test.Equal(t, "E_INVALID invalid", err.Error())

	var fatal error = NewFatalClientErr(nil, "E_INVALID", "invalid")
	test.Equal(t, "E_INVALID invalid", fatal.Error())

	child, ok := err.(ChildErr)
	test.Equal(t, true, ok)
	test.Nil(t, child.Parent())

	fatalChild, ok := fatal.(ChildErr)
	test.Equal(t, true, ok)
	test.Nil(t, fatalChild.Parent())
}

func TestClientErrParentRoundTrip(t *testing.T) {
	// a ClientErr may wrap another ClientErr; Parent() should hand back the
	// wrapped value untouched
	inner := NewClientErr(nil, "E_INNER", "inner")
	outer := NewFatalClientErr(inner, "E_OUTER", "outer")

	test.Equal(t, "E_OUTER outer", outer.Error())

	parent, ok := outer.Parent().(*ClientErr)
	test.Equal(t, true, ok)
	test.Equal(t, "E_INNER inner", parent.Error())
}
