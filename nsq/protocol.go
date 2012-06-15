package nsq

import (
	"io"
	"reflect"
	"strings"
)

type StatefulReadWriter interface {
	io.ReadWriter
	GetState(key string) (interface{}, bool)
	SetState(key string, val interface{})
	String() string
}

type Protocol interface {
	IOLoop(client StatefulReadWriter) error
	Execute(client StatefulReadWriter, params ...string) ([]byte, error)
}

type ClientError struct {
	errStr string
}

func (e ClientError) Error() string {
	return e.errStr
}

var (
	ClientErrInvalid = ClientError{"E_INVALID"}
	ClientErrBadProtocol = ClientError{"E_BAD_PROTOCOL"}
)

func ProtocolExecute(p interface{}, client StatefulReadWriter, params ...string) ([]byte, error) {
	var err error
	var response []byte

	typ := reflect.TypeOf(p)
	args := make([]reflect.Value, 3)
	args[0] = reflect.ValueOf(p)
	args[1] = reflect.ValueOf(client)

	cmd := strings.ToUpper(params[0])

	// use reflection to call the appropriate method for this 
	// command on the protocol object
	if method, ok := typ.MethodByName(cmd); ok {
		args[2] = reflect.ValueOf(params)
		returnValues := method.Func.Call(args)
		response = nil
		if !returnValues[0].IsNil() {
			response = returnValues[0].Interface().([]byte)
		}
		err = nil
		if !returnValues[1].IsNil() {
			err = returnValues[1].Interface().(error)
		}

		return response, err
	}

	return nil, ClientErrInvalid
}
