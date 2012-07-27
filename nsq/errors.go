package nsq

import (
	"errors"
)

var (
	ClientErrInvalid       = errors.New("E_INVALID")
	ClientErrBadProtocol   = errors.New("E_BAD_PROTOCOL")
	ClientErrBadTopic      = errors.New("E_BAD_TOPIC")
	ClientErrBadChannel    = errors.New("E_BAD_CHANNEL")
	ClientErrBadMessage    = errors.New("E_BAD_MESSAGE")
	ClientErrRequeueFailed = errors.New("E_REQ_FAILED")
	ClientErrFinishFailed  = errors.New("E_FIN_FAILED")
	ClientErrPutFailed     = errors.New("E_PUT_FAILED")
)
