package nsq

const MaxNameLength = 32

// Protocol describes the basic behavior of any protocol in the system
type Protocol interface {
	IOLoop(client *ServerClient) error
}

// ClientError is a native protocol error type
type ClientError struct {
	errStr string
}

// Error returns the error as string
func (e ClientError) Error() string {
	return e.errStr
}

var (
	// the following errors should only be expected if there is
	// an error *up to and including* sending magic
	ClientErrInvalid     = ClientError{"E_INVALID"}
	ClientErrBadProtocol = ClientError{"E_BAD_PROTOCOL"}
)
