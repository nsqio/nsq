package nsq

type DescriptiveError interface {
	Description() string
}

// ClientErr provides a way for NSQ daemons to log a human reabable
// error string and return a machine readable string to the client.
//
//     E_INVALID
//     E_BAD_PROTOCOL
//     E_BAD_TOPIC
//     E_BAD_CHANNEL
//     E_BAD_BODY
//     E_REQ_FAILED
//     E_FIN_FAILED
//     E_PUT_FAILED
//     E_MISSING_PARAMS
type ClientErr struct {
	Err  string
	Desc string
}

// Error returns the machine readable form
func (e *ClientErr) Error() string {
	return e.Err
}

// Description return the human readable form
func (e *ClientErr) Description() string {
	return e.Desc
}

// NewClientErr creates a ClientErr with the supplied human and machine readable strings
func NewClientErr(err string, description string) *ClientErr {
	return &ClientErr{err, description}
}

type FatalClientErr struct {
	Err  string
	Desc string
}

// Error returns the machine readable form
func (e *FatalClientErr) Error() string {
	return e.Err
}

// Description return the human readable form
func (e *FatalClientErr) Description() string {
	return e.Desc
}

// NewClientErr creates a ClientErr with the supplied human and machine readable strings
func NewFatalClientErr(err string, description string) *FatalClientErr {
	return &FatalClientErr{err, description}
}
