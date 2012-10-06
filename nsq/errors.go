package nsq

// E_INVALID
// E_BAD_PROTOCOL
// E_BAD_TOPIC
// E_BAD_CHANNEL
// E_BAD_BODY
// E_REQ_FAILED
// E_FIN_FAILED
// E_PUT_FAILED
// E_MISSING_PARAMS

type ClientErr struct {
	Err  string
	Desc string
}

func (e *ClientErr) Error() string {
	return e.Err
}

func (e *ClientErr) Description() string {
	return e.Desc
}

func NewClientErr(err string, description string) *ClientErr {
	return &ClientErr{err, description}
}
