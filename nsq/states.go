package nsq

const (
	StateInit = iota
	StateDisconnected
	StateConnected
	StateSubscribed
	// close has started. responses are ok, but no new messages will be sent
	StateClosing
)
