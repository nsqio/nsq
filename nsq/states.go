package nsq

const (
	StateInit = iota
	StateDisconnected
	StateConnected
	StateSubscribed
	StateClosing // close has started. responses are ok, but no new messages will be sent
)
