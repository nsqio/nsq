package nsq

const ProtocolV1Magic = "  V1"

const (
	ClientStateV1Init = 0
)

var (
	ClientErrV1Invalid  = ClientError{"E_INVALID"}
	ClientErrV1BadTopic = ClientError{"E_BAD_TOPIC"}
)
