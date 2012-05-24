package nsq

const ProtocolV2Magic = "  V2"

const (
	ClientStateV2Init       = 0
	ClientStateV2Subscribed = 1
)

const (
	FrameTypeResponse = 0
	FrameTypeError    = 1
	FrameTypeMessage  = 2
)

var (
	ClientErrV2Invalid    = ClientError{"E_INVALID"}
	ClientErrV2BadTopic   = ClientError{"E_BAD_TOPIC"}
	ClientErrV2BadChannel = ClientError{"E_BAD_CHANNEL"}
	ClientErrV2BadMessage = ClientError{"E_BAD_MESSAGE"}
)

type ProtocolV2 interface {
	Protocol
	SUB(client StatefulReadWriter, params []string) ([]byte, error)
	RDY(client StatefulReadWriter, params []string) ([]byte, error)
	FIN(client StatefulReadWriter, params []string) ([]byte, error)
	REQ(client StatefulReadWriter, params []string) ([]byte, error)
}
