package nsq

const (
	ClientStateV1Init         = 0
	ClientStateV1WaitGet      = 1
	ClientStateV1WaitResponse = 2
)

var (
	ClientErrV1Invalid    = ClientError{"E_INVALID"}
	ClientErrV1BadTopic   = ClientError{"E_BAD_TOPIC"}
	ClientErrV1BadChannel = ClientError{"E_BAD_CHANNEL"}
	ClientErrV1BadMessage = ClientError{"E_BAD_MESSAGE"}
)

type ProtocolV1 interface {
	Protocol
	SUB(client StatefulReadWriter, params []string) ([]byte, error)
	GET(client StatefulReadWriter, params []string) ([]byte, error)
	FIN(client StatefulReadWriter, params []string) ([]byte, error)
	REQ(client StatefulReadWriter, params []string) ([]byte, error)
	PUB(client StatefulReadWriter, params []string) ([]byte, error)
}
