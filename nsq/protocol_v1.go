package nsq

const ProtocolV1Magic = "  V1"

const (
	ClientStateV1Init = 0
)

var (
	ClientErrV1Invalid = ClientError{"E_INVALID"}
)

type ProtocolV1 interface {
	Protocol
	PUB(client StatefulReadWriter, params []string) ([]byte, error)
}
