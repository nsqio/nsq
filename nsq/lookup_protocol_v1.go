package nsq

const LookupProtocolV1Magic = "  V1"

const (
	LookupClientStateV1Init = 0
)

const (
	LookupFrameTypeChannel = 0
)

var (
	LookupClientErrV1Invalid = ClientError{"E_INVALID"}
)

type LookupProtocolV1 interface {
	Protocol
	ANNOUNCE(client StatefulReadWriter, params []string) ([]byte, error)
}
