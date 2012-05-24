package nsq

const LookupProtocolV1Magic = "  V1"

const (
	LookupClientStateV1Init         = 0
)

var (
	LookupClientErrV1Invalid    = ClientError{"E_INVALID"}
)

type LookupProtocolV1 interface {
	Protocol
	GET(client StatefulReadWriter, params []string) ([]byte, error)
	SET(client StatefulReadWriter, params []string) ([]byte, error)
}
