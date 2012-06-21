package nsq

const LookupProtocolV1Magic = "  V1"

const (
	LookupClientStateV1Init = 0
)

var (
	LookupClientErrV1Invalid = ClientError{"E_INVALID"}
)
