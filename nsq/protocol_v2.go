package nsq

const ProtocolV2Magic = "  V2"

const (
	ClientStateV2Init       = 0
	ClientStateV2Subscribed = 1
	ClientStateV2Closing    = 2 // close has started. responses are ok, but no new messages will be sent
)

const (
	FrameTypeResponse int32 = 0
	FrameTypeError    int32 = 1
	FrameTypeMessage  int32 = 2
)

var (
	ClientErrV2Invalid       = ClientError{"E_INVALID"}
	ClientErrV2BadTopic      = ClientError{"E_BAD_TOPIC"}
	ClientErrV2BadChannel    = ClientError{"E_BAD_CHANNEL"}
	ClientErrV2BadMessage    = ClientError{"E_BAD_MESSAGE"}
	ClientErrV2RequeueFailed = ClientError{"E_REQ_FAILED"}
	ClientErrV2FinishFailed  = ClientError{"E_FIN_FAILED"}
)
