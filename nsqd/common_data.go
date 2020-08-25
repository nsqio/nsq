package nsqd

const (
	NoTopicFound = "No_Topic_Found"
	SuccessCode  = 0
	SuccessMsg   = "success"
	ErrorCode    = 1
)

type topicInfoRsp struct {
	Code int         `json:"code"`
	Msg  string      `json:"msg"`
	Data interface{} `json:"data"`
}

type topicData struct {
	TopicDepth   int64          `json:"topicDepth"`
	MessageCount uint64         `json:"messageCount"`
	ConsumerInfo consumeInfoRsp `json:"consumerInfo"`
}

type consumeInfoRsp struct {
	AllChannelCount int                      `json:"allChannelCount"`
	ConsumeInfo     map[string]consumeClient `json:"consumeInfo"`
}

type consumeClient struct {
	ClientNumbers int    `json:"clientNumbers"`
	Depth         int64  `json:"Depth"`
	MessageCount  uint64 `json:"messageCount"`
}
