package nsqd

import (
	"time"
)

type IMsgTracer interface {
	Start()
	TracePub(topic string, traceID uint64, msg *Message, diskOffset BackendOffset, currentCnt int64)
	// state will be Start, Req, Fin, Timeout
	TraceSub(topic string, state string, traceID uint64, msg *Message, clientID string)
}

var nsqMsgTracer = &LogMsgTracer{}

// just print the trace log
type LogMsgTracer struct {
	MID string
}

func (self *LogMsgTracer) Start() {
}

func (self *LogMsgTracer) TracePub(topic string, traceID uint64, msg *Message, diskOffset BackendOffset, currentCnt int64) {
	nsqLog.Logf("[TRACE] topic %v trace id %v: message %v put at offset: %v, current count: %v at time %v", topic, msg.TraceID,
		msg.ID, diskOffset, currentCnt, time.Now().UnixNano())
}

func (self *LogMsgTracer) TraceSub(topic string, state string, traceID uint64, msg *Message, clientID string) {
	nsqLog.Logf("[TRACE] topic %v trace id %v: message %v (offset: %v) consume state %v from client %v at time: %v", topic, msg.TraceID,
		msg.ID, msg.offset, state, clientID, time.Now().UnixNano())
}

// this tracer will send the trace info to remote server for each seconds
type RemoteMsgTracer struct {
	// local machine id
	MID        string
	remoteAddr string
}

func (self *RemoteMsgTracer) Start() {
}

func (self *RemoteMsgTracer) TracePub(topic string, traceID uint64, msg *Message, diskOffset BackendOffset, currentCnt int64) {
}

func (self *RemoteMsgTracer) TraceSub(topic string, state string, traceID uint64, msg *Message, clientID string) {
}
