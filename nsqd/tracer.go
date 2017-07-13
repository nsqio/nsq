package nsqd

import (
	"fmt"
	"github.com/absolute8511/nsq/internal/flume_log"
	"github.com/absolute8511/nsq/internal/levellogger"
	"time"
)

const (
	traceModule = "msgtracer"
)

type IMsgTracer interface {
	Start()
	TracePub(topic string, part int, pubMethod string, traceID uint64, msg *Message, diskOffset BackendOffset, currentCnt int64)
	// state will be READ_QUEUE, Start, Req, Fin, Timeout
	TraceSub(topic string, channel string, state string, traceID uint64, msg *Message, clientID string)
}

var nsqMsgTracer IMsgTracer

type TraceLogItemInfo struct {
	MsgID     uint64 `json:"msgid"`
	TraceID   uint64 `json:"traceid"`
	Topic     string `json:"topic"`
	Channel   string `json:"channel"`
	Timestamp int64  `json:"timestamp"`
	Action    string `json:"action"`
}

func SetRemoteMsgTracer(remote string) {
	if remote != "" {
		nsqMsgTracer = NewRemoteMsgTracer(remote)
	}
}

// just print the trace log
type LogMsgTracer struct {
	MID string
}

func (self *LogMsgTracer) Start() {
}

func (self *LogMsgTracer) TracePub(topic string, part int, pubMethod string, traceID uint64, msg *Message, diskOffset BackendOffset, currentCnt int64) {
	nsqLog.Logf("[TRACE] topic %v-%v trace id %v: message %v put %v at offset: %v, current count: %v at time %v, delayed: %v", topic, part, msg.TraceID,
		msg.ID, pubMethod, diskOffset, currentCnt, time.Now().UnixNano(), msg.DelayedTs)
}

func (self *LogMsgTracer) TraceSub(topic string, channel string, state string, traceID uint64, msg *Message, clientID string) {
	nsqLog.Logf("[TRACE] topic %v channel %v trace id %v: message %v (offset: %v) consume state %v from client %v(%v) at time: %v, attempt: %v",
		topic, channel, msg.TraceID,
		msg.ID, msg.Offset, state, clientID, msg.GetClientID(), time.Now().UnixNano(), msg.Attempts)
}

// this tracer will send the trace info to remote server for each seconds
type RemoteMsgTracer struct {
	remoteAddr   string
	remoteLogger *flume_log.FlumeLogger
	localTracer  *LogMsgTracer
}

func NewRemoteMsgTracer(remote string) IMsgTracer {
	return &RemoteMsgTracer{
		remoteAddr:   remote,
		remoteLogger: flume_log.NewFlumeLoggerWithAddr(remote),
		localTracer:  &LogMsgTracer{},
	}
}

func (self *RemoteMsgTracer) Start() {
	self.localTracer.Start()
}

func (self *RemoteMsgTracer) Stop() {
	self.remoteLogger.Stop()
}

func (self *RemoteMsgTracer) TracePub(topic string, part int, pubMethod string, traceID uint64, msg *Message, diskOffset BackendOffset, currentCnt int64) {
	now := time.Now().UnixNano()
	detail := flume_log.NewDetailInfo(traceModule)
	var traceItem [1]TraceLogItemInfo
	traceItem[0].MsgID = uint64(msg.ID)
	traceItem[0].TraceID = msg.TraceID
	traceItem[0].Topic = topic
	traceItem[0].Timestamp = now
	traceItem[0].Action = pubMethod
	detail.SetExtraInfo(traceItem[:])

	l := fmt.Sprintf("[TRACE] topic %v-%v trace id %v: message %v put at offset: %v, current count: %v at time %v, delayed to %v", topic, part, msg.TraceID,
		msg.ID, diskOffset, currentCnt, now, msg.DelayedTs)
	err := self.remoteLogger.Info(l, detail)
	if err != nil || nsqLog.Level() >= levellogger.LOG_DEBUG {
		if err != nil {
			nsqLog.Warningf("send log to remote error: %v", err)
		}
		self.localTracer.TracePub(topic, part, pubMethod, traceID, msg, diskOffset, currentCnt)
	}
}

func (self *RemoteMsgTracer) TraceSub(topic string, channel string, state string, traceID uint64, msg *Message, clientID string) {
	now := time.Now().UnixNano()
	var traceItem [1]TraceLogItemInfo
	traceItem[0].MsgID = uint64(msg.ID)
	traceItem[0].TraceID = msg.TraceID
	traceItem[0].Topic = topic
	traceItem[0].Channel = channel
	traceItem[0].Timestamp = now
	traceItem[0].Action = state
	detail := flume_log.NewDetailInfo(traceModule)
	detail.SetExtraInfo(traceItem[:])

	l := fmt.Sprintf("[TRACE] topic %v channel %v trace id %v: message %v (offset: %v) consume state %v from client %v(%v) at time: %v, attempt: %v",
		topic, channel, msg.TraceID, msg.ID, msg.Offset, state, clientID, msg.GetClientID(), time.Now().UnixNano(), msg.Attempts)
	err := self.remoteLogger.Info(l, detail)
	if err != nil || nsqLog.Level() >= levellogger.LOG_DEBUG {
		if err != nil {
			nsqLog.Warningf("send log to remote error: %v", err)
		}
		self.localTracer.TraceSub(topic, channel, state, traceID, msg, clientID)
	}
}

func init() {
	nsqMsgTracer = &LogMsgTracer{}
}
