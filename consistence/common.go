package consistence

import (
	"bytes"
	"strings"
)

const (
	ErrFailedOnNotLeader   = "E_FAILED_ON_NOT_LEADER"
	ErrFailedOnNotWritable = "E_FAILED_ON_NOT_WRITABLE"
)

type CoordErrType int

const (
	CoordNoErr CoordErrType = iota
	CoordCommonErr
	CoordNetErr
	CoordElectionErr
	CoordElectionTmpErr
	CoordClusterErr
	CoordSlaveErr
	CoordLocalErr
	CoordLocalTmpErr
	CoordTmpErr
)

type CoordErr struct {
	ErrMsg  string
	ErrCode ErrRPCRetCode
	ErrType CoordErrType
}

func NewCoordErr(msg string, etype CoordErrType) *CoordErr {
	return &CoordErr{
		ErrMsg:  msg,
		ErrType: etype,
		ErrCode: RpcCommonErr,
	}
}

func NewCoordErrWithCode(msg string, etype CoordErrType, code ErrRPCRetCode) *CoordErr {
	return &CoordErr{
		ErrMsg:  msg,
		ErrType: etype,
		ErrCode: code,
	}
}

func (self *CoordErr) Error() string {
	return self.ErrMsg
}

func (self *CoordErr) HasError() bool {
	if self.ErrType == CoordNoErr && self.ErrCode == RpcNoErr {
		return false
	}
	return true
}

func (self *CoordErr) IsEqual(other *CoordErr) bool {
	if other == nil || self == nil {
		return false
	}

	if self == other {
		return true
	}

	if other.ErrCode != self.ErrCode || other.ErrType != self.ErrType {
		return false
	}

	if other.ErrCode != RpcCommonErr {
		return true
	}
	// only common error need to check if errmsg is equal
	if other.ErrMsg == self.ErrMsg {
		return true
	}
	return false
}

func (self *CoordErr) IsNetErr() bool {
	return self.ErrType == CoordNetErr
}

func (self *CoordErr) IsLocalErr() bool {
	return self.ErrType == CoordLocalErr
}

func (self *CoordErr) CanRetryWrite() bool {
	return self.ErrType == CoordTmpErr ||
		self.ErrType == CoordElectionTmpErr ||
		self.ErrType == CoordLocalTmpErr ||
		self.ErrType == CoordSlaveErr
}

var (
	ErrTopicInfoNotFound = NewCoordErr("topic info not found", CoordClusterErr)

	ErrNotTopicLeader                = NewCoordErrWithCode("not topic leader", CoordElectionErr, RpcErrNotTopicLeader)
	ErrEpochMismatch                 = NewCoordErrWithCode("commit epoch not match", CoordElectionErr, RpcErrEpochMismatch)
	ErrEpochLessThanCurrent          = NewCoordErrWithCode("epoch should be increased", CoordElectionErr, RpcErrEpochLessThanCurrent)
	ErrWriteQuorumFailed             = NewCoordErrWithCode("write to quorum failed.", CoordElectionTmpErr, RpcErrWriteQuorumFailed)
	ErrCommitLogIDDup                = NewCoordErrWithCode("commit id duplicated", CoordElectionErr, RpcErrCommitLogIDDup)
	ErrMissingTopicLeaderSession     = NewCoordErrWithCode("missing topic leader session", CoordElectionErr, RpcErrMissingTopicLeaderSession)
	ErrLeaderSessionMismatch         = NewCoordErrWithCode("leader session mismatch", CoordElectionErr, RpcErrLeaderSessionMismatch)
	ErrWriteDisabled                 = NewCoordErrWithCode("write is disabled on the topic", CoordElectionErr, RpcErrWriteDisabled)
	ErrLeavingISRWait                = NewCoordErrWithCode("leaving isr need wait.", CoordElectionTmpErr, RpcErrLeavingISRWait)
	ErrTopicCoordExistingAndMismatch = NewCoordErrWithCode("topic coordinator existing with a different partition", CoordClusterErr, RpcErrTopicCoordExistingAndMismatch)
	ErrTopicLeaderChanged            = NewCoordErrWithCode("topic leader changed", CoordElectionTmpErr, RpcErrTopicLeaderChanged)
	ErrTopicCommitLogEOF             = NewCoordErrWithCode("topic commit log end of file", CoordCommonErr, RpcErrCommitLogEOF)
	ErrTopicCommitLogOutofBound      = NewCoordErrWithCode("topic commit log offset out of bound", CoordCommonErr, RpcErrCommitLogOutofBound)
	ErrTopicCommitLogNotConsistent   = NewCoordErrWithCode("topic commit log is not consistent", CoordClusterErr, RpcCommonErr)
	ErrMissingTopicCoord             = NewCoordErrWithCode("missing topic coordinator", CoordClusterErr, RpcErrMissingTopicCoord)
	ErrTopicLoading                  = NewCoordErrWithCode("topic is still loading data", CoordLocalTmpErr, RpcErrTopicLoading)
	ErrTopicExiting                  = NewCoordErr("topic coordinator is exiting", CoordLocalTmpErr)
	ErrTopicExitingOnSlave           = NewCoordErr("topic coordinator is exiting on slave", CoordTmpErr)
	ErrTopicCoordStateInvalid        = NewCoordErrWithCode("invalid coordinator state", CoordClusterErr, RpcErrTopicCoordStateInvalid)
	ErrTopicSlaveInvalid             = NewCoordErrWithCode("topic slave has some invalid state", CoordSlaveErr, RpcErrSlaveStateInvalid)
	ErrTopicLeaderSessionInvalid     = NewCoordErrWithCode("topic leader session is invalid", CoordElectionTmpErr, RpcCommonErr)
	ErrTopicWriteOnNonISR            = NewCoordErrWithCode("topic write on a node not in ISR", CoordTmpErr, RpcErrWriteOnNonISR)
	ErrTopicISRNotEnough             = NewCoordErrWithCode("topic isr nodes not enough", CoordTmpErr, RpcCommonErr)

	ErrPubArgError                = NewCoordErr("pub argument error", CoordCommonErr)
	ErrTopicNotRelated            = NewCoordErr("topic not related to me", CoordCommonErr)
	ErrTopicCatchupAlreadyRunning = NewCoordErr("topic is already running catchup", CoordCommonErr)
	ErrTopicArgError              = NewCoordErr("topic argument error", CoordCommonErr)
	ErrOperationExpired           = NewCoordErr("operation has expired since wait too long", CoordCommonErr)

	ErrMissingTopicLog             = NewCoordErr("missing topic log ", CoordLocalErr)
	ErrLocalTopicPartitionMismatch = NewCoordErr("local topic partition not match", CoordLocalErr)
	ErrLocalFallBehind             = NewCoordErr("local data fall behind", CoordElectionErr)
	ErrLocalForwardThanLeader      = NewCoordErr("local data is more than leader", CoordElectionErr)
	ErrLocalWriteFailed            = NewCoordErr("write data to local failed", CoordLocalErr)
	ErrLocalMissingTopic           = NewCoordErr("local topic missing", CoordLocalErr)
	ErrLocalNotReadyForWrite       = NewCoordErr("local topic is not ready for write.", CoordLocalErr)
	ErrLocalInitTopicFailed        = NewCoordErr("local topic init failed", CoordLocalErr)
	ErrLocalInitTopicCoordFailed   = NewCoordErr("topic coordinator init failed", CoordLocalErr)
	ErrLocalTopicDataCorrupt       = NewCoordErr("local topic data corrupt", CoordLocalErr)
)

func GenNsqdNodeID(n *NsqdNodeInfo, extra string) string {
	var tmpbuf bytes.Buffer
	tmpbuf.WriteString(n.NodeIP)
	tmpbuf.WriteString(":")
	tmpbuf.WriteString(n.RpcPort)
	tmpbuf.WriteString(":")
	tmpbuf.WriteString(n.TcpPort)
	tmpbuf.WriteString(":")
	tmpbuf.WriteString(extra)
	return tmpbuf.String()
}

func GenNsqLookupNodeID(n *NsqLookupdNodeInfo, extra string) string {
	var tmpbuf bytes.Buffer
	tmpbuf.WriteString(n.NodeIP)
	tmpbuf.WriteString(":")
	tmpbuf.WriteString(n.RpcPort)
	tmpbuf.WriteString(":")
	tmpbuf.WriteString(n.TcpPort)
	tmpbuf.WriteString(":")
	tmpbuf.WriteString(extra)
	return tmpbuf.String()
}

func ExtractRpcAddrFromID(nid string) string {
	pos1 := strings.Index(nid, ":")
	pos2 := strings.Index(nid[pos1+1:], ":")
	return nid[:pos1+pos2+1]
}

func FindSlice(in []string, e string) int {
	for i, v := range in {
		if v == e {
			return i
		}
	}
	return -1
}

func MergeList(l []string, r []string) []string {
	tmp := make(map[string]struct{})
	for _, v := range l {
		tmp[v] = struct{}{}
	}
	for _, v := range r {
		tmp[v] = struct{}{}
	}
	ret := make([]string, 0, len(tmp))
	for k, _ := range tmp {
		ret = append(ret, k)
	}
	return ret
}

func FilterList(l []string, filter []string) []string {
	tmp := make(map[string]struct{})
	for _, v := range l {
		tmp[v] = struct{}{}
	}
	for _, v := range filter {
		delete(tmp, v)
	}
	ret := make([]string, 0, len(tmp))
	for k, _ := range tmp {
		ret = append(ret, k)
	}
	return ret
}
