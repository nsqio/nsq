package consistence

import (
	"bytes"
	"strconv"
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
	CoordClusterNoRetryWriteErr
)

// note: since the gorpc will treat error type as special,
// we should not implement the error interface for CoordErr as response type
type CoordErr struct {
	ErrMsg  string
	ErrCode ErrRPCRetCode
	ErrType CoordErrType
}

type CommonCoordErr struct {
	CoordErr
}

func (self *CommonCoordErr) Error() string {
	return self.String()
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

func (self *CoordErr) ToErrorType() error {
	e := &CommonCoordErr{}
	e.ErrMsg = self.ErrMsg
	e.ErrType = self.ErrType
	e.ErrCode = self.ErrCode
	return e
}

func (self *CoordErr) String() string {
	var tmpbuf bytes.Buffer
	tmpbuf.WriteString("ErrType:")
	tmpbuf.WriteString(strconv.Itoa(int(self.ErrType)))
	tmpbuf.WriteString(" ErrCode:")
	tmpbuf.WriteString(strconv.Itoa(int(self.ErrCode)))
	tmpbuf.WriteString(" : ")
	tmpbuf.WriteString(self.ErrMsg)
	return tmpbuf.String()
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

func (self *CoordErr) CanRetryWrite(retryTimes int) bool {
	if self.ErrType == CoordClusterNoRetryWriteErr {
		return false
	}
	if self.ErrType == CoordElectionErr {
		if retryTimes > MAX_WRITE_RETRY {
			return false
		}
		return true
	}
	if self.ErrType == CoordTmpErr ||
		self.ErrType == CoordElectionTmpErr ||
		self.ErrType == CoordLocalTmpErr {
		if retryTimes > MAX_WRITE_RETRY*3 {
			return false
		}
		return true
	}
	return self.ErrType == CoordSlaveErr ||
		self.IsNetErr()
}

var (
	ErrTopicInfoNotFound = NewCoordErr("topic info not found", CoordClusterErr)

	ErrNotTopicLeader                     = NewCoordErrWithCode("not topic leader", CoordClusterNoRetryWriteErr, RpcErrNotTopicLeader)
	ErrEpochMismatch                      = NewCoordErrWithCode("commit epoch not match", CoordElectionTmpErr, RpcErrEpochMismatch)
	ErrEpochLessThanCurrent               = NewCoordErrWithCode("epoch should be increased", CoordElectionErr, RpcErrEpochLessThanCurrent)
	ErrWriteQuorumFailed                  = NewCoordErrWithCode("write to quorum failed.", CoordElectionTmpErr, RpcErrWriteQuorumFailed)
	ErrCommitLogIDDup                     = NewCoordErrWithCode("commit id duplicated", CoordElectionErr, RpcErrCommitLogIDDup)
	ErrMissingTopicLeaderSession          = NewCoordErrWithCode("missing topic leader session", CoordElectionErr, RpcErrMissingTopicLeaderSession)
	ErrLeaderSessionMismatch              = NewCoordErrWithCode("leader session mismatch", CoordElectionTmpErr, RpcErrLeaderSessionMismatch)
	ErrWriteDisabled                      = NewCoordErrWithCode("write is disabled on the topic", CoordElectionErr, RpcErrWriteDisabled)
	ErrLeavingISRWait                     = NewCoordErrWithCode("leaving isr need wait.", CoordElectionTmpErr, RpcErrLeavingISRWait)
	ErrTopicCoordExistingAndMismatch      = NewCoordErrWithCode("topic coordinator existing with a different partition", CoordClusterErr, RpcErrTopicCoordExistingAndMismatch)
	ErrTopicCoordTmpConflicted            = NewCoordErrWithCode("topic coordinator is conflicted temporally", CoordClusterErr, RpcErrTopicCoordConflicted)
	ErrTopicLeaderChanged                 = NewCoordErrWithCode("topic leader changed", CoordElectionTmpErr, RpcErrTopicLeaderChanged)
	ErrTopicCommitLogEOF                  = NewCoordErrWithCode(ErrCommitLogEOF.Error(), CoordCommonErr, RpcErrCommitLogEOF)
	ErrTopicCommitLogOutofBound           = NewCoordErrWithCode(ErrCommitLogOutofBound.Error(), CoordCommonErr, RpcErrCommitLogOutofBound)
	ErrTopicCommitLogLessThanSegmentStart = NewCoordErrWithCode(ErrCommitLogLessThanSegmentStart.Error(), CoordCommonErr, RpcErrCommitLogLessThanSegmentStart)
	ErrTopicCommitLogNotConsistent        = NewCoordErrWithCode("topic commit log is not consistent", CoordClusterErr, RpcCommonErr)
	ErrMissingTopicCoord                  = NewCoordErrWithCode("missing topic coordinator", CoordClusterErr, RpcErrMissingTopicCoord)
	ErrTopicLoading                       = NewCoordErrWithCode("topic is still loading data", CoordLocalTmpErr, RpcErrTopicLoading)
	ErrTopicExiting                       = NewCoordErr("topic coordinator is exiting", CoordLocalTmpErr)
	ErrTopicExitingOnSlave                = NewCoordErr("topic coordinator is exiting on slave", CoordTmpErr)
	ErrTopicCoordStateInvalid             = NewCoordErrWithCode("invalid coordinator state", CoordClusterErr, RpcErrTopicCoordStateInvalid)
	ErrTopicSlaveInvalid                  = NewCoordErrWithCode("topic slave has some invalid state", CoordSlaveErr, RpcErrSlaveStateInvalid)
	ErrTopicLeaderSessionInvalid          = NewCoordErrWithCode("topic leader session is invalid", CoordElectionTmpErr, RpcCommonErr)
	ErrTopicWriteOnNonISR                 = NewCoordErrWithCode("topic write on a node not in ISR", CoordTmpErr, RpcErrWriteOnNonISR)
	ErrTopicISRNotEnough                  = NewCoordErrWithCode("topic isr nodes not enough", CoordTmpErr, RpcCommonErr)
	ErrClusterChanged                     = NewCoordErrWithCode("cluster changed ", CoordTmpErr, RpcNoErr)

	ErrPubArgError                = NewCoordErr("pub argument error", CoordCommonErr)
	ErrTopicNotRelated            = NewCoordErr("topic not related to me", CoordCommonErr)
	ErrTopicCatchupAlreadyRunning = NewCoordErr("topic is already running catchup", CoordCommonErr)
	ErrTopicArgError              = NewCoordErr("topic argument error", CoordCommonErr)
	ErrOperationExpired           = NewCoordErr("operation has expired since wait too long", CoordCommonErr)
	ErrCatchupRunningBusy         = NewCoordErr("too much running catchup", CoordCommonErr)

	ErrMissingTopicLog                     = NewCoordErr("missing topic log ", CoordLocalErr)
	ErrLocalTopicPartitionMismatch         = NewCoordErr("local topic partition not match", CoordLocalErr)
	ErrLocalFallBehind                     = NewCoordErr("local data fall behind", CoordElectionErr)
	ErrLocalForwardThanLeader              = NewCoordErr("local data is more than leader", CoordElectionErr)
	ErrLocalSetChannelOffsetNotFirstClient = NewCoordErr("failed to set channel offset since not first client", CoordLocalErr)
	ErrLocalMissingTopic                   = NewCoordErr("local topic missing", CoordLocalErr)
	ErrLocalNotReadyForWrite               = NewCoordErr("local topic is not ready for write.", CoordLocalErr)
	ErrLocalInitTopicFailed                = NewCoordErr("local topic init failed", CoordLocalErr)
	ErrLocalInitTopicCoordFailed           = NewCoordErr("topic coordinator init failed", CoordLocalErr)
	ErrLocalTopicDataCorrupt               = NewCoordErr("local topic data corrupt", CoordLocalErr)
	ErrLocalChannelPauseFailed               = NewCoordErr("local channel pause/unpause failed", CoordLocalErr)
	ErrLocalChannelSkipFailed               = NewCoordErr("local channel skip/unskip failed", CoordLocalErr)
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
	for _, e := range r {
		if FindSlice(l, e) == -1 {
			l = append(l, e)
		}
	}
	return l
}

func FilterList(l []string, filter []string) []string {
	for _, e := range filter {
		for i, v := range l {
			if e == v {
				copy(l[i:], l[i+1:])
				l = l[:len(l)-1]
				break
			}
		}
	}
	return l
}
