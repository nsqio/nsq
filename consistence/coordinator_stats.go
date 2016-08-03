package consistence

import (
	"sync"
	"sync/atomic"
)

type CoordErrStats struct {
	sync.Mutex
	WriteEpochError        int64
	WriteNotLeaderError    int64
	WriteQuorumError       int64
	WriteBusyError         int64
	RpcCheckFailed         int64
	LeadershipError        int64
	TopicCoordMissingError int64
	LocalErr               int64
	OtherCoordErrs         map[string]int64
}

func newCoordErrStats() *CoordErrStats {
	return &CoordErrStats{
		OtherCoordErrs: make(map[string]int64, 100),
	}
}

func (self *CoordErrStats) GetCopy() *CoordErrStats {
	var ret CoordErrStats
	self.Lock()
	ret = *coordErrStats
	ret.OtherCoordErrs = make(map[string]int64, len(self.OtherCoordErrs))
	for k, v := range self.OtherCoordErrs {
		ret.OtherCoordErrs[k] = v
	}
	self.Unlock()
	return &ret
}

func (self *CoordErrStats) incLocalErr() {
	atomic.AddInt64(&self.LocalErr, 1)
}

func (self *CoordErrStats) incRpcCheckFailed() {
	atomic.AddInt64(&self.RpcCheckFailed, 1)
}

func (self *CoordErrStats) incLeadershipErr() {
	atomic.AddInt64(&self.LeadershipError, 1)
}

func (self *CoordErrStats) incTopicCoordMissingErr() {
	atomic.AddInt64(&self.TopicCoordMissingError, 1)
}

func (self *CoordErrStats) incWriteErr(e *CoordErr) {
	if e == ErrEpochMismatch || e == ErrEpochLessThanCurrent {
		atomic.AddInt64(&self.WriteEpochError, 1)
	} else if e == ErrNotTopicLeader || e == ErrMissingTopicLeaderSession {
		atomic.AddInt64(&self.WriteNotLeaderError, 1)
	} else if e == ErrWriteQuorumFailed {
		atomic.AddInt64(&self.WriteQuorumError, 1)
	} else if e == ErrWriteDisabled {
		atomic.AddInt64(&self.WriteBusyError, 1)
	} else if e.ErrType == CoordLocalErr {
		atomic.AddInt64(&self.LocalErr, 1)
	} else {
		self.incCoordErr(e)
	}
}

func (self *CoordErrStats) incCoordErr(e *CoordErr) {
	if e == nil || !e.HasError() {
		return
	}
	self.Lock()
	cnt := self.OtherCoordErrs[e.ErrMsg]
	cnt++
	self.OtherCoordErrs[e.ErrMsg] = cnt
	self.Unlock()
}

var coordErrStats = newCoordErrStats()
