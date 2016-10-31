package consistence

import (
	"github.com/absolute8511/gorpc"
	"sync"
	"sync/atomic"
)

const maxNumCounters = 1024

var (
	cnames       = make([]string, maxNumCounters)
	counters     = make([]uint64, maxNumCounters)
	curCounterID = new(uint32)
)

func AddCounter(name string) uint32 {
	id := atomic.AddUint32(curCounterID, 1) - 1
	if id >= maxNumCounters {
		panic("Too many counters")
	}
	cnames[id] = name
	return id
}

func IncCounter(id uint32) {
	atomic.AddUint64(&counters[id], 1)
}

func IncCounterBy(id uint32, amount uint64) {
	atomic.AddUint64(&counters[id], amount)
}

func getAllCounters() map[string]uint64 {
	ret := make(map[string]uint64)
	numIDs := int(atomic.LoadUint32(curCounterID))

	for i := 0; i < numIDs; i++ {
		ret[cnames[i]] = atomic.LoadUint64(&counters[i])
	}

	return ret
}

type CoordErrStatsData struct {
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

type CoordErrStats struct {
	sync.Mutex
	CoordErrStatsData
}

func newCoordErrStats() *CoordErrStats {
	s := &CoordErrStats{}
	s.OtherCoordErrs = make(map[string]int64, 100)
	return s
}

func (self *CoordErrStats) GetCopy() *CoordErrStatsData {
	var ret CoordErrStatsData
	self.Lock()
	ret = coordErrStats.CoordErrStatsData
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

type ISRStat struct {
	HostName string `json:"hostname"`
	NodeID   string `json:"node_id"`
}

type CatchupStat struct {
	HostName string `json:"hostname"`
	NodeID   string `json:"node_id"`
	Progress int    `json:"progress"`
}

type TopicCoordStat struct {
	Node         string        `json:"node"`
	Name         string        `json:"name"`
	Partition    int           `json:"partition"`
	ISRStats     []ISRStat     `json:"isr_stats"`
	CatchupStats []CatchupStat `json:"catchup_stats"`
}

type CoordStats struct {
	RpcStats        *gorpc.ConnStats `json:"rpc_stats"`
	ErrStats        CoordErrStatsData
	TopicCoordStats []TopicCoordStat `json:"topic_coord_stats"`
}
