package consistence

import (
	"os"
	"sync"
	"sync/atomic"
)

type ChannelConsumerOffset struct {
	VOffset int64
	Flush   bool
}

type ChannelConsumeMgr struct {
	sync.Mutex
	channelConsumeOffset map[string]ChannelConsumerOffset
}

func newChannelComsumeMgr() *ChannelConsumeMgr {
	return &ChannelConsumeMgr{
		channelConsumeOffset: make(map[string]ChannelConsumerOffset),
	}
}

type coordData struct {
	topicInfo          TopicPartitionMetaInfo
	topicLeaderSession TopicLeaderSession
	consumeMgr         *ChannelConsumeMgr
	logMgr             *TopicCommitLogMgr
	forceLeave         int32
}

func (self *coordData) GetCopy() *coordData {
	newCoordData := &coordData{}
	if self == nil {
		return newCoordData
	}
	*newCoordData = *self
	return newCoordData
}

type TopicCoordinator struct {
	dataMutex sync.Mutex
	*coordData
	// hold for write to avoid disable or exiting or catchup
	// lock order: first lock writehold then lock data to avoid deadlock
	writeHold      sync.Mutex
	catchupRunning int32
	disableWrite   int32
	exiting        int32
}

func NewTopicCoordinator(name string, partition int, basepath string, syncEvery int) (*TopicCoordinator, error) {
	tc := &TopicCoordinator{}
	tc.coordData = &coordData{}
	tc.coordData.consumeMgr = newChannelComsumeMgr()
	tc.topicInfo.Name = name
	tc.topicInfo.Partition = partition
	tc.disableWrite = 1
	var err error
	err = os.MkdirAll(basepath, 0755)
	if err != nil {
		coordLog.Errorf("topic(%v) failed to create directory: %v ", name, err)
		return nil, err
	}
	buf := syncEvery - 1
	if buf < 0 {
		buf = DEFAULT_COMMIT_BUF_SIZE
	}
	tc.logMgr, err = InitTopicCommitLogMgr(name, partition, basepath, buf)
	if err != nil {
		coordLog.Errorf("topic(%v) failed to init log: %v ", name, err)
		return nil, err
	}
	return tc, nil
}

func (self *TopicCoordinator) Delete(removeData bool) {
	self.Exiting()
	self.SetForceLeave(true)
	self.writeHold.Lock()
	self.dataMutex.Lock()
	if removeData {
		self.logMgr.Delete()
	} else {
		self.logMgr.Close()
	}
	self.dataMutex.Unlock()
	self.writeHold.Unlock()
}

func (self *TopicCoordinator) GetData() *coordData {
	self.dataMutex.Lock()
	d := self.coordData
	self.dataMutex.Unlock()
	return d
}

func (self *TopicCoordinator) IsWriteDisabled() bool {
	return atomic.LoadInt32(&self.disableWrite) == 1
}

func (self *TopicCoordinator) DisableWrite(disable bool) {
	// hold the write lock to wait the current write finish.
	self.writeHold.Lock()
	if disable {
		atomic.StoreInt32(&self.disableWrite, 1)
	} else {
		atomic.StoreInt32(&self.disableWrite, 0)
	}
	self.writeHold.Unlock()
}

func (self *TopicCoordinator) IsExiting() bool {
	return atomic.LoadInt32(&self.exiting) == 1
}

func (self *TopicCoordinator) Exiting() {
	atomic.StoreInt32(&self.exiting, 1)
}

func (self *coordData) GetLeader() string {
	return self.topicInfo.Leader
}

func (self *coordData) GetLeaderSessionID() string {
	if self.topicLeaderSession.LeaderNode == nil {
		return ""
	}
	return self.topicLeaderSession.LeaderNode.GetID()
}

func (self *coordData) IsMineISR(id string) bool {
	return FindSlice(self.topicInfo.ISR, id) != -1
}

func (self *coordData) IsMineLeaderSessionReady(id string) bool {
	if self.topicLeaderSession.LeaderNode != nil &&
		self.topicLeaderSession.LeaderNode.GetID() == id &&
		self.topicLeaderSession.Session != "" {
		return true
	}
	return false
}

func (self *coordData) GetLeaderSession() string {
	return self.topicLeaderSession.Session
}

func (self *coordData) GetLeaderSessionEpoch() EpochType {
	return self.topicLeaderSession.LeaderEpoch
}

func (self *coordData) GetTopicEpochForWrite() EpochType {
	return self.topicInfo.EpochForWrite
}

func (self *TopicCoordinator) checkWriteForLeader(myID string) *CoordErr {
	return self.GetData().checkWriteForLeader(myID)
}

func (self *coordData) checkWriteForLeader(myID string) *CoordErr {
	if self.IsForceLeave() {
		return ErrNotTopicLeader
	}
	if self.GetLeaderSessionID() != myID || self.topicInfo.Leader != myID {
		return ErrNotTopicLeader
	}
	if self.topicLeaderSession.Session == "" {
		return ErrMissingTopicLeaderSession
	}
	return nil
}

func (self *coordData) IsISRReadyForWrite() bool {
	return len(self.topicInfo.ISR) > self.topicInfo.Replica/2
}

func (self *coordData) SetForceLeave(leave bool) {
	if leave {
		atomic.StoreInt32(&self.forceLeave, 1)
	} else {
		atomic.StoreInt32(&self.forceLeave, 0)
	}
}

func (self *coordData) IsForceLeave() bool {
	return atomic.LoadInt32(&self.forceLeave) == 1
}
