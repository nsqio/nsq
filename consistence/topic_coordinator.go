package consistence

import (
	"os"
	"sync"
)

type ChannelConsumerOffset struct {
	OffsetID int64
	VOffset  int64
}

type coordData struct {
	topicInfo            TopicPartionMetaInfo
	topicLeaderSession   TopicLeaderSession
	channelConsumeOffset map[string]ChannelConsumerOffset
	localDataLoaded      bool
	logMgr               *TopicCommitLogMgr
}

type TopicCoordinator struct {
	dataRWMutex sync.RWMutex
	coordData
	// hold for write to avoid disable or exiting or catchup
	// lock order: first lock writehold then lock data to avoid deadlock
	writeHold      sync.Mutex
	catchupRunning int32
	exiting        bool
	disableWrite   bool
}

func NewTopicCoordinator(name string, partition int, basepath string) (*TopicCoordinator, error) {
	tc := &TopicCoordinator{}
	tc.channelConsumeOffset = make(map[string]ChannelConsumerOffset)
	tc.topicInfo.Name = name
	tc.topicInfo.Partition = partition
	var err error
	err = os.MkdirAll(basepath, 0700)
	if err != nil {
		coordLog.Errorf("topic(%v) failed to create directory: %v ", name, err)
		return nil, err
	}
	tc.logMgr, err = InitTopicCommitLogMgr(name, partition, basepath, DEFAULT_COMMIT_BUF_SIZE)
	if err != nil {
		coordLog.Errorf("topic(%v) failed to init log: %v ", name, err)
		return nil, err
	}
	return tc, nil
}

func (self *TopicCoordinator) GetData() *coordData {
	self.dataRWMutex.RLock()
	d := self.coordData
	self.dataRWMutex.RUnlock()
	return &d
}

func (self *TopicCoordinator) DisableWrite(disable bool) {
	self.writeHold.Lock()
	self.disableWrite = disable
	self.writeHold.Unlock()
}

func (self *TopicCoordinator) Exiting() {
	self.writeHold.Lock()
	self.exiting = true
	self.writeHold.Unlock()
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

func (self *TopicCoordinator) GetLeader() string {
	return self.coordData.GetLeader()
}

func (self *TopicCoordinator) GetLeaderSessionID() string {
	return self.coordData.GetLeaderSessionID()
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

func (self *coordData) GetLeaderEpoch() int32 {
	return int32(self.topicLeaderSession.LeaderEpoch)
}

func (self *coordData) GetTopicInfoEpoch() int32 {
	return int32(self.topicInfo.Epoch)
}

func (self *coordData) checkWriteForLeader(myID string) *CoordErr {
	if self.GetLeaderSessionID() != myID || self.topicInfo.Leader != myID {
		return ErrNotTopicLeader
	}
	if self.topicLeaderSession.Session == "" {
		return ErrMissingTopicLeaderSession
	}
	return nil
}

func (self *TopicCoordinator) checkWriteForLeader(myID string) *CoordErr {
	if self.disableWrite {
		return ErrWriteDisabled
	}
	return self.coordData.checkWriteForLeader(myID)
}

func (self *coordData) IsISRReadyForWrite() bool {
	return len(self.topicInfo.ISR) > self.topicInfo.Replica/2
}

func (self *TopicCoordinator) IsISRReadyForWrite() bool {
	return self.coordData.IsISRReadyForWrite()
}
