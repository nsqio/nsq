package consistence

import (
	"os"
)

type ChannelConsumerOffset struct {
	OffsetID int64
	VOffset  int64
}

type TopicCoordinator struct {
	topicInfo            TopicPartionMetaInfo
	topicLeaderSession   TopicLeaderSession
	disableWrite         bool
	channelConsumeOffset map[string]ChannelConsumerOffset
	localDataLoaded      bool
	logMgr               *TopicCommitLogMgr
}

func NewTopicCoordinator(name string, partition int, basepath string) *TopicCoordinator {
	tc := &TopicCoordinator{
		channelConsumeOffset: make(map[string]ChannelConsumerOffset),
	}
	tc.topicInfo.Name = name
	tc.topicInfo.Partition = partition
	var err error
	os.MkdirAll(basepath, 0700)
	if err != nil {
		coordLog.Errorf("topic(%v) failed to create directory: %v ", name, err)
		return nil
	}
	tc.logMgr, err = InitTopicCommitLogMgr(name, partition, basepath, DEFAULT_COMMIT_BUF_SIZE)
	if err != nil {
		coordLog.Errorf("topic(%v) failed to init log: %v ", name, err)
		return nil
	}
	return tc
}

func (self *TopicCoordinator) GetLeaderID() string {
	if self.topicLeaderSession.LeaderNode == nil {
		return ""
	}
	return self.topicLeaderSession.LeaderNode.GetID()
}

func (self *TopicCoordinator) GetLeaderSession() string {
	return self.topicLeaderSession.Session
}

func (self *TopicCoordinator) GetLeaderEpoch() int32 {
	return self.topicLeaderSession.LeaderEpoch
}

func (self *TopicCoordinator) GetTopicInfoEpoch() int32 {
	return int32(self.topicInfo.Epoch)
}

func (self *TopicCoordinator) checkWriteForLeader(myID string) *CoordErr {
	if self.GetLeaderID() != myID || self.topicInfo.Leader != myID {
		return ErrNotTopicLeader
	}
	if self.disableWrite {
		return ErrWriteDisabled
	}
	if self.GetLeaderSession() == "" {
		return ErrMissingTopicLeaderSession
	}
	if !self.IsISRReadyForWrite() {
		return ErrWriteQuorumFailed
	}
	return nil
}

func (self *TopicCoordinator) refreshTopicCoord() *CoordErr {
	return nil
}

func (self *TopicCoordinator) IsISRReadyForWrite() bool {
	return len(self.topicInfo.ISR) > self.topicInfo.Replica/2
}
