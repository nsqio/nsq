package consistence

import ()

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
	tc.logMgr, err = InitTopicCommitLogMgr(name, partition, basepath, DEFAULT_COMMIT_BUF_SIZE)
	if err != nil {
		coordLog.Errorf("topic(%v) failed to init log: %v ", name, err)
		return nil
	}
	return tc
}

func (self *TopicCoordinator) GetLeaderID() string {
	return self.topicLeaderSession.LeaderNode.GetID()
}

func (self *TopicCoordinator) GetLeaderSession() string {
	return self.topicLeaderSession.Session
}

func (self *TopicCoordinator) GetLeaderEpoch() int32 {
	return self.topicLeaderSession.LeaderEpoch
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
