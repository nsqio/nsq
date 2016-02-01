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

func (self *TopicCoordinator) checkWriteForLeader(myID string) error {
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

func (self *TopicCoordinator) refreshTopicCoord() error {
	return nil
}

func (self *TopicCoordinator) IsISRReadyForWrite() bool {
	return len(self.topicInfo.ISR) > self.topicInfo.Replica/2
}
