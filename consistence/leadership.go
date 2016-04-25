package consistence

import (
	"errors"
	"strconv"
)

var (
	ErrLeaderSessionAlreadyExist = errors.New("leader session already exist")
)

type EpochType int64

type NsqdNodeInfo struct {
	ID      string
	NodeIp  string
	TcpPort string
	RpcPort string
}

func (self *NsqdNodeInfo) GetID() string {
	return self.ID
}

type NsqLookupdNodeInfo struct {
	ID      string
	NodeIp  string
	TcpPort string
	RpcPort string
	Epoch   EpochType
}

func (self *NsqLookupdNodeInfo) GetID() string {
	return self.ID
}

// topicLeaderEpoch increase while leader changed.
// topicEpoch increase while leader or isr changed. (seems not really needed?)
// lookupEpoch increase while lookup leader changed.

type TopicPartionMetaInfo struct {
	Name        string
	Partition   int
	Channels    []string
	Leader      string
	ISR         []string
	CatchupList []string
	Epoch       EpochType
	Replica     int
	// the suggest load factor for each topic partition.
	SuggestLF int
}

func (self *TopicPartionMetaInfo) GetTopicDesp() string {
	return self.Name + "-" + strconv.Itoa(self.Partition)
}

type TopicLeaderSession struct {
	ClusterID   string
	Topic       string
	Partition   int
	LeaderNode  *NsqdNodeInfo
	Session     string
	LeaderEpoch EpochType
}

func (self *TopicLeaderSession) IsSame(other *TopicLeaderSession) bool {
	if other == nil || self == nil {
		return false
	}
	if self.LeaderEpoch != other.LeaderEpoch {
		return false
	}
	if self.Session != "" && other.Session != "" && self.Session != other.Session {
		return false
	}
	if self.LeaderNode == nil || other.LeaderNode == nil {
		return false
	}
	if self.LeaderNode.GetID() != other.LeaderNode.GetID() {
		return false
	}
	return true
}

type ConsistentStore interface {
	WriteKey(key, value string) error
	ReadKey(key string) (string, error)
	ListKey(key string) ([]string, error)
}

// We need check leader lock session before do any modify to etcd.
// Make sure all returned value should be copied to avoid modify by outside.
type NSQLookupdLeadership interface {
	InitClusterID(id string)
	Register(value *NsqLookupdNodeInfo) error
	Unregister() error
	Stop()
	GetClusterEpoch() (EpochType, error)
	GetAllLookupdNodes() ([]NsqLookupdNodeInfo, error) // add
	AcquireAndWatchLeader(leader chan *NsqLookupdNodeInfo, stop chan struct{})
	CheckIfLeader(session string) bool
	UpdateLookupEpoch(oldGen EpochType) (EpochType, error)
	// cluster nsqd node
	WatchNsqdNodes(nsqds chan []NsqdNodeInfo, stop chan struct{})
	ScanTopics() ([]TopicPartionMetaInfo, error)
	GetTopicInfo(topic string, partition int) (*TopicPartionMetaInfo, error)
	CreateTopicPartition(topic string, partition int) error
	CreateTopic(topic string, partitionNum int, replica int) error
	IsExistTopic(topic string) (bool, error)
	IsExistTopicPartition(topic string, partitionNum int) (bool, error)
	GetTopicPartitionNum(topic string) (int, error)
	DeleteTopic(topic string, partition int) error
	// update leader, isr, epoch
	// Note: update should do check-and-set to avoid unexpected override.
	CreateTopicNodeInfo(topic string, partition int, topicInfo *TopicPartionMetaInfo) (error, EpochType)
	// the epoch in topicInfo should be updated to the new epoch
	UpdateTopicNodeInfo(topic string, partition int, topicInfo *TopicPartionMetaInfo, oldGen EpochType) error
	// get leadership information
	GetTopicLeaderSession(topic string, partition int) (*TopicLeaderSession, error)
	// watch any leadership lock change for all topic partitions, should return the token used later by release.
	WatchTopicLeader(leader chan *TopicLeaderSession, stop chan struct{}) error
}

type NSQDLeadership interface {
	InitClusterID(id string)
	RegisterNsqd(nodeData *NsqdNodeInfo) error // update
	UnregisterNsqd(nodeData *NsqdNodeInfo) error
	// get the topic leadership lock.
	AcquireTopicLeader(topic string, partition int, nodeData *NsqdNodeInfo, epoch EpochType, topicLeader chan *TopicLeaderSession) error
	// release the lock using the acquired session.
	ReleaseTopicLeader(topic string, partition int, session *TopicLeaderSession) error
	// all registered lookup nodes.
	GetAllLookupdNodes() ([]NsqLookupdNodeInfo, error)
	WatchLookupdLeader(leader chan *NsqLookupdNodeInfo, stop chan struct{}) error
	GetTopicInfo(topic string, partition int) (*TopicPartionMetaInfo, error)
	GetTopicLeaderSession(topic string, partition int) (*TopicLeaderSession, error)
}
