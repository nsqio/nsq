package consistence

import (
	"errors"
	"strconv"
)

var (
	ErrLeaderSessionAlreadyExist = errors.New("leader session already exist")
	ErrLeaderSessionNotExist     = errors.New("session not exist")
	ErrKeyAlreadyExist           = errors.New("Key already exist")
	ErrKeyNotFound               = errors.New("Key not found")
)

type EpochType int64

type NsqdNodeInfo struct {
	ID       string
	NodeIP   string
	TcpPort  string
	RpcPort  string
	HttpPort string
}

func (self *NsqdNodeInfo) GetID() string {
	return self.ID
}

type NsqLookupdNodeInfo struct {
	ID       string
	NodeIP   string
	TcpPort  string
	HttpPort string
	RpcPort  string
	Epoch    EpochType
}

func (self *NsqLookupdNodeInfo) GetID() string {
	return self.ID
}

// topicLeaderEpoch increase while leader changed.
// topicEpoch increase while leader or isr changed. (seems not really needed?)
// lookupEpoch increase while lookup leader changed.

type TopicMetaInfo struct {
	PartitionNum int
	Replica      int
	// the suggest load factor for each topic partition.
	SuggestLF int
	// other options
	SyncEvery int
	// to verify the data of the create -> delete -> create with same topic
	MagicCode int64
	// the retention days for the data
	RetentionDay int32
	// used for ordered multi partition topic
	// allow multi partitions on the same node
	OrderedMulti bool
	//used for message ext
	Ext bool
}

type TopicPartitionReplicaInfo struct {
	Leader      string
	ISR         []string
	CatchupList []string
	Channels    []string
	// this is only used for write operation
	// if this changed during write, mean the current write should be abort
	EpochForWrite EpochType
	Epoch         EpochType
}

type TopicPartitionMetaInfo struct {
	Name      string
	Partition int
	TopicMetaInfo
	TopicPartitionReplicaInfo
}

func (self *TopicPartitionMetaInfo) GetTopicDesp() string {
	return self.Name + "-" + strconv.Itoa(self.Partition)
}

type TopicLeaderSession struct {
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
	Unregister(value *NsqLookupdNodeInfo) error
	Stop()
	// the cluster root modify index
	GetClusterEpoch() (EpochType, error)
	GetAllLookupdNodes() ([]NsqLookupdNodeInfo, error) // add
	AcquireAndWatchLeader(leader chan *NsqLookupdNodeInfo, stop chan struct{})
	CheckIfLeader(session string) bool
	UpdateLookupEpoch(oldGen EpochType) (EpochType, error)

	GetNsqdNodes() ([]NsqdNodeInfo, error)
	// watching the cluster nsqd node, should return the newest for the first time.
	WatchNsqdNodes(nsqds chan []NsqdNodeInfo, stop chan struct{})
	// get all topics info, should cache the newest to improve performance.
	ScanTopics() ([]TopicPartitionMetaInfo, error)
	// should return both the meta info for topic and the replica info for topic partition
	// epoch should be updated while return
	GetTopicInfo(topic string, partition int) (*TopicPartitionMetaInfo, error)
	// create and write the meta info to topic meta node
	CreateTopic(topic string, meta *TopicMetaInfo) error
	// create topic partition path
	CreateTopicPartition(topic string, partition int) error
	IsExistTopic(topic string) (bool, error)
	IsExistTopicPartition(topic string, partition int) (bool, error)
	// get topic meta info only
	GetTopicMetaInfo(topic string) (TopicMetaInfo, EpochType, error)
	UpdateTopicMetaInfo(topic string, meta *TopicMetaInfo, oldGen EpochType) error
	DeleteTopic(topic string, partition int) error
	DeleteWholeTopic(topic string) error
	//
	// update the replica info about leader, isr, epoch for partition
	// Note: update should do check-and-set to avoid unexpected override.
	// the epoch in topicInfo should be updated to the new epoch
	// if no topic partition replica info node should create only once.
	UpdateTopicNodeInfo(topic string, partition int, topicInfo *TopicPartitionReplicaInfo, oldGen EpochType) error
	// get leadership information, if not exist should return ErrLeaderSessionNotExist as error
	GetTopicLeaderSession(topic string, partition int) (*TopicLeaderSession, error)
	// watch any leadership lock change for all topic partitions, should return the token used later by release.
	WatchTopicLeader(leader chan *TopicLeaderSession, stop chan struct{}) error
	// only leader lookup can do the release, normally notify the nsqd node do the release by itself.
	// lookup node should release only when the nsqd is lost
	ReleaseTopicLeader(topic string, partition int, session *TopicLeaderSession) error
}

type NSQDLeadership interface {
	InitClusterID(id string)
	RegisterNsqd(nodeData *NsqdNodeInfo) error // update
	UnregisterNsqd(nodeData *NsqdNodeInfo) error
	// try create the topic leadership key and no need to retry if the key already exist
	AcquireTopicLeader(topic string, partition int, nodeData *NsqdNodeInfo, epoch EpochType) error
	// release the session key using the acquired session. should check current session epoch
	// to avoid release the changed session
	ReleaseTopicLeader(topic string, partition int, session *TopicLeaderSession) error
	// all registered lookup nodes.
	GetAllLookupdNodes() ([]NsqLookupdNodeInfo, error)
	// get the newest lookup leader and watch the change of it.
	WatchLookupdLeader(leader chan *NsqLookupdNodeInfo, stop chan struct{}) error
	GetTopicInfo(topic string, partition int) (*TopicPartitionMetaInfo, error)
	// get leadership information, if not exist should return ErrLeaderSessionNotExist as error
	GetTopicLeaderSession(topic string, partition int) (*TopicLeaderSession, error)
}
