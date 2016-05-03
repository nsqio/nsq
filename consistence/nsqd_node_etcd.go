//        file: consistence/nsqd_node_etcd.go
// description: opr of nsqd node to etcd

//      author: reezhou
//       email: reechou@gmail.com
//   copyright: youzan

package consistence

import (
	"encoding/json"
	"fmt"
	"path"
	"strconv"
	"time"

	"github.com/coreos/go-etcd/etcd"
	etcdlock "github.com/reechou/xlock"
)

const (
	ETCD_TTL = 15
)

type MasterChanInfo struct {
	processStopCh chan bool
	stoppedCh     chan bool
}

type NsqdEtcdMgr struct {
	client      *etcd.Client
	clusterID   string
	topicRoot   string
	lookupdRoot string

	topicLockMap map[string]*etcdlock.SeizeLock
}

func NewNsqdEtcdMgr(host string) *NsqdEtcdMgr {
	client := NewEtcdClient(host)
	return &NsqdEtcdMgr{
		client:       client,
		topicLockMap: make(map[string]*etcdlock.SeizeLock),
	}
}

func (self *NsqdEtcdMgr) InitClusterID(id string) {
	self.clusterID = id
	self.topicRoot = self.createTopicRootPath()
	self.lookupdRoot = self.createLookupdRootPath()
}

func (self *NsqdEtcdMgr) RegisterNsqd(nodeData *NsqdNodeInfo) error {
	value, err := json.Marshal(nodeData)
	if err != nil {
		return err
	}
	_, err = self.client.Create(self.createNsqdNodePath(nodeData), string(value), 0)
	if err != nil {
		return err
	}
	return nil
}

func (self *NsqdEtcdMgr) UnregisterNsqd(nodeData *NsqdNodeInfo) error {
	// clear
	for k, v := range self.topicLockMap {
		v.Unlock()
		delete(self.topicLockMap, k)
	}

	_, err := self.client.Delete(self.createNsqdNodePath(nodeData), false)
	if err != nil {
		return err
	}
	return nil
}

func (self *NsqdEtcdMgr) AcquireTopicLeader(topic string, partition int, nodeData *NsqdNodeInfo, epoch EpochType) error {
	topicLeaderSession := &TopicLeaderSession{
		ClusterID:   self.clusterID,
		Topic:       topic,
		Partition:   partition,
		LeaderNode:  nodeData,
		Session:     hostname + strconv.FormatInt(time.Now().Unix(), 10),
		LeaderEpoch: epoch,
	}
	valueB, err := json.Marshal(topicLeaderSession)
	if err != nil {
		return err
	}
	topicKey := self.createTopicLeaderPath(topic, partition)
	lock := etcdlock.NewSeizeLock(self.client, topicKey, string(valueB), ETCD_TTL)
	err = lock.Lock()
	if err != nil {
		if err == etcdlock.ErrSeizeLockAg {
			return nil
		}
		return err
	}
	self.topicLockMap[topicKey] = lock

	return nil
}

//func (self *NsqdEtcdMgr) processTopicLeaderEvents(master etcdlock.Master, topicLeader chan *TopicLeaderSession, masterChanInfo *MasterChanInfo) {
//	for {
//		select {
//		case e := <-master.GetEventsChan():
//			if e.Type == etcdlock.MASTER_ADD || e.Type == etcdlock.MASTER_MODIFY {
//				// Acquired the lock || lock change.
//				var topicLeaderSession TopicLeaderSession
//				if err := json.Unmarshal([]byte(e.Master), &topicLeaderSession); err != nil {
//					topicLeaderSession.LeaderNode = nil
//					topicLeader <- &topicLeaderSession
//					continue
//				}
//				topicLeader <- &topicLeaderSession
//			} else if e.Type == etcdlock.MASTER_DELETE {
//				// Lost the lock.
//				topicLeaderSession := &TopicLeaderSession{
//					LeaderNode: nil,
//				}
//				topicLeader <- topicLeaderSession
//			} else {
//				// lock error.
//			}
//		case <-masterChanInfo.processStopCh:
//			master.Stop()
//			masterChanInfo.stoppedCh <- true
//			return
//		}
//	}
//}

func (self *NsqdEtcdMgr) ReleaseTopicLeader(topic string, partition int, session *TopicLeaderSession) error {
	topicKey := self.createTopicLockKey(topic, partition)
	v, ok := self.topicLockMap[topicKey]
	if ok {
		err := v.Unlock()
		if err != nil {
			if err == etcdlock.ErrEtcdBad {
				return err
			}
		}
		delete(self.topicLockMap, topicKey)

		return err
	} else {
		return fmt.Errorf("topicLockMap key[%s] not found.", topicKey)
	}

	return nil
}

func (self *NsqdEtcdMgr) GetAllLookupdNodes() ([]NsqLookupdNodeInfo, error) {
	rsp, err := self.client.Get(self.lookupdRoot, false, false)
	if err != nil {
		return nil, err
	}
	lookupdNodeList := make([]NsqLookupdNodeInfo, 0)
	for _, node := range rsp.Node.Nodes {
		var nodeInfo NsqLookupdNodeInfo
		if err = json.Unmarshal([]byte(node.Value), &nodeInfo); err != nil {
			continue
		}
		lookupdNodeList = append(lookupdNodeList, nodeInfo)
	}
	return lookupdNodeList, nil
}

func (self *NsqdEtcdMgr) WatchLookupdLeader(leader chan *NsqLookupdNodeInfo, stop chan struct{}) error {
	watchCh := make(chan *etcd.Response, 1)
	watchFailCh := make(chan bool, 1)
	watchStopCh := make(chan bool, 1)

	key := self.createLookupdLeaderPath()

	rsp, err := self.client.Get(key, false, false)
	if err == nil {
		var lookupdInfo NsqLookupdNodeInfo
		err = json.Unmarshal([]byte(rsp.Node.Value), &lookupdInfo)
		if err == nil {
			select {
			case leader <- &lookupdInfo:
			case <-stop:
				close(leader)
				return
			}
		}
	}

	go self.watch(key, watchCh, watchStopCh, watchFailCh)

	for {
		select {
		case rsp := <-watchCh:
			if rsp == nil {
				continue
			}
			var lookupdInfo NsqLookupdNodeInfo
			err := json.Unmarshal([]byte(rsp.Node.Value), &lookupdInfo)
			if err != nil {
				continue
			}
			leader <- &lookupdInfo
		case <-watchFailCh:
			watchCh = make(chan *etcd.Response, 1)
			go self.watch(key, watchCh, watchStopCh, watchFailCh)
		case <-stop:
			close(watchStopCh)
			close(leader)
			return nil
		}
	}

	return nil
}

func (self *NsqdEtcdMgr) GetTopicInfo(topic string, partition int) (*TopicPartionMetaInfo, error) {
	rsp, err := self.client.Get(self.createTopicInfoPath(topic, partition), false, false)
	if err != nil {
		return nil, err
	}
	var topicInfo TopicPartionMetaInfo
	if err = json.Unmarshal([]byte(rsp.Node.Value), &topicInfo); err != nil {
		return nil, err
	}
	topicInfo.Epoch = EpochType(rsp.Node.ModifiedIndex)
	return &topicInfo, nil
}

func (self *NsqdEtcdMgr) GetTopicLeaderSession(topic string, partition int) (*TopicLeaderSession, error) {
	rsp, err := self.client.Get(self.createTopicLeaderPath(topic, partition), false, false)
	if err != nil {
		return nil, err
	}
	var topicLeaderSession TopicLeaderSession
	if err = json.Unmarshal([]byte(rsp.Node.Value), &topicLeaderSession); err != nil {
		return nil, err
	}
	return &topicLeaderSession, nil
}

func (self *NsqdEtcdMgr) watch(key string, watchCh chan *etcd.Response, watchStopCh chan bool, watchFailCh chan bool) {
	_, err := self.client.Watch(key, 0, true, watchCh, watchStopCh)
	if err == etcd.ErrWatchStoppedByUser {
		return
	} else {
		watchFailCh <- true
	}
}

func (self *NsqdEtcdMgr) createNsqdNodePath(nodeData *NsqdNodeInfo) string {
	return path.Join("/", NSQ_ROOT_DIR, self.clusterID, NSQ_NODE_DIR, "Node-"+nodeData.ID)
}

func (self *NsqdEtcdMgr) createTopicRootPath() string {
	return path.Join("/", NSQ_ROOT_DIR, self.clusterID, NSQ_TOPIC_DIR)
}

func (self *NsqdEtcdMgr) createLookupdRootPath() string {
	return path.Join("/", NSQ_ROOT_DIR, self.clusterID, NSQ_LOOKUPD_DIR, NSQ_LOOKUPD_NODE_DIR)
}

func (self *NsqdEtcdMgr) createLookupdLeaderPath() string {
	return path.Join("/", NSQ_ROOT_DIR, self.clusterID, NSQ_LOOKUPD_DIR, NSQ_LOOKUPD_LEADER_SESSION)
}

func (self *NsqdEtcdMgr) createTopicPartitionPath(topic string, partition int) string {
	return path.Join(self.topicRoot, topic, strconv.Itoa(partition))
}

func (self *NsqdEtcdMgr) createTopicInfoPath(topic string, partition int) string {
	return path.Join(self.topicRoot, topic, strconv.Itoa(partition), NSQ_TOPIC_INFO)
}

func (self *NsqdEtcdMgr) createTopicLeaderPath(topic string, partition int) string {
	return path.Join(self.topicRoot, topic, strconv.Itoa(partition), NSQ_TOPIC_LEADER_SESSION)
}

func (self *NsqdEtcdMgr) createTopicLockKey(topic string, partition int) string {
	return self.clusterID + "/" + topic + "|" + strconv.Itoa(partition)
}
