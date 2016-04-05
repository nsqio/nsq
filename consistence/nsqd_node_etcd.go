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

	"git.qima-inc.com/zhoulindong/hauntlock"
	"github.com/coreos/go-etcd/etcd"
)

type NsqdEtcdMgr struct {
	client    *etcd.Client
	clusterID string
	topicRoot string

	topicLockMap map[string]*haunt_lock.HauntTimingRWLock

	watchLookupdLeaderStopCh chan bool
}

func NewNsqdEtcdMgr(client *etcd.Client) *NsqdEtcdMgr {
	return &NsqdEtcdMgr{
		client:                   client,
		topicLockMap:             make(map[string]*haunt_lock.HauntTimingRWLock),
		watchLookupdLeaderStopCh: make(chan bool, 1),
	}
}

func (self *NsqdEtcdMgr) InitClusterID(id string) {
	self.clusterID = id
	self.topicRoot = self.createTopicRootPath()
}

func (self *NsqdEtcdMgr) Register(nodeData *NsqdNodeInfo) error {
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

func (self *NsqdEtcdMgr) Unregister(nodeData *NsqdNodeInfo) error {
	// clear
	close(self.watchLookupdLeaderStopCh)
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

func (self *NsqdEtcdMgr) AcquireTopicLeader(topic string, partition int, nodeData *NsqdNodeInfo) error {
	valueB, err := json.Marshal(nodeData)
	if err != nil {
		return err
	}
	topicKey := self.createTopicLockKey(topic, partition)
	lock := haunt_lock.NewHauntTimingRWLock(self.client, haunt_lock.H_LOCK_WRITE, ETCD_LOCK_NSQ_NAMESPACE, topicKey, string(valueB), 15)
	err = lock.Lock()
	if err != nil {
		return err
	}
	self.topicLockMap[topicKey] = lock
	return nil
}

func (self *NsqdEtcdMgr) ReleaseTopicLeader(topic string, partition int) error {
	topicKey := self.createTopicLockKey(topic, partition)
	v, ok := self.topicLockMap[topicKey]
	if ok {
		v.Unlock()
		delete(self.topicLockMap, topicKey)
	} else {
		return fmt.Errorf("topicLockMap key[%s] not found.", topicKey)
	}
	return nil
}

func (self *NsqdEtcdMgr) WatchLookupdLeader(key string, leader chan *NsqLookupdNodeInfo, stop chan struct{}) error {
	watchCh := make(chan *etcd.Response, 1)
	watchFailCh := make(chan bool, 1)

	go self.watch(key, watchCh, self.watchLookupdLeaderStopCh, watchFailCh)

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
			watchCh := make(chan *etcd.Response, 1)
			go self.watch(key, watchCh, self.watchLookupdLeaderStopCh, watchFailCh)
		case <-stop:
			close(self.watchLookupdLeaderStopCh)
			return nil
		case <-self.watchLookupdLeaderStopCh:
			return nil
		}
	}

	return nil
}

func (self *NsqdEtcdMgr) GetTopicInfo(topic string, partition int) (*TopicPartionMetaInfo, error) {
	rsp, err := self.client.Get(self.createTopicPartitionPath(topic, partition), false, false)
	if err != nil {
		return nil, err
	}
	replicasPath := self.createTopicReplicasPath(topic, partition)
	catchupPath := self.createTopicCatchupPath(topic, partition)
	channelsPath := self.createTopicChannelsPath(topic, partition)
	topicInfo := &TopicPartionMetaInfo{
		Name:      topic,
		Partition: partition,
	}
	for _, node := range rsp.Node.Nodes {
		switch node.Key {
		case replicasPath:
			var replicasInfo TopicReplicasInfo
			err := json.Unmarshal([]byte(node.Value), &replicasInfo)
			if err != nil {
				continue
			}
			topicInfo.Leader = replicasInfo.Leader
			topicInfo.ISR = replicasInfo.ISR
		case catchupPath:
			var catchupInfo []string
			err := json.Unmarshal([]byte(node.Value), &catchupInfo)
			if err != nil {
				continue
			}
			topicInfo.CatchupList = catchupInfo
		case channelsPath:
			var channelsInfo map[string]string
			err := json.Unmarshal([]byte(node.Value), &channelsInfo)
			if err != nil {
				continue
			}
			channels := make([]string, 0)
			for _, v := range channelsInfo {
				channels = append(channels, v)
			}
			topicInfo.Channels = channels
		}
	}
	topicInfo.Epoch = int(rsp.Node.ModifiedIndex)
	return topicInfo, nil
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

func (self *NsqdEtcdMgr) createTopicPartitionPath(topic string, partition int) string {
	return path.Join(self.topicRoot, topic, strconv.Itoa(partition))
}

func (self *NsqdEtcdMgr) createTopicReplicasPath(topic string, partition int) string {
	return path.Join(self.topicRoot, topic, strconv.Itoa(partition), NSQ_TOPIC_REPLICAS)
}

func (self *NsqdEtcdMgr) createTopicCatchupPath(topic string, partition int) string {
	return path.Join(self.topicRoot, topic, strconv.Itoa(partition), NSQ_TOPIC_CATCHUP)
}

func (self *NsqdEtcdMgr) createTopicChannelsPath(topic string, partition int) string {
	return path.Join(self.topicRoot, topic, strconv.Itoa(partition), NSQ_TOPIC_CHANNELS)
}

func (self *NsqdEtcdMgr) createTopicLockKey(topic string, partition int) string {
	return self.clusterID + "|" + topic + "|" + strconv.Itoa(partition)
}
