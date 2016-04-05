//        file: consistence/nsq_lookupd_etcd.go
// description: opr of nsq lookupd to etcd

//      author: reezhou
//       email: reechou@gmail.com
//   copyright: youzan

package consistence

import (
	"encoding/json"
	"fmt"
	"path"
	"strconv"
	"strings"

	"git.qima-inc.com/zhoulindong/hauntlock"
	"github.com/coreos/go-etcd/etcd"
)

type NsqLookupdEtcdMgr struct {
	client            *etcd.Client
	clusterID         string
	topicRoot         string
	leaderSessionPath string
	leaderStr         string
	partitionNum      int
	replica           int
	topicMetaInfos    []*TopicPartionMetaInfo
	ifTopicChanged    bool
	nodeInfo          *NsqLookupdNodeInfo

	watchTopicLeaderStopCh chan bool
	watchTopicsStopCh      chan bool
	watchNsqdNodesStopCh   chan bool
}

func NewNsqLookupdEtcdMgr(client *etcd.Client) *NsqLookupdEtcdMgr {
	return &NsqLookupdEtcdMgr{
		client:                 client,
		ifTopicChanged:         true,
		watchTopicLeaderStopCh: make(chan bool, 1),
		watchTopicsStopCh:      make(chan bool, 1),
		watchNsqdNodesStopCh:   make(chan bool, 1),
	}
}

func (self *NsqLookupdEtcdMgr) InitClusterID(id string) {
	self.clusterID = id
	self.topicRoot = self.createTopicRootPath()
	self.leaderSessionPath = self.createLookupdLeaderPath()
	go self.watchTopics()
}

func (self *NsqLookupdEtcdMgr) Register(value *NsqLookupdNodeInfo) error {
	self.nodeInfo = value
	valueB, err := json.Marshal(value)
	if err != nil {
		return err
	}
	self.leaderStr = string(valueB)
	_, err = self.client.Create(self.createLookupdPath(value), self.leaderStr, 0)
	if err != nil {
		return err
	}
	return nil
}

func (self *NsqLookupdEtcdMgr) Unregister() error {
	_, err := self.client.Delete(self.createLookupdPath(self.nodeInfo), false)
	if err != nil {
		return err
	}
	return nil
}

func (self *NsqLookupdEtcdMgr) Stop() {
	//	self.Unregister()
	if self.watchTopicLeaderStopCh != nil {
		close(self.watchTopicLeaderStopCh)
	}
	if self.watchNsqdNodesStopCh != nil {
		close(self.watchNsqdNodesStopCh)
	}
	if self.watchTopicsStopCh != nil {
		close(self.watchTopicsStopCh)
	}
}

func (self *NsqLookupdEtcdMgr) AcquireAndWatchLeader(leader chan *NsqLookupdNodeInfo, stop chan struct{}) {
	master := haunt_lock.NewMaster(self.client, ETCD_LOCK_NSQ_NAMESPACE, self.leaderSessionPath, self.leaderStr, 15)
	go self.processMasterEvents(master, leader, stop)
	master.Start()
}

func (self *NsqLookupdEtcdMgr) processMasterEvents(master haunt_lock.Master, leader chan *NsqLookupdNodeInfo, stop chan struct{}) {
	for {
		select {
		case e := <-master.GetEventsChan():
			if e.Type == haunt_lock.MASTER_ADD {
				// Acquired the lock.
				self.setLookupdLeaderSession()
				leader <- self.nodeInfo
			} else if e.Type == haunt_lock.MASTER_DELETE {
				// Lost the lock.
				self.deleteLookupdLeaderSession()
			} else {
				// Lock ownership changed.
				var lookupdNode NsqLookupdNodeInfo
				err := json.Unmarshal([]byte(e.Master), &lookupdNode)
				if err != nil {
					continue
				}
				leader <- &lookupdNode
			}
		case <-stop:
			master.Stop()
			return
		}
	}
}

func (self *NsqLookupdEtcdMgr) setLookupdLeaderSession() error {
	_, err := self.client.Set(self.leaderSessionPath, self.leaderStr, 0)
	if err != nil {
		return err
	}
	return nil
}

func (self *NsqLookupdEtcdMgr) deleteLookupdLeaderSession() error {
	_, err := self.client.CompareAndDelete(self.leaderSessionPath, self.leaderStr, 0)
	if err != nil {
		return err
	}
	return nil
}

func (self *NsqLookupdEtcdMgr) CheckIfLeader(session string) bool {
	rsp, err := self.client.Get(self.leaderSessionPath, false, false)
	if err != nil {
		return false
	}
	if rsp.Node.Value == session {
		return true
	}
	return false
}

func (self *NsqLookupdEtcdMgr) UpdateLookupEpoch(key string, oldGen int) (int, error) {
	return 0, nil
}

func (self *NsqLookupdEtcdMgr) WatchNsqdNodes(nsqds chan []*NsqdNodeInfo, stop chan struct{}) {
	watchCh := make(chan *etcd.Response, 1)
	watchFailCh := make(chan bool, 1)

	go self.watch(self.createNsqdRootPath(), watchCh, self.watchNsqdNodesStopCh, watchFailCh)

	for {
		select {
		case rsp := <-watchCh:
			if rsp == nil {
				continue
			}
			nsqdNodes, err := self.getNsqdNodes()
			if err != nil {
				continue
			}
			nsqds <- nsqdNodes
		case <-watchFailCh:
			watchCh = make(chan *etcd.Response, 1)
			go self.watch(self.createNsqdRootPath(), watchCh, self.watchNsqdNodesStopCh, watchFailCh)
		case <-stop:
			close(self.watchNsqdNodesStopCh)
			return
		case <-self.watchNsqdNodesStopCh:
			return
		}
	}
}

func (self *NsqLookupdEtcdMgr) getNsqdNodes() ([]*NsqdNodeInfo, error) {
	rsp, err := self.client.Get(self.createNsqdRootPath(), false, false)
	if err != nil {
		return nil, err
	}
	nsqdNodes := make([]*NsqdNodeInfo, 0)
	for _, node := range rsp.Node.Nodes {
		if node.Dir {
			continue
		}
		var nodeInfo NsqdNodeInfo
		err := json.Unmarshal([]byte(node.Value), &nodeInfo)
		if err != nil {
			continue
		}
		nsqdNodes = append(nsqdNodes, &nodeInfo)
	}
	return nsqdNodes, nil
}

func (self *NsqLookupdEtcdMgr) ScanTopics() ([]*TopicPartionMetaInfo, error) {
	if self.ifTopicChanged {
		return self.scanTopics()
	}
	return self.topicMetaInfos, nil
}

func (self *NsqLookupdEtcdMgr) watchTopics() {
	watchCh := make(chan *etcd.Response, 1)
	watchFailCh := make(chan bool, 1)

	go self.watch(self.topicRoot, watchCh, self.watchTopicsStopCh, watchFailCh)

	for {
		select {
		case <-watchCh:
			self.ifTopicChanged = true
		case <-watchFailCh:
			watchCh = make(chan *etcd.Response, 1)
			go self.watch(self.createTopicRootPath(), watchCh, self.watchTopicsStopCh, watchFailCh)
		case <-self.watchTopicsStopCh:
			return
		}
	}
}

func (self *NsqLookupdEtcdMgr) scanTopics() ([]*TopicPartionMetaInfo, error) {
	rsp, err := self.client.Get(self.createTopicRootPath(), false, true)
	if err != nil {
		return nil, err
	}
	topicMap := make(map[string]map[string]*TopicPartionMetaInfo)
	for _, node := range rsp.Node.Nodes {
		if node.Dir {
			continue
		}
		keys := strings.Split(node.Key, "/")
		keyLen := len(keys)
		if keyLen < 3 {
			continue
		}
		topicName := keys[keyLen-3]
		partition := keys[keyLen-2]
		v, ok := topicMap[topicName]
		if !ok {
			partitionMap := make(map[string]*TopicPartionMetaInfo)
			var topicInfo TopicPartionMetaInfo
			self.parseTopicInfo(keys[keyLen-1], node.Value, node.ModifiedIndex, &topicInfo)
			partitionMap[partition] = &topicInfo
			topicMap[topicName] = partitionMap
		} else {
			vp, ok2 := v[partition]
			if !ok2 {
				var topicInfo TopicPartionMetaInfo
				self.parseTopicInfo(keys[keyLen-1], node.Value, node.ModifiedIndex, &topicInfo)
				v[partition] = &topicInfo
			} else {
				self.parseTopicInfo(keys[keyLen-1], node.Value, node.ModifiedIndex, vp)
			}
		}
	}
	self.topicMetaInfos = make([]*TopicPartionMetaInfo, 0)
	for k, v := range topicMap {
		for k2, v2 := range v {
			v2.Name = k
			v2.Partition, err = strconv.Atoi(k2)
			if err != nil {
				continue
			}
			v2.Replica = self.replica
			self.topicMetaInfos = append(self.topicMetaInfos, v2)
		}
	}
	self.ifTopicChanged = false
	return self.topicMetaInfos, nil
}

func (self *NsqLookupdEtcdMgr) parseTopicInfo(key string, value string, modifyIdx uint64, topicInfo *TopicPartionMetaInfo) {
	epoch := int(modifyIdx)
	switch key {
	case NSQ_TOPIC_REPLICAS:
		var replicasInfo TopicReplicasInfo
		err := json.Unmarshal([]byte(value), &replicasInfo)
		if err != nil {
			return
		}
		topicInfo.Leader = replicasInfo.Leader
		topicInfo.ISR = replicasInfo.ISR
		if epoch > topicInfo.Epoch {
			topicInfo.Epoch = epoch
		}
	case NSQ_TOPIC_CATCHUP:
		var catchupInfo []string
		err := json.Unmarshal([]byte(value), &catchupInfo)
		if err != nil {
			return
		}
		topicInfo.CatchupList = catchupInfo
		if epoch > topicInfo.Epoch {
			topicInfo.Epoch = epoch
		}
	case NSQ_TOPIC_CHANNELS:
		var channelsInfo map[string]string
		err := json.Unmarshal([]byte(value), &channelsInfo)
		if err != nil {
			return
		}
		channels := make([]string, 0)
		for _, v := range channelsInfo {
			channels = append(channels, v)
		}
		topicInfo.Channels = channels
		if epoch > topicInfo.Epoch {
			topicInfo.Epoch = epoch
		}
	default:
		return
	}
}

func (self *NsqLookupdEtcdMgr) GetTopicInfo(topic string, partition int) (*TopicPartionMetaInfo, error) {
	rsp, err := self.client.Get(self.createTopicPartitionPath(topic, partition), false, false)
	if err != nil {
		return nil, err
	}
	replicasPath := self.createTopicReplicasPath(topic, partition)
	catchupPath := self.createTopicCatchupPath(topic, partition)
	channelsPath := self.createTopicChannelsPath(topic, partition)
	//	leaderSessionPath := self.createTopicLeaderSessionPath(topic, partition)
	topicInfo := &TopicPartionMetaInfo{
		Name:      topic,
		Partition: partition,
		Replica:   self.replica,
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

func (self *NsqLookupdEtcdMgr) CreateTopicPartition(topic string, partition int) error {
	_, err := self.client.CreateDir(self.createTopicPartitionPath(topic, partition), 0)
	if err != nil {
		return err
	}
	return nil
}

func (self *NsqLookupdEtcdMgr) CreateTopic(topic string, partitionNum int, replica int) error {
	self.partitionNum = partitionNum
	self.replica = replica
	metaInfo := &TopicMetaInfo{
		PartitionNum: partitionNum,
		Replica:      replica,
	}
	metaValue, err := json.Marshal(metaInfo)
	if err != nil {
		return err
	}
	_, err = self.client.Create(self.createTopicMetaPath(topic), string(metaValue), 0)
	if err != nil {
		return err
	}
	return nil
}

func (self *NsqLookupdEtcdMgr) IsExistTopic(topic string) (bool, error) {
	_, err := self.client.Get(self.createTopicPath(topic), false, false)
	if err != nil {
		if CheckKeyIfExist(err) {
			return false, nil
		} else {
			return false, err
		}
	}
	return true, nil
}

func (self *NsqLookupdEtcdMgr) IsExistTopicPartition(topic string, partitionNum int) (bool, error) {
	_, err := self.client.Get(self.createTopicPartitionPath(topic, partitionNum), false, false)
	if err != nil {
		if CheckKeyIfExist(err) {
			return false, nil
		} else {
			return false, err
		}
	}
	return true, nil
}

func (self *NsqLookupdEtcdMgr) GetTopicPartitionNum(topic string) (int, error) {
	rsp, err := self.client.Get(self.createTopicMetaPath(topic), false, false)
	if err != nil {
		return 0, err
	}
	var metaInfo TopicMetaInfo
	err = json.Unmarshal([]byte(rsp.Node.Value), &metaInfo)
	return metaInfo.PartitionNum, nil
}

func (self *NsqLookupdEtcdMgr) DeleteTopic(topic string, partition int) error {
	_, err := self.client.Delete(self.createTopicPartitionPath(topic, partition), true)
	if err != nil {
		return err
	}
	return nil
}

func (self *NsqLookupdEtcdMgr) CreateTopicNodeInfo(topic string, partition int, topicInfo *TopicPartionMetaInfo) (error, int) {
	replicas := &TopicReplicasInfo{
		Leader: topicInfo.Leader,
		ISR:    topicInfo.ISR,
	}
	value, err := json.Marshal(replicas)
	if err != nil {
		return err, 0
	}
	rsp, err := self.client.Create(self.createTopicReplicasPath(topic, partition), string(value), 0)
	if err != nil {
		return err, 0
	}
	return nil, int(rsp.Node.ModifiedIndex)
}

func (self *NsqLookupdEtcdMgr) UpdateTopicNodeInfo(topic string, partition int, topicInfo *TopicPartionMetaInfo, oldGen int) error {
	replicas := &TopicReplicasInfo{
		Leader: topicInfo.Leader,
		ISR:    topicInfo.ISR,
	}
	value, err := json.Marshal(replicas)
	if err != nil {
		return err
	}
	_, err = self.client.CompareAndSwap(self.createTopicReplicasPath(topic, partition), string(value), 0, "", uint64(oldGen))
	if err != nil {
		return err
	}
	return nil
}

func (self *NsqLookupdEtcdMgr) CreateTopicCatchupList(topic string, partition int, catchupList []string) (error, int) {
	value, err := json.Marshal(catchupList)
	if err != nil {
		return err, 0
	}
	rsp, err := self.client.Create(self.createTopicCatchupPath(topic, partition), string(value), 0)
	if err != nil {
		return err, 0
	}
	return nil, int(rsp.Node.ModifiedIndex)
}

func (self *NsqLookupdEtcdMgr) UpdateTopicCatchupList(topic string, partition int, catchupList []string, oldGen int) error {
	value, err := json.Marshal(catchupList)
	if err != nil {
		return err
	}
	_, err = self.client.CompareAndSwap(self.createTopicCatchupPath(topic, partition), string(value), 0, "", uint64(oldGen))
	if err != nil {
		return err
	}
	return nil
}

func (self *NsqLookupdEtcdMgr) CreateChannel(topic string, partition int, channel string) error {
	channelPath := self.createTopicChannelsPath(topic, partition)
	rsp, err := self.client.Get(channelPath, false, false)
	if err != nil {
		if CheckKeyIfExist(err) {
			channelMap := make(map[string]string)
			channelMap[channel] = channel
			channelB, err := json.Marshal(&channelMap)
			if err != nil {
				return err
			}
			_, err = self.client.Create(channelPath, string(channelB), 0)
			if err != nil {
				return err
			}
			return nil
		}
		return err
	}
	channelMap := make(map[string]string)
	err = json.Unmarshal([]byte(rsp.Node.Value), &channelMap)
	if err != nil {
		return err
	}
	_, ok := channelMap[channel]
	if ok {
		return nil
	}
	channelMap[channel] = channel
	channelB, err := json.Marshal(&channelMap)
	if err != nil {
		return err
	}
	_, err = self.client.CompareAndSwap(channelPath, string(channelB), 0, rsp.Node.Value, 0)
	if err != nil {
		return err
	}
	return nil
}

func (self *NsqLookupdEtcdMgr) DeleteChannel(topic string, partition int, channel string) error {
	channelPath := self.createTopicChannelsPath(topic, partition)
	rsp, err := self.client.Get(channelPath, false, false)
	if err != nil {
		return err
	}
	channelMap := make(map[string]string)
	err = json.Unmarshal([]byte(rsp.Node.Value), &channelMap)
	if err != nil {
		return err
	}
	_, ok := channelMap[channel]
	if !ok {
		return nil
	}
	delete(channelMap, channel)
	channelB, err := json.Marshal(&channelMap)
	if err != nil {
		return err
	}
	_, err = self.client.CompareAndSwap(channelPath, string(channelB), 0, rsp.Node.Value, 0)
	if err != nil {
		return err
	}
	return nil
}

func (self *NsqLookupdEtcdMgr) GetTopicLeaderSession(topic string, partition int) (*TopicLeaderSession, error) {
	rsp, err := self.client.Get(self.createTopicLockPath(topic, partition), true, true)
	if err != nil {
		return nil, err
	}
	for i, node := range rsp.Node.Nodes {
		if i == 0 {
			_, token := path.Split(node.Key)
			var leaderNode NsqdNodeInfo
			err = json.Unmarshal([]byte(node.Value), &leaderNode)
			if err != nil {
				return nil, err
			}
			leader := &TopicLeaderSession{
				ClusterID:   self.clusterID,
				Topic:       topic,
				Partition:   partition,
				LeaderNode:  &leaderNode,
				Session:     token,
				LeaderEpoch: int(node.ModifiedIndex),
			}
			return leader, nil
		}
	}
	return nil, fmt.Errorf("None rsp nodes.")
}

// maybe use: go WatchTopicLeader()...
func (self *NsqLookupdEtcdMgr) WatchTopicLeader(leader chan *TopicLeaderSession, stop chan struct{}) error {
	watchCh := make(chan *etcd.Response, 1)
	watchFailCh := make(chan bool, 1)

	go self.watch(self.createTopicLockRoot(), watchCh, self.watchTopicLeaderStopCh, watchFailCh)

	for {
		select {
		case rsp := <-watchCh:
			if rsp == nil {
				continue
			}
			keys := strings.Split(rsp.Node.Key, "/")
			lenK := len(keys)
			if lenK < 2 {
				continue
			}
			session := keys[lenK-1]
			clusterID, topic, partition, err := self.splitTopicLockKey(keys[lenK-2])
			if err != nil {
				continue
			}
			if rsp.Action == "expire" || rsp.Action == "delete" {
				fmt.Println(rsp.Action)
				leaderS := &TopicLeaderSession{
					ClusterID: clusterID,
					Topic:     topic,
					Partition: partition,
					Session:   session,
				}
				leader <- leaderS
			} else if rsp.Action == "create" {
				lockValue, err := haunt_lock.ParseTimingLockValue(rsp.Node.Value)
				if err != nil {
					continue
				}
				var nsqNodeInfo NsqdNodeInfo
				err = json.Unmarshal([]byte(lockValue), &nsqNodeInfo)
				if err != nil {
					continue
				}
				leaderS := &TopicLeaderSession{
					ClusterID:   clusterID,
					Topic:       topic,
					Partition:   partition,
					LeaderNode:  &nsqNodeInfo,
					Session:     session,
					LeaderEpoch: int(rsp.Node.ModifiedIndex),
				}
				//				fmt.Println("leaderS:", rsp.Action, rsp.Node.Key, rsp.Node.Value)
				leader <- leaderS
			}
		case <-watchFailCh:
			watchCh = make(chan *etcd.Response, 1)
			go self.watch(self.createTopicLockRoot(), watchCh, self.watchTopicLeaderStopCh, watchFailCh)
		case <-stop:
			close(self.watchTopicLeaderStopCh)
			return nil
		case <-self.watchTopicLeaderStopCh:
			return nil
		}
	}

	return nil
}

func (self *NsqLookupdEtcdMgr) watch(key string, watchCh chan *etcd.Response, watchStopCh chan bool, watchFailCh chan bool) {
	_, err := self.client.Watch(key, 0, true, watchCh, watchStopCh)
	if err == etcd.ErrWatchStoppedByUser {
		return
	} else {
		watchFailCh <- true
	}
}

func (self *NsqLookupdEtcdMgr) createLookupdPath(value *NsqLookupdNodeInfo) string {
	return path.Join("/", NSQ_ROOT_DIR, self.clusterID, NSQ_LOOKUPD_DIR, "Node-"+value.ID)
}

func (self *NsqLookupdEtcdMgr) createLookupdLeaderPath() string {
	return path.Join("/", NSQ_ROOT_DIR, self.clusterID, NSQ_LOOKUPD_LEADER_SESSION)
}

func (self *NsqLookupdEtcdMgr) createNsqdRootPath() string {
	return path.Join("/", NSQ_ROOT_DIR, self.clusterID, NSQ_NODE_DIR)
}

func (self *NsqLookupdEtcdMgr) createTopicRootPath() string {
	return path.Join("/", NSQ_ROOT_DIR, self.clusterID, NSQ_TOPIC_DIR)
}

func (self *NsqLookupdEtcdMgr) createTopicPath(topic string) string {
	return path.Join(self.topicRoot, topic)
}

func (self *NsqLookupdEtcdMgr) createTopicMetaPath(topic string) string {
	return path.Join(self.topicRoot, topic, NSQ_TOPIC_META)
}

func (self *NsqLookupdEtcdMgr) createTopicPartitionPath(topic string, partition int) string {
	return path.Join(self.topicRoot, topic, strconv.Itoa(partition))
}

func (self *NsqLookupdEtcdMgr) createTopicReplicasPath(topic string, partition int) string {
	return path.Join(self.topicRoot, topic, strconv.Itoa(partition), NSQ_TOPIC_REPLICAS)
}

func (self *NsqLookupdEtcdMgr) createTopicCatchupPath(topic string, partition int) string {
	return path.Join(self.topicRoot, topic, strconv.Itoa(partition), NSQ_TOPIC_CATCHUP)
}

func (self *NsqLookupdEtcdMgr) createTopicChannelsPath(topic string, partition int) string {
	return path.Join(self.topicRoot, topic, strconv.Itoa(partition), NSQ_TOPIC_CHANNELS)
}

func (self *NsqLookupdEtcdMgr) createTopicLeaderSessionPath(topic string, partition int) string {
	return path.Join(self.topicRoot, topic, strconv.Itoa(partition), NSQ_TOPIC_LEADER_SESSION)
}

func (self *NsqLookupdEtcdMgr) createTopicLockRoot() string {
	return path.Join(haunt_lock.HAUNT_TIMING_LOCK_DIR, ETCD_LOCK_NSQ_NAMESPACE)
}

func (self *NsqLookupdEtcdMgr) createTopicLockPath(topic string, partition int) string {
	return path.Join(haunt_lock.HAUNT_TIMING_LOCK_DIR, ETCD_LOCK_NSQ_NAMESPACE, self.clusterID+"|"+topic+"|"+strconv.Itoa(partition))
}

func (self *NsqLookupdEtcdMgr) splitTopicLockKey(key string) (string, string, int, error) {
	infos := strings.Split(key, "|")
	if len(infos) < 3 {
		return "", "", 0, fmt.Errorf("infos's len < 3")
	}
	partition, _ := strconv.Atoi(infos[2])
	return infos[0], infos[1], partition, nil
}
