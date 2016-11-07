//        file: consistence/nsq_lookupd_etcd.go
// description: opr of nsq lookupd to etcd

//      author: reezhou
//       email: reechou@gmail.com
//   copyright: youzan

package consistence

import (
	"encoding/json"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coreos/etcd/client"
	etcdlock "github.com/reechou/xlock2"
	"golang.org/x/net/context"
)

const (
	EVENT_WATCH_TOPIC_L_CREATE = iota
	EVENT_WATCH_TOPIC_L_DELETE
)

type WatchTopicLeaderInfo struct {
	event       int
	topic       string
	partition   int
	watchStopCh chan bool
	stoppedCh   chan bool
}

type NsqLookupdEtcdMgr struct {
	tmisMutex sync.Mutex
	tmiMutex  sync.Mutex
	wtliMutex sync.Mutex
	itcMutex  sync.Mutex

	client            *etcdlock.EtcdClient
	clusterID         string
	topicRoot         string
	clusterPath       string
	leaderSessionPath string
	leaderStr         string
	lookupdRootPath   string
	topicMetaInfos    []TopicPartitionMetaInfo
	topicMetaMap      map[string]*TopicMetaInfo
	ifTopicChanged    bool
	nodeInfo          *NsqLookupdNodeInfo
	nodeKey           string
	nodeValue         string

	watchTopicLeaderChanMap   map[string]*WatchTopicLeaderInfo
	watchTopicLeaderEventChan chan *WatchTopicLeaderInfo

	refreshStopCh          chan bool
	watchTopicLeaderStopCh chan bool
	watchTopicsStopCh      chan bool
	watchNsqdNodesStopCh   chan bool
}

func NewNsqLookupdEtcdMgr(host string) *NsqLookupdEtcdMgr {
	client := etcdlock.NewEClient(host)
	return &NsqLookupdEtcdMgr{
		client:                    client,
		ifTopicChanged:            true,
		watchTopicLeaderStopCh:    make(chan bool, 1),
		watchTopicsStopCh:         make(chan bool, 1),
		watchNsqdNodesStopCh:      make(chan bool, 1),
		topicMetaMap:              make(map[string]*TopicMetaInfo),
		watchTopicLeaderChanMap:   make(map[string]*WatchTopicLeaderInfo),
		watchTopicLeaderEventChan: make(chan *WatchTopicLeaderInfo, 1),
		refreshStopCh:             make(chan bool, 1),
	}
}

func (self *NsqLookupdEtcdMgr) InitClusterID(id string) {
	self.clusterID = id
	self.topicRoot = self.createTopicRootPath()
	self.clusterPath = self.createClusterPath()
	self.leaderSessionPath = self.createLookupdLeaderPath()
	self.lookupdRootPath = self.createLookupdRootPath()
	go self.watchTopics()
}

func (self *NsqLookupdEtcdMgr) Register(value *NsqLookupdNodeInfo) error {
	self.nodeInfo = value
	valueB, err := json.Marshal(value)
	if err != nil {
		return err
	}
	self.leaderStr = string(valueB)
	self.nodeKey = self.createLookupdPath(value)
	self.nodeValue = string(valueB)
	_, err = self.client.Set(self.nodeKey, self.nodeValue, ETCD_TTL)
	if err != nil {
		return err
	}
	// start to refresh
	go self.refresh()

	return nil
}

func (self *NsqLookupdEtcdMgr) refresh() {
	for {
		select {
		case <-self.refreshStopCh:
			return
		case <-time.After(time.Second * time.Duration(ETCD_TTL*4/10)):
			_, err := self.client.SetWithTTL(self.nodeKey, ETCD_TTL)
			if err != nil {
				coordLog.Errorf("update error: %s", err.Error())
				_, err := self.client.Set(self.nodeKey, self.nodeValue, ETCD_TTL)
				if err != nil {
					coordLog.Errorf("set key error: %s", err.Error())
				}
			}
		}
	}
}

func (self *NsqLookupdEtcdMgr) Unregister(value *NsqLookupdNodeInfo) error {
	_, err := self.client.Delete(self.createLookupdPath(value), false)
	if err != nil {
		return err
	}
	// stop to refresh
	if self.refreshStopCh != nil {
		close(self.refreshStopCh)
		self.refreshStopCh = nil
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

func (self *NsqLookupdEtcdMgr) GetClusterEpoch() (EpochType, error) {
	rsp, err := self.client.Get(self.clusterPath, false, false)
	if err != nil {
		if client.IsKeyNotFound(err) {
			return 0, ErrKeyNotFound
		}
		return 0, err
	}

	return EpochType(rsp.Node.ModifiedIndex), nil
}

func (self *NsqLookupdEtcdMgr) GetAllLookupdNodes() ([]NsqLookupdNodeInfo, error) {
	rsp, err := self.client.Get(self.lookupdRootPath, false, false)
	if err != nil {
		if client.IsKeyNotFound(err) {
			return nil, ErrKeyNotFound
		}
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

func (self *NsqLookupdEtcdMgr) AcquireAndWatchLeader(leader chan *NsqLookupdNodeInfo, stop chan struct{}) {
	master := etcdlock.NewMaster(self.client, self.leaderSessionPath, self.leaderStr, ETCD_TTL)
	go self.processMasterEvents(master, leader, stop)
	master.Start()
}

func (self *NsqLookupdEtcdMgr) processMasterEvents(master etcdlock.Master, leader chan *NsqLookupdNodeInfo, stop chan struct{}) {
	for {
		select {
		case e := <-master.GetEventsChan():
			if e.Type == etcdlock.MASTER_ADD || e.Type == etcdlock.MASTER_MODIFY {
				// Acquired the lock || lock change.
				var lookupdNode NsqLookupdNodeInfo
				if err := json.Unmarshal([]byte(e.Master), &lookupdNode); err != nil {
					leader <- &lookupdNode
					continue
				}
				coordLog.Infof("master event type[%d] lookupdNode[%v].", e.Type, lookupdNode)
				leader <- &lookupdNode
			} else if e.Type == etcdlock.MASTER_DELETE {
				coordLog.Infof("master event delete.")
				// Lost the lock.
				var lookupdNode NsqLookupdNodeInfo
				leader <- &lookupdNode
			} else {
				// TODO: lock error.
			}
		case <-stop:
			master.Stop()
			close(leader)
			return
		}
	}
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

func (self *NsqLookupdEtcdMgr) UpdateLookupEpoch(oldGen EpochType) (EpochType, error) {
	return 0, nil
}

func (self *NsqLookupdEtcdMgr) GetNsqdNodes() ([]NsqdNodeInfo, error) {
	return self.getNsqdNodes()
}

func (self *NsqLookupdEtcdMgr) WatchNsqdNodes(nsqds chan []NsqdNodeInfo, stop chan struct{}) {
	nsqdNodes, err := self.getNsqdNodes()
	if err == nil {
		select {
		case nsqds <- nsqdNodes:
		case <-stop:
			close(nsqds)
			return
		}
	}

	key := self.createNsqdRootPath()
	watcher := self.client.Watch(key, 0, true)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-stop:
			cancel()
		case <-self.watchNsqdNodesStopCh:
			cancel()
		}
	}()
	for {
		rsp, err := watcher.Next(ctx)
		if err != nil {
			if err == context.Canceled {
				coordLog.Infof("watch key[%s] canceled.", key)
				close(nsqds)
				return
			} else {
				coordLog.Errorf("watcher key[%s] error: %s", key, err.Error())
				//rewatch
				if etcdlock.IsEtcdWatchExpired(err) {
					rsp, err = self.client.Get(key, false, true)
					if err != nil {
						coordLog.Errorf("rewatch and get key[%s] error: %s", key, err.Error())
						continue
					}
					watcher = self.client.Watch(key, rsp.Index+1, true)
					// should get the nodes to notify watcher since last watch is expired
				} else {
					time.Sleep(5 * time.Second)
					continue
				}
			}
		}
		nsqdNodes, err := self.getNsqdNodes()
		if err != nil {
			coordLog.Errorf("key[%s] getNsqdNodes error: %s", key, err.Error())
			continue
		}
		select {
		case nsqds <- nsqdNodes:
		case <-stop:
			close(nsqds)
			return
		}
	}
}

func (self *NsqLookupdEtcdMgr) getNsqdNodes() ([]NsqdNodeInfo, error) {
	rsp, err := self.client.Get(self.createNsqdRootPath(), false, false)
	if err != nil {
		if client.IsKeyNotFound(err) {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}
	nsqdNodes := make([]NsqdNodeInfo, 0)
	for _, node := range rsp.Node.Nodes {
		if node.Dir {
			continue
		}
		var nodeInfo NsqdNodeInfo
		err := json.Unmarshal([]byte(node.Value), &nodeInfo)
		if err != nil {
			continue
		}
		nsqdNodes = append(nsqdNodes, nodeInfo)
	}
	return nsqdNodes, nil
}

func (self *NsqLookupdEtcdMgr) ScanTopics() ([]TopicPartitionMetaInfo, error) {
	if self.ifTopicChanged {
		return self.scanTopics()
	}

	self.tmisMutex.Lock()
	topicMetaInfos := self.topicMetaInfos
	self.tmisMutex.Unlock()
	return topicMetaInfos, nil
}

// watch topics if changed
func (self *NsqLookupdEtcdMgr) watchTopics() {
	watcher := self.client.Watch(self.topicRoot, 0, true)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-self.watchTopicsStopCh:
			cancel()
		}
	}()
	for {
		_, err := watcher.Next(ctx)
		if err != nil {
			if err == context.Canceled {
				coordLog.Infof("watch key[%s] canceled.", self.topicRoot)
				return
			} else {
				coordLog.Errorf("watcher key[%s] error: %s", self.topicRoot, err.Error())
				//rewatch
				if etcdlock.IsEtcdWatchExpired(err) {
					rsp, err := self.client.Get(self.topicRoot, false, true)
					if err != nil {
						coordLog.Errorf("rewatch and get key[%s] error: %s", self.topicRoot, err.Error())
						continue
					}
					watcher = self.client.Watch(self.topicRoot, rsp.Index+1, true)
					// watch expired should be treated as changed of node
				} else {
					time.Sleep(5 * time.Second)
					continue
				}
			}
		}
		coordLog.Debugf("topic changed.")
		self.itcMutex.Lock()
		self.ifTopicChanged = true
		self.itcMutex.Unlock()
	}
}

func (self *NsqLookupdEtcdMgr) scanTopics() ([]TopicPartitionMetaInfo, error) {
	self.itcMutex.Lock()
	self.ifTopicChanged = false
	self.itcMutex.Unlock()

	rsp, err := self.client.Get(self.topicRoot, true, true)
	if err != nil {
		self.itcMutex.Lock()
		self.ifTopicChanged = true
		self.itcMutex.Unlock()
		if client.IsKeyNotFound(err) {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}

	topicMetaMap := make(map[string]TopicMetaInfo)
	topicReplicasMap := make(map[string]map[string]TopicPartitionReplicaInfo)
	self.processTopicNode(rsp.Node.Nodes, topicMetaMap, topicReplicasMap)

	topicMetaInfos := make([]TopicPartitionMetaInfo, 0)
	for k, v := range topicReplicasMap {
		topicMeta, ok := topicMetaMap[k]
		if !ok {
			continue
		}
		for k2, v2 := range v {
			partition, err := strconv.Atoi(k2)
			if err != nil {
				continue
			}
			var topicInfo TopicPartitionMetaInfo
			topicInfo.Name = k
			topicInfo.Partition = partition
			topicInfo.TopicMetaInfo = topicMeta
			topicInfo.TopicPartitionReplicaInfo = v2
			topicMetaInfos = append(topicMetaInfos, topicInfo)
		}
	}

	self.tmisMutex.Lock()
	self.topicMetaInfos = topicMetaInfos
	self.tmisMutex.Unlock()

	return topicMetaInfos, nil
}

func (self *NsqLookupdEtcdMgr) processTopicNode(nodes client.Nodes, topicMetaMap map[string]TopicMetaInfo, topicReplicasMap map[string]map[string]TopicPartitionReplicaInfo) {
	for _, node := range nodes {
		if node.Nodes != nil {
			self.processTopicNode(node.Nodes, topicMetaMap, topicReplicasMap)
		}
		if node.Dir {
			continue
		}
		_, key := path.Split(node.Key)
		if key == NSQ_TOPIC_REPLICA_INFO {
			var rInfo TopicPartitionReplicaInfo
			if err := json.Unmarshal([]byte(node.Value), &rInfo); err != nil {
				continue
			}
			rInfo.Epoch = EpochType(node.ModifiedIndex)
			keys := strings.Split(node.Key, "/")
			keyLen := len(keys)
			if keyLen < 3 {
				continue
			}
			topicName := keys[keyLen-3]
			partition := keys[keyLen-2]
			v, ok := topicReplicasMap[topicName]
			if ok {
				v[partition] = rInfo
			} else {
				pMap := make(map[string]TopicPartitionReplicaInfo)
				pMap[partition] = rInfo
				topicReplicasMap[topicName] = pMap
			}
		} else if key == NSQ_TOPIC_META {
			var mInfo TopicMetaInfo
			if err := json.Unmarshal([]byte(node.Value), &mInfo); err != nil {
				continue
			}
			keys := strings.Split(node.Key, "/")
			keyLen := len(keys)
			if keyLen < 2 {
				continue
			}
			topicName := keys[keyLen-2]
			topicMetaMap[topicName] = mInfo
		}
	}
}

func (self *NsqLookupdEtcdMgr) GetTopicInfo(topic string, partition int) (*TopicPartitionMetaInfo, error) {
	var topicInfo TopicPartitionMetaInfo
	self.tmiMutex.Lock()
	metaInfo, ok := self.topicMetaMap[topic]
	self.tmiMutex.Unlock()
	if !ok {
		rsp, err := self.client.Get(self.createTopicMetaPath(topic), false, false)
		if err != nil {
			if client.IsKeyNotFound(err) {
				self.itcMutex.Lock()
				self.ifTopicChanged = true
				self.itcMutex.Unlock()
				return nil, ErrKeyNotFound
			}
			return nil, err
		}
		var mInfo TopicMetaInfo
		err = json.Unmarshal([]byte(rsp.Node.Value), &mInfo)
		if err != nil {
			return nil, err
		}
		topicInfo.TopicMetaInfo = mInfo
		self.tmiMutex.Lock()
		self.topicMetaMap[topic] = &mInfo
		self.tmiMutex.Unlock()
	} else {
		topicInfo.TopicMetaInfo = *metaInfo
	}

	rsp, err := self.client.Get(self.createTopicReplicaInfoPath(topic, partition), false, false)
	if err != nil {
		if client.IsKeyNotFound(err) {
			self.itcMutex.Lock()
			self.ifTopicChanged = true
			self.itcMutex.Unlock()
			return nil, ErrKeyNotFound
		}
		return nil, err
	}
	var rInfo TopicPartitionReplicaInfo
	if err = json.Unmarshal([]byte(rsp.Node.Value), &rInfo); err != nil {
		return nil, err
	}
	rInfo.Epoch = EpochType(rsp.Node.ModifiedIndex)
	topicInfo.TopicPartitionReplicaInfo = rInfo
	topicInfo.Name = topic
	topicInfo.Partition = partition

	return &topicInfo, nil
}

func (self *NsqLookupdEtcdMgr) CreateTopicPartition(topic string, partition int) error {
	_, err := self.client.CreateDir(self.createTopicPartitionPath(topic, partition), 0)
	if err != nil {
		if IsEtcdNotFile(err) {
			return ErrKeyAlreadyExist
		}
		return err
	}
	// if replica == 1, no need watch leader session
	self.tmiMutex.Lock()
	v, ok := self.topicMetaMap[topic]
	if ok {
		if v.Replica == 1 {
			self.tmiMutex.Unlock()
			return nil
		}
	}
	self.tmiMutex.Unlock()

	// start to watch topic leader session
	watchTopicLeaderInfo := &WatchTopicLeaderInfo{
		event:       EVENT_WATCH_TOPIC_L_CREATE,
		topic:       topic,
		partition:   partition,
		watchStopCh: make(chan bool, 1),
		stoppedCh:   make(chan bool, 1),
	}
	select {
	case self.watchTopicLeaderEventChan <- watchTopicLeaderInfo:
	default:
		return nil
	}
	return nil
}

func (self *NsqLookupdEtcdMgr) CreateTopic(topic string, meta *TopicMetaInfo) error {
	metaValue, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	_, err = self.client.Create(self.createTopicMetaPath(topic), string(metaValue), 0)
	if err != nil {
		if IsEtcdNodeExist(err) {
			return ErrKeyAlreadyExist
		}
		return err
	}

	self.tmiMutex.Lock()
	self.topicMetaMap[topic] = meta
	self.tmiMutex.Unlock()

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

func (self *NsqLookupdEtcdMgr) GetTopicMetaInfo(topic string) (TopicMetaInfo, EpochType, error) {
	var metaInfo TopicMetaInfo
	rsp, err := self.client.Get(self.createTopicMetaPath(topic), false, false)
	if err != nil {
		if client.IsKeyNotFound(err) {
			return metaInfo, 0, ErrKeyNotFound
		}
		return metaInfo, 0, err
	}
	err = json.Unmarshal([]byte(rsp.Node.Value), &metaInfo)
	if err != nil {
		return metaInfo, 0, err
	}
	epoch := EpochType(rsp.Node.ModifiedIndex)
	return metaInfo, epoch, nil
}

func (self *NsqLookupdEtcdMgr) UpdateTopicMetaInfo(topic string, meta *TopicMetaInfo, oldGen EpochType) error {
	value, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	coordLog.Infof("Update_topic meta info: %s %s %d", topic, string(value), oldGen)
	_, err = self.client.CompareAndSwap(self.createTopicMetaPath(topic), string(value), 0, "", uint64(oldGen))
	if err != nil {
		return err
	}
	return nil
}

func (self *NsqLookupdEtcdMgr) DeleteWholeTopic(topic string) error {
	_, err := self.client.Delete(self.createTopicPath(topic), true)
	return err
}

func (self *NsqLookupdEtcdMgr) DeleteTopic(topic string, partition int) error {
	_, err := self.client.Delete(self.createTopicPartitionPath(topic, partition), true)
	if err != nil {
		return err
	}
	// stop watch topic leader and delete
	topicLeaderSession := self.createTopicLeaderSessionPath(topic, partition)

	self.wtliMutex.Lock()
	defer self.wtliMutex.Unlock()
	v, ok := self.watchTopicLeaderChanMap[topicLeaderSession]
	if ok {
		close(v.watchStopCh)
		<-v.stoppedCh
		delete(self.watchTopicLeaderChanMap, topicLeaderSession)
	}

	return nil
}

func (self *NsqLookupdEtcdMgr) UpdateTopicNodeInfo(topic string, partition int, topicInfo *TopicPartitionReplicaInfo, oldGen EpochType) error {
	value, err := json.Marshal(topicInfo)
	if err != nil {
		return err
	}
	coordLog.Infof("Update_topic info: %s %d %s %d", topic, partition, string(value), oldGen)
	if oldGen == 0 {
		rsp, err := self.client.Create(self.createTopicReplicaInfoPath(topic, partition), string(value), 0)
		if err != nil {
			return err
		}
		topicInfo.Epoch = EpochType(rsp.Node.ModifiedIndex)
		return nil
	}
	rsp, err := self.client.CompareAndSwap(self.createTopicReplicaInfoPath(topic, partition), string(value), 0, "", uint64(oldGen))
	if err != nil {
		return err
	}
	topicInfo.Epoch = EpochType(rsp.Node.ModifiedIndex)
	return nil
}

func (self *NsqLookupdEtcdMgr) GetTopicLeaderSession(topic string, partition int) (*TopicLeaderSession, error) {
	rsp, err := self.client.Get(self.createTopicLeaderSessionPath(topic, partition), false, false)
	if err != nil {
		if client.IsKeyNotFound(err) {
			return nil, ErrLeaderSessionNotExist
		}
		return nil, err
	}
	var topicLeaderSession TopicLeaderSession
	if err = json.Unmarshal([]byte(rsp.Node.Value), &topicLeaderSession); err != nil {
		return nil, err
	}

	return &topicLeaderSession, nil
}

func (self *NsqLookupdEtcdMgr) ReleaseTopicLeader(topic string, partition int, session *TopicLeaderSession) error {
	topicKey := self.createTopicLeaderSessionPath(topic, partition)
	valueB, err := json.Marshal(session)
	if err != nil {
		return err
	}

	_, err = self.client.CompareAndDelete(topicKey, string(valueB), 0)
	if err != nil {
		coordLog.Errorf("try release topic leader session [%s] error: %v", topicKey, err)
	} else {
		coordLog.Infof("try release topic leader session [%s] success: %v", topicKey, session)
	}
	return err
}

// maybe use: go WatchTopicLeader()...
func (self *NsqLookupdEtcdMgr) WatchTopicLeader(leader chan *TopicLeaderSession, stop chan struct{}) error {
	// start watch goroutine
	self.wtliMutex.Lock()
	for _, v := range self.watchTopicLeaderChanMap {
		go self.watchTopicLeaderSession(v, leader)
	}
	self.wtliMutex.Unlock()

	for {
		select {
		case event := <-self.watchTopicLeaderEventChan:
			topicLeaderSession := self.createTopicLeaderSessionPath(event.topic, event.partition)
			if event.event == EVENT_WATCH_TOPIC_L_CREATE {
				// add to watch topic leader map
				self.wtliMutex.Lock()
				self.watchTopicLeaderChanMap[topicLeaderSession] = event
				self.wtliMutex.Unlock()
				coordLog.Infof("create topic[%s] partition[%d] and start watch.", event.topic, event.partition)
				go self.watchTopicLeaderSession(event, leader)
			}
		case <-stop:
			self.wtliMutex.Lock()
			for _, v := range self.watchTopicLeaderChanMap {
				v.watchStopCh <- true
				<-v.stoppedCh
			}
			self.wtliMutex.Unlock()
			close(leader)
			return nil
		case <-self.watchTopicLeaderStopCh:
			self.wtliMutex.Lock()
			for _, v := range self.watchTopicLeaderChanMap {
				v.watchStopCh <- true
				<-v.stoppedCh
			}
			self.wtliMutex.Unlock()
			close(leader)
			return nil
		}
	}

	return nil
}

func (self *NsqLookupdEtcdMgr) watchTopicLeaderSession(watchTopicLeaderInfo *WatchTopicLeaderInfo, leader chan *TopicLeaderSession) {
	topicLeaderSessionPath := self.createTopicLeaderSessionPath(watchTopicLeaderInfo.topic, watchTopicLeaderInfo.partition)
	watcher := self.client.Watch(topicLeaderSessionPath, 0, true)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-watchTopicLeaderInfo.watchStopCh:
			cancel()
		}
	}()
	for {
		rsp, err := watcher.Next(ctx)
		if err != nil {
			if err == context.Canceled {
				coordLog.Infof("watch key[%s] canceled.", topicLeaderSessionPath)
				watchTopicLeaderInfo.stoppedCh <- true
				return
			} else {
				coordLog.Errorf("watcher key[%s] error: %s", topicLeaderSessionPath, err.Error())
				//rewatch
				if etcdlock.IsEtcdWatchExpired(err) {
					rsp, err = self.client.Get(topicLeaderSessionPath, false, true)
					if err != nil {
						coordLog.Errorf("rewatch and get key[%s] error: %s", topicLeaderSessionPath, err.Error())
						continue
					}
					watcher = self.client.Watch(topicLeaderSessionPath, rsp.Index+1, true)
					// watch changed since the expired event happened
				} else {
					time.Sleep(5 * time.Second)
					continue
				}
			}
		}
		if rsp == nil || rsp.Node == nil {
			continue
		}
		if rsp.PrevNode == nil {
			coordLog.Infof("watch key[%s] action[%s] value[%s] modified[%d]", rsp.Node.Key, rsp.Action, rsp.Node.Value, rsp.Node.ModifiedIndex)
		} else {
			coordLog.Debugf("watch key[%s] action[%s] value[%s] pre_modified[%d] modified[%d]", rsp.Node.Key, rsp.Action, rsp.Node.Value, rsp.PrevNode.ModifiedIndex, rsp.Node.ModifiedIndex)
		}
		if rsp.Action == "compareAndDelete" || rsp.Action == "delete" || rsp.Action == "expire" {
			keys := strings.Split(rsp.Node.Key, "/")
			keyLen := len(keys)
			if keyLen < 3 {
				continue
			}
			partition, err := strconv.Atoi(keys[keyLen-2])
			if err != nil {
				continue
			}
			coordLog.Infof("topic[%s] partition[%d] action[%s] leader deleted.", keys[keyLen-3], partition, rsp.Action)
			topicLeaderSession := &TopicLeaderSession{
				Topic:     keys[keyLen-3],
				Partition: partition,
			}
			leader <- topicLeaderSession
		} else if rsp.Action == "create" {
			var topicLeaderSession TopicLeaderSession
			if err := json.Unmarshal([]byte(rsp.Node.Value), &topicLeaderSession); err != nil {
				continue
			}
			coordLog.Infof("topicLeaderSession[%v] create.", topicLeaderSession)
			leader <- &topicLeaderSession
		}
	}
}

func (self *NsqLookupdEtcdMgr) createClusterPath() string {
	return path.Join("/", NSQ_ROOT_DIR, self.clusterID)
}

func (self *NsqLookupdEtcdMgr) createLookupdPath(value *NsqLookupdNodeInfo) string {
	return path.Join("/", NSQ_ROOT_DIR, self.clusterID, NSQ_LOOKUPD_DIR, NSQ_LOOKUPD_NODE_DIR, "Node-"+value.ID)
}

func (self *NsqLookupdEtcdMgr) createLookupdRootPath() string {
	return path.Join("/", NSQ_ROOT_DIR, self.clusterID, NSQ_LOOKUPD_DIR, NSQ_LOOKUPD_NODE_DIR)
}

func (self *NsqLookupdEtcdMgr) createLookupdLeaderPath() string {
	return path.Join("/", NSQ_ROOT_DIR, self.clusterID, NSQ_LOOKUPD_DIR, NSQ_LOOKUPD_LEADER_SESSION)
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

func (self *NsqLookupdEtcdMgr) createTopicReplicaInfoPath(topic string, partition int) string {
	return path.Join(self.topicRoot, topic, strconv.Itoa(partition), NSQ_TOPIC_REPLICA_INFO)
}

func (self *NsqLookupdEtcdMgr) createTopicLeaderSessionPath(topic string, partition int) string {
	return path.Join(self.topicRoot, topic, strconv.Itoa(partition), NSQ_TOPIC_LEADER_SESSION)
}
