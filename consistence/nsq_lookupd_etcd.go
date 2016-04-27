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

	"github.com/coreos/go-etcd/etcd"
	etcdlock "github.com/reechou/xlock"
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
	sync.Mutex

	client            *etcd.Client
	clusterID         string
	topicRoot         string
	clusterPath       string
	leaderSessionPath string
	leaderStr         string
	lookupdRootPath   string
	topicMetaInfos    []TopicPartionMetaInfo
	topicMetaMap      map[string]TopicMetaInfo
	ifTopicChanged    bool
	nodeInfo          *NsqLookupdNodeInfo

	watchTopicLeaderChanMap   map[string]*WatchTopicLeaderInfo
	watchTopicLeaderEventChan chan *WatchTopicLeaderInfo

	watchTopicLeaderStopCh chan bool
	watchTopicsStopCh      chan bool
	watchNsqdNodesStopCh   chan bool
}

func NewNsqLookupdEtcdMgr(host string) *NsqLookupdEtcdMgr {
	client := NewEtcdClient(host)
	return &NsqLookupdEtcdMgr{
		client:                    client,
		ifTopicChanged:            true,
		watchTopicLeaderStopCh:    make(chan bool, 1),
		watchTopicsStopCh:         make(chan bool, 1),
		watchNsqdNodesStopCh:      make(chan bool, 1),
		topicMetaMap:              make(map[string]TopicMetaInfo),
		watchTopicLeaderChanMap:   make(map[string]*WatchTopicLeaderInfo),
		watchTopicLeaderEventChan: make(chan *WatchTopicLeaderInfo, 1),
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

func (self *NsqLookupdEtcdMgr) GetClusterEpoch() (EpochType, error) {
	rsp, err := self.client.Get(self.clusterPath, false, false)
	if err != nil {
		return 0, err
	}
	return EpochType(rsp.Node.ModifiedIndex), nil
}

func (self *NsqLookupdEtcdMgr) GetAllLookupdNodes() ([]NsqLookupdNodeInfo, error) {
	rsp, err := self.client.Get(self.lookupdRootPath, false, false)
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
				leader <- &lookupdNode
			} else if e.Type == etcdlock.MASTER_DELETE {
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

func (self *NsqLookupdEtcdMgr) WatchNsqdNodes(nsqds chan []NsqdNodeInfo, stop chan struct{}) {
	watchCh := make(chan *etcd.Response, 1)
	watchFailCh := make(chan bool, 1)
	watchStopCh := make(chan bool, 1)

	go self.watch(self.createNsqdRootPath(), watchCh, watchStopCh, watchFailCh)

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
			go self.watch(self.createNsqdRootPath(), watchCh, watchStopCh, watchFailCh)
		case <-stop:
			close(watchStopCh)
			close(nsqds)
			return
		case <-self.watchNsqdNodesStopCh:
			close(watchStopCh)
			return
		}
	}
}

func (self *NsqLookupdEtcdMgr) getNsqdNodes() ([]NsqdNodeInfo, error) {
	rsp, err := self.client.Get(self.createNsqdRootPath(), false, false)
	if err != nil {
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

func (self *NsqLookupdEtcdMgr) ScanTopics() ([]TopicPartionMetaInfo, error) {
	if self.ifTopicChanged {
		return self.scanTopics()
	}
	return self.topicMetaInfos, nil
}

// watch topics if changed
func (self *NsqLookupdEtcdMgr) watchTopics() {
	watchCh := make(chan *etcd.Response, 1)
	watchFailCh := make(chan bool, 1)
	watchStopCh := make(chan bool, 1)

	go self.watch(self.topicRoot, watchCh, watchStopCh, watchFailCh)

	for {
		select {
		case <-watchCh:
			self.ifTopicChanged = true
		case <-watchFailCh:
			watchCh = make(chan *etcd.Response, 1)
			go self.watch(self.createTopicRootPath(), watchCh, watchStopCh, watchFailCh)
		case <-self.watchTopicsStopCh:
			close(watchStopCh)
			return
		}
	}
}

func (self *NsqLookupdEtcdMgr) scanTopics() ([]TopicPartionMetaInfo, error) {
	rsp, err := self.client.Get(self.createTopicRootPath(), false, true)
	if err != nil {
		return nil, err
	}

	self.topicMetaInfos = make([]TopicPartionMetaInfo, 0)
	for _, node := range rsp.Node.Nodes {
		if node.Dir {
			continue
		}
		_, key := path.Split(node.Key)
		if key == NSQ_TOPIC_INFO {
			var topicInfo TopicPartionMetaInfo
			if err = json.Unmarshal([]byte(node.Value), &topicInfo); err != nil {
				continue
			}
			topicInfo.Epoch = EpochType(rsp.Node.ModifiedIndex)
			self.topicMetaInfos = append(self.topicMetaInfos, topicInfo)
		}
	}
	self.ifTopicChanged = false

	return self.topicMetaInfos, nil
}

func (self *NsqLookupdEtcdMgr) GetTopicInfo(topic string, partition int) (*TopicPartionMetaInfo, error) {
	rsp, err := self.client.Get(self.createTopicInfoPath(topic, partition), false, false)
	if err != nil {
		return nil, err
	}
	var topicInfo TopicPartionMetaInfo
	if err = json.Unmarshal([]byte(rsp.Node.Value), &topicInfo); err != nil {
		return nil, err
	}
	return &topicInfo, nil
}

func (self *NsqLookupdEtcdMgr) CreateTopicPartition(topic string, partition int) error {
	_, err := self.client.CreateDir(self.createTopicPartitionPath(topic, partition), 0)
	if err != nil {
		return err
	}
	// if replica == 1, no need watch leader session
	v, ok := self.topicMetaMap[topic]
	if ok {
		if v.Replica == 1 {
			return nil
		}
	}
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

func (self *NsqLookupdEtcdMgr) CreateTopic(topic string, partitionNum int, replica int) error {
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
	self.Lock()
	defer self.Unlock()
	self.topicMetaMap[topic] = *metaInfo
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

func (self *NsqLookupdEtcdMgr) CreateTopicNodeInfo(topic string, partition int, topicInfo *TopicPartionMetaInfo) (error, EpochType) {
	value, err := json.Marshal(topicInfo)
	if err != nil {
		return err, 0
	}
	rsp, err := self.client.Create(self.createTopicInfoPath(topic, partition), string(value), 0)
	if err != nil {
		return err, 0
	}
	return nil, EpochType(rsp.Node.ModifiedIndex)
}

func (self *NsqLookupdEtcdMgr) UpdateTopicNodeInfo(topic string, partition int, topicInfo *TopicPartionMetaInfo, oldGen EpochType) error {
	value, err := json.Marshal(topicInfo)
	if err != nil {
		return err
	}
	_, err = self.client.CompareAndSwap(self.createTopicInfoPath(topic, partition), string(value), 0, "", uint64(oldGen))
	if err != nil {
		return err
	}
	return nil
}

func (self *NsqLookupdEtcdMgr) GetTopicLeaderSession(topic string, partition int) (*TopicLeaderSession, error) {
	rsp, err := self.client.Get(self.createTopicLeaderSessionPath(topic, partition), false, false)
	if err != nil {
		return nil, err
	}
	var topicLeaderSession TopicLeaderSession
	if err = json.Unmarshal([]byte(rsp.Node.Value), &topicLeaderSession); err != nil {
		return nil, err
	}
	return &topicLeaderSession, nil
}

// maybe use: go WatchTopicLeader()...
func (self *NsqLookupdEtcdMgr) WatchTopicLeader(leader chan *TopicLeaderSession, stop chan struct{}) error {
	// start watch goroutine
	for _, v := range self.watchTopicLeaderChanMap {
		go self.watchTopicLeaderSession(v, leader)
	}

	for {
		select {
		case event := <-self.watchTopicLeaderEventChan:
			topicLeaderSession := self.createTopicLeaderSessionPath(event.topic, event.partition)
			if event.event == EVENT_WATCH_TOPIC_L_CREATE {
				// add to watch topic leader map
				self.Lock()
				self.watchTopicLeaderChanMap[topicLeaderSession] = event
				self.Unlock()
				go self.watchTopicLeaderSession(event, leader)
			} else if event.event == EVENT_WATCH_TOPIC_L_DELETE {
				// del from watch topic leader map
				event.watchStopCh <- true
				<-event.stoppedCh
				self.Lock()
				delete(self.watchTopicLeaderChanMap, topicLeaderSession)
				self.Unlock()
			}
		case <-stop:
			for _, v := range self.watchTopicLeaderChanMap {
				v.watchStopCh <- true
				<-v.stoppedCh
			}
			close(leader)
			return nil
		case <-self.watchTopicLeaderStopCh:
			for _, v := range self.watchTopicLeaderChanMap {
				v.watchStopCh <- true
				<-v.stoppedCh
			}
			return nil
		}
	}

	return nil
}

func (self *NsqLookupdEtcdMgr) watchTopicLeaderSession(watchTopicLeaderInfo *WatchTopicLeaderInfo, leader chan *TopicLeaderSession) {
	watchCh := make(chan *etcd.Response, 1)
	processStopCh := make(chan bool, 1)

	topicLeaderSessionPath := self.createTopicLeaderSessionPath(watchTopicLeaderInfo.topic, watchTopicLeaderInfo.partition)

	go func() {
		for {
			select {
			case rsp := <-watchCh:
				if rsp == nil {
					continue
				}
				if rsp.Action == "expire" || rsp.Action == "delete" {
					keys := strings.Split(rsp.Node.Key, "/")
					keyLen := len(keys)
					if keyLen < 3 {
						continue
					}
					partition, err := strconv.Atoi(keys[keyLen-2])
					if err != nil {
						continue
					}
					topicLeaderSession := &TopicLeaderSession{
						Topic:     keys[keyLen-3],
						Partition: partition,
					}
					leader <- topicLeaderSession
				} else {
					var topicLeaderSession TopicLeaderSession
					if err := json.Unmarshal([]byte(rsp.Node.Value), &topicLeaderSession); err != nil {
						continue
					}
					leader <- &topicLeaderSession
				}
			case <-processStopCh:
				return
			}
		}
	}()

	for {
		_, err := self.client.Watch(topicLeaderSessionPath, 0, true, watchCh, watchTopicLeaderInfo.watchStopCh)
		if err == etcd.ErrWatchStoppedByUser {
			// stop process goroutine
			close(processStopCh)
			watchTopicLeaderInfo.stoppedCh <- true
			return
		} else {
			time.Sleep(time.Second)
			watchCh = make(chan *etcd.Response, 1)
		}
	}
}

func (self *NsqLookupdEtcdMgr) stopWatchTopicLeaderSession(watchTopicLeaderInfo *WatchTopicLeaderInfo) {
	topicLeaderSession := self.createTopicLeaderSessionPath(watchTopicLeaderInfo.topic, watchTopicLeaderInfo.partition)
	v, ok := self.watchTopicLeaderChanMap[topicLeaderSession]
	if ok {
		v.watchStopCh <- true
		<-v.stoppedCh
	}
}

func (self *NsqLookupdEtcdMgr) watch(key string, watchCh chan *etcd.Response, watchStopCh chan bool, watchFailCh chan bool) {
	_, err := self.client.Watch(key, 0, true, watchCh, watchStopCh)
	if err == etcd.ErrWatchStoppedByUser {
		return
	} else {
		watchFailCh <- true
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

func (self *NsqLookupdEtcdMgr) createTopicInfoPath(topic string, partition int) string {
	return path.Join(self.topicRoot, topic, strconv.Itoa(partition), NSQ_TOPIC_INFO)
}

func (self *NsqLookupdEtcdMgr) createTopicLeaderSessionPath(topic string, partition int) string {
	return path.Join(self.topicRoot, topic, strconv.Itoa(partition), NSQ_TOPIC_LEADER_SESSION)
}
