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
	"sync"
	"time"

	"github.com/coreos/etcd/client"
	etcdlock "github.com/reechou/xlock2"
	"golang.org/x/net/context"
)

const (
	ETCD_TTL = 15
)

type MasterChanInfo struct {
	processStopCh chan bool
	stoppedCh     chan bool
}

type NsqdEtcdMgr struct {
	sync.Mutex

	client      *etcdlock.EtcdClient
	clusterID   string
	topicRoot   string
	lookupdRoot string

	topicLockMap map[string]*etcdlock.SeizeLock

	nodeKey       string
	nodeValue     string
	refreshStopCh chan bool
}

func SetEtcdLogger(log etcdlock.Logger, level int32) {
	etcdlock.SetLogger(log, int(level))
}

func NewNsqdEtcdMgr(host string) *NsqdEtcdMgr {
	client := etcdlock.NewEClient(host)
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
	self.nodeKey = self.createNsqdNodePath(nodeData)
	self.nodeValue = string(value)
	_, err = self.client.Set(self.nodeKey, self.nodeValue, ETCD_TTL)
	if err != nil {
		return err
	}
	if self.refreshStopCh != nil {
		close(self.refreshStopCh)
	}
	self.refreshStopCh = make(chan bool, 1)
	// start refresh node
	go self.refresh(self.refreshStopCh)

	return nil
}

func (self *NsqdEtcdMgr) refresh(stopChan chan bool) {
	for {
		select {
		case <-stopChan:
			return
		case <-time.After(time.Second * time.Duration(ETCD_TTL*4/10)):
			_, err := self.client.SetWithTTL(self.nodeKey, ETCD_TTL)
			//_, err := self.client.Set(self.nodeKey, self.nodeValue, ETCD_TTL)
			if err != nil {
				coordLog.Errorf("[NsqdEtcdMgr][refresh] update error: %s", err.Error())
			}
		}
	}
}

func (self *NsqdEtcdMgr) UnregisterNsqd(nodeData *NsqdNodeInfo) error {
	self.Lock()
	defer self.Unlock()

	// clear
	for k, v := range self.topicLockMap {
		v.Unlock()
		delete(self.topicLockMap, k)
	}

	_, err := self.client.Delete(self.createNsqdNodePath(nodeData), false)
	if err != nil {
		return err
	}
	// stop refresh
	if self.refreshStopCh != nil {
		close(self.refreshStopCh)
		self.refreshStopCh = nil
	}

	coordLog.Infof("[UnregisterNsqd] cluser[%s] node[%s]", self.clusterID, nodeData.ID)

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
		coordLog.Errorf("[AcquireTopicLeader] topic_key[%s] lock error: %s", topicKey, err.Error())
		if err == etcdlock.ErrSeizeLockAg {
			return nil
		}
		return err
	}
	coordLog.Infof("[AcquireTopicLeader] topic_key[%s] lock success.", topicKey)

	self.Lock()
	self.topicLockMap[topicKey] = lock
	coordLog.Debugf("[AcquireTopicLeader] map: %v", self.topicLockMap)
	self.Unlock()

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
	self.Lock()
	defer self.Unlock()

	coordLog.Infof("[ReleaseTopicLeader] topic[%s] partition[%d] leader", topic, partition)
	coordLog.Infof("[ReleaseTopicLeader] map: %v", self.topicLockMap)
	topicKey := self.createTopicLeaderPath(topic, partition)
	v, ok := self.topicLockMap[topicKey]
	if ok {
		err := v.Unlock()
		if err != nil {
			coordLog.Errorf("[ReleaseTopicLeader] unlock error: %s", err.Error())
			if err == etcdlock.ErrEtcdBad {
				return err
			}
		}
		delete(self.topicLockMap, topicKey)
		coordLog.Infof("[ReleaseTopicLeader] topic[%s] partition[%d] success.", topic, partition)

		return err
	} else {
		coordLog.Errorf("[ReleaseTopicLeader] topicLockMap key[%s] not found.", topicKey)
		return fmt.Errorf("topicLockMap key[%s] not found.", topicKey)
	}

	return nil
}

func (self *NsqdEtcdMgr) GetAllLookupdNodes() ([]NsqLookupdNodeInfo, error) {
	rsp, err := self.client.Get(self.lookupdRoot, false, false)
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

func (self *NsqdEtcdMgr) WatchLookupdLeader(leader chan *NsqLookupdNodeInfo, stop chan struct{}) error {
	key := self.createLookupdLeaderPath()

	rsp, err := self.client.Get(key, false, false)
	if err == nil {
		coordLog.Infof("[WatchLookupdLeader] key: %s value: %s", rsp.Node.Key, rsp.Node.Value)
		var lookupdInfo NsqLookupdNodeInfo
		err = json.Unmarshal([]byte(rsp.Node.Value), &lookupdInfo)
		if err == nil {
			select {
			case leader <- &lookupdInfo:
			case <-stop:
				close(leader)
				return nil
			}
		}
	} else {
		coordLog.Errorf("[WatchLookupdLeader] get error: %s", err.Error())
	}

	watcher := self.client.Watch(key, 0, true)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-stop:
			cancel()
		}
	}()
	for {
		rsp, err = watcher.Next(ctx)
		if err != nil {
			if err == context.Canceled {
				coordLog.Infof("watch key[%s] canceled.", key)
				close(leader)
				return nil
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
					continue
				} else {
					time.Sleep(5 * time.Second)
				}
			}
			continue
		}
		if rsp == nil {
			continue
		}
		var lookupdInfo NsqLookupdNodeInfo
		if rsp.Action == "expire" || rsp.Action == "delete" {
			coordLog.Infof("key[%s] action[%s]", key, rsp.Action)
		} else if rsp.Action == "create" || rsp.Action == "update" || rsp.Action == "set" {
			err := json.Unmarshal([]byte(rsp.Node.Value), &lookupdInfo)
			if err != nil {
				continue
			}
		} else {
			continue
		}
		select {
		case leader <- &lookupdInfo:
		case <-stop:
			close(leader)
			return nil
		}
	}

	return nil
}

func (self *NsqdEtcdMgr) GetTopicInfo(topic string, partition int) (*TopicPartitionMetaInfo, error) {
	var topicInfo TopicPartitionMetaInfo
	rsp, err := self.client.Get(self.createTopicMetaPath(topic), false, false)
	if err != nil {
		if client.IsKeyNotFound(err) {
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

	rsp, err = self.client.Get(self.createTopicReplicaInfoPath(topic, partition), false, false)
	if err != nil {
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

func (self *NsqdEtcdMgr) GetTopicLeaderSession(topic string, partition int) (*TopicLeaderSession, error) {
	rsp, err := self.client.Get(self.createTopicLeaderPath(topic, partition), false, false)
	if err != nil {
		if client.IsKeyNotFound(err) {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}
	var topicLeaderSession TopicLeaderSession
	if err = json.Unmarshal([]byte(rsp.Node.Value), &topicLeaderSession); err != nil {
		return nil, err
	}
	return &topicLeaderSession, nil
}

func (self *NsqdEtcdMgr) createNsqdNodePath(nodeData *NsqdNodeInfo) string {
	return path.Join("/", NSQ_ROOT_DIR, self.clusterID, NSQ_NODE_DIR, "Node-"+nodeData.ID)
}

func (self *NsqdEtcdMgr) createTopicRootPath() string {
	return path.Join("/", NSQ_ROOT_DIR, self.clusterID, NSQ_TOPIC_DIR)
}

func (self *NsqdEtcdMgr) createTopicMetaPath(topic string) string {
	return path.Join(self.topicRoot, topic, NSQ_TOPIC_META)
}

func (self *NsqdEtcdMgr) createTopicReplicaInfoPath(topic string, partition int) string {
	return path.Join(self.topicRoot, topic, strconv.Itoa(partition), NSQ_TOPIC_REPLICA_INFO)
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

func (self *NsqdEtcdMgr) createTopicLeaderPath(topic string, partition int) string {
	return path.Join(self.topicRoot, topic, strconv.Itoa(partition), NSQ_TOPIC_LEADER_SESSION)
}
