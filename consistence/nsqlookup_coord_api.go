package consistence

import (
	"errors"
	"github.com/absolute8511/nsq/internal/protocol"
	"strconv"
	"sync/atomic"
	"time"
)

const (
	MAX_PARTITION_NUM  = 255
	MAX_SYNC_EVERY     = 5000
	MAX_RETENTION_DAYS = 60
)

func (self *NsqLookupCoordinator) GetAllLookupdNodes() ([]NsqLookupdNodeInfo, error) {
	return self.leadership.GetAllLookupdNodes()
}

func (self *NsqLookupCoordinator) GetLookupLeader() NsqLookupdNodeInfo {
	return self.leaderNode
}

func (self *NsqLookupCoordinator) GetTopicMetaInfo(topicName string) (*TopicMetaInfo, error) {
	meta, _, err := self.leadership.GetTopicMetaInfo(topicName)
	return &meta, err
}

func (self *NsqLookupCoordinator) GetTopicLeaderNodes(topicName string) (map[string]string, error) {
	meta, _, err := self.leadership.GetTopicMetaInfo(topicName)
	if err != nil {
		coordLog.Infof("failed to get topic %v meta: %v", topicName, err)
		return nil, err
	}
	ret := make(map[string]string)
	var anyErr error
	for i := 0; i < meta.PartitionNum; i++ {
		info, err := self.leadership.GetTopicInfo(topicName, i)
		if err != nil {
			anyErr = err
			continue
		}
		if len(info.ISR) > info.Replica/2 && !self.isTopicWriteDisabled(info) {
			ret[strconv.Itoa(info.Partition)] = info.Leader
		}
	}
	if len(ret) == 0 {
		return ret, anyErr
	}
	return ret, nil
}

func (self *NsqLookupCoordinator) IsMineLeader() bool {
	return self.leaderNode.GetID() == self.myNode.GetID()
}

func (self *NsqLookupCoordinator) IsClusterStable() bool {
	return atomic.LoadInt32(&self.isClusterUnstable) == 0 &&
		atomic.LoadInt32(&self.isUpgrading) == 0
}

func (self *NsqLookupCoordinator) SetClusterUpgradeState(upgrading bool) error {
	if self.leaderNode.GetID() != self.myNode.GetID() {
		coordLog.Infof("not leader while delete topic")
		return ErrNotNsqLookupLeader
	}

	if upgrading {
		if !atomic.CompareAndSwapInt32(&self.isUpgrading, 0, 1) {
			coordLog.Infof("the cluster state is already upgrading")
			return nil
		}
		coordLog.Infof("the cluster state has been changed to upgrading")
	} else {
		if !atomic.CompareAndSwapInt32(&self.isUpgrading, 1, 0) {
			return nil
		}
		coordLog.Infof("the cluster state has been changed to normal")
		topics, err := self.leadership.ScanTopics()
		if err != nil {
			coordLog.Infof("failed to scan topics: %v", err)
			return err
		}
		for _, topicInfo := range topics {
			retry := 0
			for retry < 3 {
				retry++
				leaderSession, err := self.leadership.GetTopicLeaderSession(topicInfo.Name, topicInfo.Partition)
				if err != nil {
					coordLog.Infof("failed to get topic %v leader session: %v", topicInfo.GetTopicDesp(), err)
					self.notifyISRTopicMetaInfo(&topicInfo)
					self.notifyAcquireTopicLeader(&topicInfo)
					time.Sleep(time.Millisecond * 100)
				} else {
					self.notifyTopicLeaderSession(&topicInfo, leaderSession, "")
					break
				}
			}
		}
		self.triggerCheckTopics("", 0, time.Second)
	}
	return nil
}

func (self *NsqLookupCoordinator) MoveTopicPartitionDataByManual(topicName string,
	partitionID int, moveLeader bool, fromNode string, toNode string) error {
	coordLog.Infof("try move topic %v-%v from node %v to %v", topicName, partitionID, fromNode, toNode)
	err := self.dpm.moveTopicPartitionByManual(topicName, partitionID, moveLeader, fromNode, toNode)
	if err != nil {
		coordLog.Infof("failed to move the topic partition: %v", err)
	}
	return err
}

func (self *NsqLookupCoordinator) GetClusterNodeLoadFactor() (map[string]float64, map[string]float64) {
	currentNodes := self.getCurrentNodes()
	leaderFactors := make(map[string]float64, len(currentNodes))
	nodeFactors := make(map[string]float64, len(currentNodes))
	for nodeID, nodeInfo := range currentNodes {
		topicStat, err := self.getNsqdTopicStat(nodeInfo)
		if err != nil {
			coordLog.Infof("failed to get node topic status : %v", nodeID)
			continue
		}
		leaderLF, nodeLF := topicStat.GetNodeLoadFactor()
		leaderFactors[nodeID] = leaderLF
		nodeFactors[nodeID] = nodeLF
	}
	return leaderFactors, nodeFactors
}

func (self *NsqLookupCoordinator) IsTopicLeader(topic string, part int, nid string) bool {
	t, err := self.leadership.GetTopicInfo(topic, part)
	if err != nil {
		return false
	}
	return t.Leader == nid
}

func (self *NsqLookupCoordinator) MarkNodeAsRemoving(nid string) error {
	if self.leaderNode.GetID() != self.myNode.GetID() {
		coordLog.Infof("not leader while delete topic")
		return ErrNotNsqLookupLeader
	}

	coordLog.Infof("try mark node %v as removed", nid)
	self.nodesMutex.Lock()
	newRemovingNodes := make(map[string]string)
	if _, ok := self.removingNodes[nid]; ok {
		coordLog.Infof("already mark as removing")
	} else {
		newRemovingNodes[nid] = "marked"
		for id, removeState := range self.removingNodes {
			newRemovingNodes[id] = removeState
		}
		self.removingNodes = newRemovingNodes
	}
	self.nodesMutex.Unlock()
	return nil
}

func (self *NsqLookupCoordinator) DeleteTopicForce(topic string, partition string) error {
	if self.leaderNode.GetID() != self.myNode.GetID() {
		coordLog.Infof("not leader while delete topic")
		return ErrNotNsqLookupLeader
	}

	coordLog.Infof("delete topic: %v, with partition: %v", topic, partition)

	if partition == "**" {
		self.joinStateMutex.Lock()
		state, ok := self.joinISRState[topic]
		self.joinStateMutex.Unlock()
		if ok {
			state.Lock()
			if state.waitingJoin {
				state.waitingJoin = false
				state.waitingSession = ""
				if state.doneChan != nil {
					close(state.doneChan)
					state.doneChan = nil
				}
			}
			state.Unlock()
		}
		// delete all
		for pid := 0; pid < MAX_PARTITION_NUM; pid++ {
			self.deleteTopicPartitionForce(topic, pid)
		}
		self.leadership.DeleteWholeTopic(topic)
	} else {
		pid, err := strconv.Atoi(partition)
		if err != nil {
			coordLog.Infof("failed to parse the partition id : %v, %v", partition, err)
			return err
		}
		self.deleteTopicPartitionForce(topic, pid)
	}
	return nil
}

func (self *NsqLookupCoordinator) DeleteTopic(topic string, partition string) error {
	if self.leaderNode.GetID() != self.myNode.GetID() {
		coordLog.Infof("not leader while delete topic")
		return ErrNotNsqLookupLeader
	}

	coordLog.Infof("delete topic: %v, with partition: %v", topic, partition)
	if ok, err := self.leadership.IsExistTopic(topic); !ok {
		coordLog.Infof("no topic : %v", err)
		return errors.New("Topic not exist")
	}

	if partition == "**" {
		// delete all
		meta, _, err := self.leadership.GetTopicMetaInfo(topic)
		if err != nil {
			coordLog.Infof("failed to get meta for topic: %v", err)
			meta.PartitionNum = MAX_PARTITION_NUM
		}
		self.joinStateMutex.Lock()
		state, ok := self.joinISRState[topic]
		self.joinStateMutex.Unlock()
		if ok {
			state.Lock()
			if state.waitingJoin {
				state.waitingJoin = false
				state.waitingSession = ""
				if state.doneChan != nil {
					close(state.doneChan)
					state.doneChan = nil
				}
			}
			state.Unlock()
		}

		for pid := 0; pid < meta.PartitionNum; pid++ {
			err := self.deleteTopicPartition(topic, pid)
			if err != nil {
				coordLog.Infof("failed to delete partition %v for topic: %v, err:%v", pid, topic, err)
			}
		}
		self.leadership.DeleteWholeTopic(topic)
	} else {
		pid, err := strconv.Atoi(partition)
		if err != nil {
			coordLog.Infof("failed to parse the partition id : %v, %v", partition, err)
			return err
		}

		return self.deleteTopicPartition(topic, pid)
	}
	return nil
}

func (self *NsqLookupCoordinator) deleteTopicPartitionForce(topic string, pid int) error {
	self.leadership.DeleteTopic(topic, pid)
	currentNodes := self.getCurrentNodes()
	var topicInfo TopicPartitionMetaInfo
	topicInfo.Name = topic
	topicInfo.Partition = pid
	for _, node := range currentNodes {
		c, rpcErr := self.acquireRpcClient(node.ID)
		if rpcErr != nil {
			coordLog.Infof("failed to get rpc client: %v, %v", node.ID, rpcErr)
			continue
		}
		rpcErr = c.DeleteNsqdTopic(self.leaderNode.Epoch, &topicInfo)
		if rpcErr != nil {
			coordLog.Infof("failed to call rpc : %v, %v", node.ID, rpcErr)
		}
	}
	return nil
}

func (self *NsqLookupCoordinator) deleteTopicPartition(topic string, pid int) error {
	topicInfo, commonErr := self.leadership.GetTopicInfo(topic, pid)
	if commonErr != nil {
		coordLog.Infof("failed to get the topic info while delete topic: %v", commonErr)
		return commonErr
	}
	commonErr = self.leadership.DeleteTopic(topic, pid)
	if commonErr != nil {
		coordLog.Infof("failed to delete the topic info : %v", commonErr)
		return commonErr
	}
	for _, id := range topicInfo.CatchupList {
		c, rpcErr := self.acquireRpcClient(id)
		if rpcErr != nil {
			coordLog.Infof("failed to get rpc client: %v, %v", id, rpcErr)
			continue
		}
		rpcErr = c.DeleteNsqdTopic(self.leaderNode.Epoch, topicInfo)
		if rpcErr != nil {
			coordLog.Infof("failed to call rpc : %v, %v", id, rpcErr)
		}
	}
	for _, id := range topicInfo.ISR {
		c, rpcErr := self.acquireRpcClient(id)
		if rpcErr != nil {
			coordLog.Infof("failed to get rpc client: %v, %v", id, rpcErr)
			continue
		}
		rpcErr = c.DeleteNsqdTopic(self.leaderNode.Epoch, topicInfo)
		if rpcErr != nil {
			coordLog.Infof("failed to call rpc : %v, %v", id, rpcErr)
		}
	}
	// try remove on other nodes, maybe some left data
	allNodes := self.getCurrentNodes()
	for _, n := range allNodes {
		c, rpcErr := self.acquireRpcClient(n.GetID())
		if rpcErr != nil {
			continue
		}
		c.DeleteNsqdTopic(self.leaderNode.Epoch, topicInfo)
	}

	return nil
}

func (self *NsqLookupCoordinator) ChangeTopicMetaParam(topic string,
	newSyncEvery int, newRetentionDay int, newReplicator int) error {
	if self.leaderNode.GetID() != self.myNode.GetID() {
		coordLog.Infof("not leader while create topic")
		return ErrNotNsqLookupLeader
	}

	if !protocol.IsValidTopicName(topic) {
		return errors.New("invalid topic name")
	}

	if newRetentionDay > MAX_RETENTION_DAYS {
		return errors.New("max retention days allowed exceed")
	}
	if newSyncEvery > MAX_SYNC_EVERY {
		return errors.New("max sync every allowed exceed")
	}
	if newReplicator > 5 {
		return errors.New("max replicator allowed exceed")
	}

	self.joinStateMutex.Lock()
	state, ok := self.joinISRState[topic]
	if !ok {
		state = &JoinISRState{}
		self.joinISRState[topic] = state
	}
	self.joinStateMutex.Unlock()
	state.Lock()
	defer state.Unlock()
	if state.waitingJoin {
		coordLog.Warningf("topic state is not ready:%v, %v ", topic, state)
		return ErrWaitingJoinISR.ToErrorType()
	}
	var meta TopicMetaInfo
	if ok, _ := self.leadership.IsExistTopic(topic); !ok {
		coordLog.Infof("topic not exist %v :%v", topic)
		return ErrTopicNotCreated
	} else {
		oldMeta, oldGen, err := self.leadership.GetTopicMetaInfo(topic)
		if err != nil {
			coordLog.Infof("get topic key %v failed :%v", topic, err)
			return err
		}
		currentNodes := self.getCurrentNodes()
		meta = oldMeta
		if newSyncEvery >= 0 {
			meta.SyncEvery = newSyncEvery
		}
		if newRetentionDay >= 0 {
			meta.RetentionDay = int32(newRetentionDay)
		}
		if newReplicator > 0 {
			meta.Replica = newReplicator
		}
		err = self.updateTopicMeta(currentNodes, topic, meta, oldGen)
		if err != nil {
			return err
		}
		for i := 0; i < meta.PartitionNum; i++ {
			topicInfo, err := self.leadership.GetTopicInfo(topic, i)
			if err != nil {
				coordLog.Infof("failed get info for topic : %v-%v, %v", topic, i, err)
				continue
			}
			topicReplicaInfo := &topicInfo.TopicPartitionReplicaInfo
			err = self.leadership.UpdateTopicNodeInfo(topic, i, topicReplicaInfo, topicReplicaInfo.Epoch)
			if err != nil {
				coordLog.Infof("failed update info for topic : %v-%v, %v", topic, i, err)
				continue
			}
			rpcErr := self.notifyTopicMetaInfo(topicInfo)
			if rpcErr != nil {
				coordLog.Warningf("failed notify topic info : %v", rpcErr)
			} else {
				coordLog.Infof("topic %v update successful.", topicInfo)
			}
		}

		self.triggerCheckTopics("", 0, 0)
	}
	return nil
}

func (self *NsqLookupCoordinator) updateTopicMeta(currentNodes map[string]NsqdNodeInfo, topic string, meta TopicMetaInfo, oldGen EpochType) error {
	if meta.SyncEvery > MAX_SYNC_EVERY {
		coordLog.Infof("topic %v sync every with too large %v, set to max", topic, meta)
		meta.SyncEvery = MAX_SYNC_EVERY
	}
	coordLog.Infof("update topic: %v, with meta: %v", topic, meta)

	if len(currentNodes) < meta.Replica || len(currentNodes) < meta.PartitionNum {
		coordLog.Infof("nodes %v is less than replica or partition %v", len(currentNodes), meta)
		return ErrNodeUnavailable.ToErrorType()
	}
	if len(currentNodes) < meta.Replica*meta.PartitionNum {
		coordLog.Infof("nodes is less than replica*partition")
		return ErrNodeUnavailable.ToErrorType()
	}
	return self.leadership.UpdateTopicMetaInfo(topic, &meta, oldGen)
}

func (self *NsqLookupCoordinator) ExpandTopicPartition(topic string, newPartitionNum int) error {
	if self.leaderNode.GetID() != self.myNode.GetID() {
		coordLog.Infof("not leader while create topic")
		return ErrNotNsqLookupLeader
	}

	if !protocol.IsValidTopicName(topic) {
		return errors.New("invalid topic name")
	}

	if newPartitionNum >= MAX_PARTITION_NUM {
		return errors.New("max partition allowed exceed")
	}

	coordLog.Infof("expand topic %v partition number to %v", topic, newPartitionNum)
	if !self.IsClusterStable() {
		return ErrClusterUnstable
	}
	self.joinStateMutex.Lock()
	state, ok := self.joinISRState[topic]
	if !ok {
		state = &JoinISRState{}
		self.joinISRState[topic] = state
	}
	self.joinStateMutex.Unlock()
	state.Lock()
	defer state.Unlock()
	if state.waitingJoin {
		coordLog.Warningf("topic state is not ready:%v, %v ", topic, state)
		return ErrWaitingJoinISR.ToErrorType()
	}
	var meta TopicMetaInfo
	if ok, _ := self.leadership.IsExistTopic(topic); !ok {
		coordLog.Infof("topic not exist %v :%v", topic)
		return ErrTopicNotCreated
	} else {
		oldMeta, oldGen, err := self.leadership.GetTopicMetaInfo(topic)
		if err != nil {
			coordLog.Infof("get topic key %v failed :%v", topic, err)
			return err
		}
		meta = oldMeta
		if newPartitionNum < meta.PartitionNum {
			return errors.New("the partition number can not be reduced")
		}
		currentNodes := self.getCurrentNodes()
		meta.PartitionNum = newPartitionNum
		err = self.updateTopicMeta(currentNodes, topic, meta, oldGen)
		if err != nil {
			coordLog.Infof("update topic %v meta failed :%v", topic, err)
			return err
		}
		return self.checkAndUpdateTopicPartitions(currentNodes, topic, meta)
	}
}

func (self *NsqLookupCoordinator) CreateTopic(topic string, meta TopicMetaInfo) error {
	if self.leaderNode.GetID() != self.myNode.GetID() {
		coordLog.Infof("not leader while create topic")
		return ErrNotNsqLookupLeader
	}

	if !protocol.IsValidTopicName(topic) {
		return errors.New("invalid topic name")
	}

	// TODO: handle default load factor
	if meta.PartitionNum >= MAX_PARTITION_NUM {
		return errors.New("max partition allowed exceed")
	}

	currentNodes := self.getCurrentNodes()
	if len(currentNodes) < meta.Replica || len(currentNodes) < meta.PartitionNum {
		coordLog.Infof("nodes %v is less than replica or partition %v", len(currentNodes), meta)
		return ErrNodeUnavailable.ToErrorType()
	}
	if len(currentNodes) < meta.Replica*meta.PartitionNum {
		coordLog.Infof("nodes is less than replica*partition")
		return ErrNodeUnavailable.ToErrorType()
	}

	self.joinStateMutex.Lock()
	state, ok := self.joinISRState[topic]
	if !ok {
		state = &JoinISRState{}
		self.joinISRState[topic] = state
	}
	self.joinStateMutex.Unlock()
	state.Lock()
	defer state.Unlock()
	if state.waitingJoin {
		coordLog.Warningf("topic state is not ready:%v, %v ", topic, state)
		return ErrWaitingJoinISR.ToErrorType()
	}
	if meta.SyncEvery > MAX_SYNC_EVERY {
		coordLog.Infof("topic %v sync every with too large %v, set to max", topic, meta)
		meta.SyncEvery = MAX_SYNC_EVERY
	}

	if ok, _ := self.leadership.IsExistTopic(topic); !ok {
		meta.MagicCode = time.Now().UnixNano()
		err := self.leadership.CreateTopic(topic, &meta)
		if err != nil {
			coordLog.Infof("create topic key %v failed :%v", topic, err)
			return err
		}
	} else {
		coordLog.Warningf("topic already exist :%v ", topic)
		return ErrAlreadyExist
	}
	coordLog.Infof("create topic: %v, with meta: %v", topic, meta)

	return self.checkAndUpdateTopicPartitions(currentNodes, topic, meta)
}

func (self *NsqLookupCoordinator) checkAndUpdateTopicPartitions(currentNodes map[string]NsqdNodeInfo,
	topic string, meta TopicMetaInfo) error {
	existPart := make(map[int]*TopicPartitionMetaInfo)
	for i := 0; i < meta.PartitionNum; i++ {
		err := self.leadership.CreateTopicPartition(topic, i)
		if err != nil {
			coordLog.Warningf("failed to create topic %v-%v: %v", topic, i, err)
			// handle already exist
			t, err := self.leadership.GetTopicInfo(topic, i)
			if err != nil {
				coordLog.Warningf("exist topic partition failed to get info: %v", err)
				if err != ErrKeyNotFound {
					return err
				}
			} else {
				coordLog.Infof("create topic partition already exist %v-%v", topic, i)
				existPart[i] = t
			}
		}
	}
	leaders, isrList, err := self.dpm.allocTopicLeaderAndISR(currentNodes, meta.Replica, meta.PartitionNum, existPart)
	if err != nil {
		coordLog.Infof("failed to alloc nodes for topic: %v", err)
		return err.ToErrorType()
	}
	if len(leaders) != meta.PartitionNum || len(isrList) != meta.PartitionNum {
		return ErrNodeUnavailable.ToErrorType()
	}
	for i := 0; i < meta.PartitionNum; i++ {
		if _, ok := existPart[i]; ok {
			continue
		}
		var tmpTopicReplicaInfo TopicPartitionReplicaInfo
		tmpTopicReplicaInfo.ISR = isrList[i]
		tmpTopicReplicaInfo.Leader = leaders[i]
		tmpTopicReplicaInfo.EpochForWrite = 1

		commonErr := self.leadership.UpdateTopicNodeInfo(topic, i, &tmpTopicReplicaInfo, tmpTopicReplicaInfo.Epoch)
		if commonErr != nil {
			coordLog.Infof("failed update info for topic : %v-%v, %v", topic, i, commonErr)
			continue
		}
		tmpTopicInfo := TopicPartitionMetaInfo{}
		tmpTopicInfo.Name = topic
		tmpTopicInfo.Partition = i
		tmpTopicInfo.TopicMetaInfo = meta
		tmpTopicInfo.TopicPartitionReplicaInfo = tmpTopicReplicaInfo
		rpcErr := self.notifyTopicMetaInfo(&tmpTopicInfo)
		if rpcErr != nil {
			coordLog.Warningf("failed notify topic info : %v", rpcErr)
		} else {
			coordLog.Infof("topic %v init successful.", tmpTopicInfo)
		}
	}
	self.triggerCheckTopics("", 0, time.Millisecond*500)
	return nil
}
