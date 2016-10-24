package consistence

import (
	"errors"
	"github.com/absolute8511/nsq/internal/protocol"
	"strconv"
	"sync/atomic"
	"time"
)

const (
	MAX_PARTITION_NUM = 255
)

func (self *NsqLookupCoordinator) GetAllLookupdNodes() ([]NsqLookupdNodeInfo, error) {
	return self.leadership.GetAllLookupdNodes()
}

func (self *NsqLookupCoordinator) GetLookupLeader() NsqLookupdNodeInfo {
	return self.leaderNode
}

func (self *NsqLookupCoordinator) GetTopicLeaderNodes(topicName string) map[string]string {
	meta, err := self.leadership.GetTopicMetaInfo(topicName)
	if err != nil {
		coordLog.Infof("failed to get topic %v meta: %v", topicName, err)
		return nil
	}
	ret := make(map[string]string)
	for i := 0; i < meta.PartitionNum; i++ {
		info, err := self.leadership.GetTopicInfo(topicName, i)
		if err != nil {
			continue
		}
		ret[strconv.Itoa(info.Partition)] = info.Leader
	}
	return ret
}

func (self *NsqLookupCoordinator) IsMineLeader() bool {
	return self.leaderNode.GetID() == self.myNode.GetID()
}

func (self *NsqLookupCoordinator) IsClusterStable() bool {
	return atomic.LoadInt32(&self.isClusterUnstable) == 0
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
		meta, err := self.leadership.GetTopicMetaInfo(topic)
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

	if ok, _ := self.leadership.IsExistTopic(topic); !ok {
		meta.MagicCode = time.Now().UnixNano()
		err := self.leadership.CreateTopic(topic, &meta)
		if err != nil {
			coordLog.Infof("create topic key %v failed :%v", topic, err)
			oldMeta, getErr := self.leadership.GetTopicMetaInfo(topic)
			if getErr != nil {
				coordLog.Infof("get topic key %v failed :%v", topic, getErr)
				return err
			}
			if oldMeta != meta {
				coordLog.Infof("topic meta not the same with exist :%v, old: %v", topic, oldMeta)
				return err
			}
		}
	} else {
		coordLog.Warningf("topic already exist :%v ", topic)
		return ErrAlreadyExist
	}
	coordLog.Infof("create topic: %v, with meta: %v", topic, meta)

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
	self.triggerCheckTopics("", 0, 0)
	return nil
}
