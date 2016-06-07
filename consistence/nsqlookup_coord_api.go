package consistence

import (
	"errors"
	"strconv"
	"time"
)

func (self *NsqLookupCoordinator) GetAllLookupdNodes() ([]NsqLookupdNodeInfo, error) {
	return self.leadership.GetAllLookupdNodes()
}

func (self *NsqLookupCoordinator) GetLookupLeader() NsqLookupdNodeInfo {
	return self.leaderNode
}

func (self *NsqLookupCoordinator) IsMineLeader() bool {
	return self.leaderNode.GetID() == self.myNode.GetID()
}

func (self *NsqLookupCoordinator) IsTopicLeader(topic string, part int, nid string) bool {
	t, err := self.leadership.GetTopicInfo(topic, part)
	if err != nil {
		return false
	}
	return t.Leader == nid
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
			return err
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

	return nil
}

func (self *NsqLookupCoordinator) CreateTopic(topic string, meta TopicMetaInfo) error {
	if self.leaderNode.GetID() != self.myNode.GetID() {
		coordLog.Infof("not leader while create topic")
		return ErrNotNsqLookupLeader
	}

	// TODO: handle default load factor
	coordLog.Infof("create topic: %v, with meta: %v", topic, meta)

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
		coordLog.Infof("topic state is not ready:%v, %v ", topic, state)
		return ErrWaitingJoinISR
	}

	existPart := make(map[int]*TopicPartitionMetaInfo)
	for i := 0; i < meta.PartitionNum; i++ {
		err := self.leadership.CreateTopicPartition(topic, i)
		if err != nil {
			coordLog.Warningf("failed to create topic %v-%v: %v", topic, i, err)
			// handle already exist
			t, err := self.leadership.GetTopicInfo(topic, i)
			if err != nil {
				coordLog.Warningf("exist topic partition failed to get info: %v", err)
			} else {
				coordLog.Infof("create topic partition already exist %v-%v: %v", topic, i, err.Error())
				existPart[i] = t
			}
		}
	}
	self.nodesMutex.RLock()
	currentNodes := self.nsqdNodes
	self.nodesMutex.RUnlock()
	leaders, isrList, err := self.allocTopicLeaderAndISR(currentNodes, meta.Replica, meta.PartitionNum, existPart)
	if err != nil {
		coordLog.Infof("failed to alloc nodes for topic: %v", err)
		return err
	}
	if len(leaders) != meta.PartitionNum || len(isrList) != meta.PartitionNum {
		return ErrNodeUnavailable
	}
	if err != nil {
		coordLog.Infof("failed alloc nodes for topic: %v", err)
		return err
	}
	for i := 0; i < meta.PartitionNum; i++ {
		if _, ok := existPart[i]; ok {
			continue
		}
		var tmpTopicReplicaInfo TopicPartitionReplicaInfo
		tmpTopicReplicaInfo.ISR = isrList[i]
		tmpTopicReplicaInfo.Leader = leaders[i]
		tmpTopicReplicaInfo.EpochForWrite = 1

		err = self.leadership.UpdateTopicNodeInfo(topic, i, &tmpTopicReplicaInfo, tmpTopicReplicaInfo.Epoch)
		if err != nil {
			coordLog.Infof("failed update info for topic : %v-%v, %v", topic, i, err)
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
	return nil
}
