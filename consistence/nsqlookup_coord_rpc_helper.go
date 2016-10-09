package consistence

import (
	"net"
	"strconv"
	"time"
)

// some failed rpc means lost, we should always try to notify to the node when they are available
func (self *NsqLookupCoordinator) rpcFailRetryFunc(monitorChan chan struct{}) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-monitorChan:
			return
		case <-ticker.C:
			failList := make(map[string]RpcFailedInfo, 0)
			var currentNodes map[string]NsqdNodeInfo
			self.failedRpcMutex.Lock()
			for _, v := range self.failedRpcList {
				failList[v.nodeID+v.topic+strconv.Itoa(v.partition)] = v
			}
			if len(failList) > 0 {
				coordLog.Infof("failed rpc total: %v, %v", len(self.failedRpcList), len(failList))
				currentNodes = self.getCurrentNodes()
			}
			self.failedRpcList = self.failedRpcList[0:0]
			self.failedRpcMutex.Unlock()
			epoch := self.leaderNode.Epoch
			for _, info := range failList {
				coordLog.Infof("retry failed rpc call for topic: %v", info)
				topicInfo, err := self.leadership.GetTopicInfo(info.topic, info.partition)
				if err != nil {
					// TODO: ignore if the error is not exist key on etcd
					self.addRetryFailedRpc(info.topic, info.partition, info.nodeID)
					continue
				}
				if _, ok := currentNodes[info.nodeID]; !ok {
					coordLog.Infof("retry cancelled since node not exist: %v", info)
					continue
				}
				if FindSlice(topicInfo.ISR, info.nodeID) == -1 && FindSlice(topicInfo.CatchupList, info.nodeID) == -1 {
					continue
				}
				c, rpcErr := self.acquireRpcClient(info.nodeID)
				if rpcErr != nil {
					self.addRetryFailedRpc(info.topic, info.partition, info.nodeID)
					continue
				}
				rpcErr = c.UpdateTopicInfo(epoch, topicInfo)
				if rpcErr != nil {
					// this error should not retry anymore
					if !rpcErr.IsEqual(ErrTopicCoordExistingAndMismatch) {
						self.addRetryFailedRpc(info.topic, info.partition, info.nodeID)
					}
					continue
				}
				leaderSession, err := self.leadership.GetTopicLeaderSession(info.topic, info.partition)
				if err != nil {
					self.addRetryFailedRpc(info.topic, info.partition, info.nodeID)
					continue
				}
				rpcErr = c.NotifyTopicLeaderSession(epoch, topicInfo, leaderSession, "")
				if rpcErr != nil {
					self.addRetryFailedRpc(info.topic, info.partition, info.nodeID)
					continue
				}
			}
		}
	}
}

func (self *NsqLookupCoordinator) doNotifyToNsqdNodes(nodes []string, notifyRpcFunc func(string) *CoordErr) *CoordErr {
	currentNodes := self.getCurrentNodes()
	var coordErr *CoordErr
	for _, n := range nodes {
		node, ok := currentNodes[n]
		if !ok {
			coordLog.Infof("notify to nsqd node %v failed since node not found", n)
			coordErr = ErrNodeNotFound
			continue
		}
		err := notifyRpcFunc(node.GetID())
		if err != nil {
			coordLog.Infof("notify to nsqd node %v failed: %v", node, err)
			coordErr = err
		}
	}
	return coordErr
}

func (self *NsqLookupCoordinator) doNotifyToSingleNsqdNode(nodeID string, notifyRpcFunc func(string) *CoordErr) *CoordErr {
	self.nodesMutex.RLock()
	node, ok := self.nsqdNodes[nodeID]
	self.nodesMutex.RUnlock()
	if !ok {
		return ErrNodeNotFound
	}
	err := notifyRpcFunc(node.GetID())
	if err != nil {
		coordLog.Infof("notify to nsqd node %v failed: %v", node, err)
	}
	return err
}

func (self *NsqLookupCoordinator) doNotifyToTopicLeaderThenOthers(leader string, others []string, notifyRpcFunc func(string) *CoordErr) *CoordErr {
	err := self.doNotifyToSingleNsqdNode(leader, notifyRpcFunc)
	if err != nil {
		coordLog.Infof("notify to topic leader %v failed: %v", leader, err)
		return err
	}
	return self.doNotifyToNsqdNodes(others, notifyRpcFunc)
}

func (self *NsqLookupCoordinator) notifyTopicLeaderSession(topicInfo *TopicPartitionMetaInfo, leaderSession *TopicLeaderSession, joinSession string) *CoordErr {
	others := getOthersExceptLeader(topicInfo)
	coordLog.Infof("notify topic leader session changed: %v, %v, others: %v", topicInfo.GetTopicDesp(), leaderSession.Session, others)
	err := self.doNotifyToTopicLeaderThenOthers(topicInfo.Leader, others, func(nid string) *CoordErr {
		return self.sendTopicLeaderSessionToNsqd(self.leaderNode.Epoch, nid, topicInfo, leaderSession, joinSession)
	})
	return err
}

func (self *NsqLookupCoordinator) notifyAcquireTopicLeader(topicInfo *TopicPartitionMetaInfo) *CoordErr {
	rpcErr := self.doNotifyToSingleNsqdNode(topicInfo.Leader, func(nid string) *CoordErr {
		return self.sendAcquireTopicLeaderToNsqd(self.leaderNode.Epoch, nid, topicInfo)
	})
	if rpcErr != nil {
		coordLog.Infof("notify leader to acquire leader failed: %v", rpcErr)
	}
	return rpcErr
}

func (self *NsqLookupCoordinator) notifyReleaseTopicLeader(topicInfo *TopicPartitionMetaInfo, leaderSessionEpoch EpochType) *CoordErr {
	rpcErr := self.doNotifyToSingleNsqdNode(topicInfo.Leader, func(nid string) *CoordErr {
		return self.sendReleaseTopicLeaderToNsqd(self.leaderNode.Epoch, nid, topicInfo, leaderSessionEpoch)
	})
	if rpcErr != nil {
		coordLog.Infof("notify leader to acquire leader failed: %v", rpcErr)
	}
	return rpcErr
}

func (self *NsqLookupCoordinator) notifyISRTopicMetaInfo(topicInfo *TopicPartitionMetaInfo) *CoordErr {
	rpcErr := self.doNotifyToNsqdNodes(topicInfo.ISR, func(nid string) *CoordErr {
		return self.sendTopicInfoToNsqd(self.leaderNode.Epoch, nid, topicInfo)
	})
	if rpcErr != nil {
		coordLog.Infof("notify isr for topic meta info failed: %v", rpcErr)
	}
	return rpcErr
}

func (self *NsqLookupCoordinator) notifyCatchupTopicMetaInfo(topicInfo *TopicPartitionMetaInfo) *CoordErr {
	rpcErr := self.doNotifyToNsqdNodes(topicInfo.CatchupList, func(nid string) *CoordErr {
		return self.sendTopicInfoToNsqd(self.leaderNode.Epoch, nid, topicInfo)
	})
	if rpcErr != nil {
		coordLog.Infof("notify catchup for topic meta info failed: %v", rpcErr)
	}
	return rpcErr
}

func (self *NsqLookupCoordinator) notifyTopicMetaInfo(topicInfo *TopicPartitionMetaInfo) *CoordErr {
	others := getOthersExceptLeader(topicInfo)
	coordLog.Infof("notify topic meta info changed: %v", topicInfo)
	if topicInfo.Name == "" {
		coordLog.Infof("==== notify topic name is empty")
	}
	rpcErr := self.doNotifyToTopicLeaderThenOthers(topicInfo.Leader, others, func(nid string) *CoordErr {
		return self.sendTopicInfoToNsqd(self.leaderNode.Epoch, nid, topicInfo)
	})
	if rpcErr != nil {
		coordLog.Infof("notify topic meta info failed: %v", rpcErr)
	}
	return rpcErr
}

func (self *NsqLookupCoordinator) notifyOldNsqdsForTopicMetaInfo(topicInfo *TopicPartitionMetaInfo, oldNodes []string) *CoordErr {
	return self.doNotifyToNsqdNodes(oldNodes, func(nid string) *CoordErr {
		return self.sendTopicInfoToNsqd(self.leaderNode.Epoch, nid, topicInfo)
	})
}

func (self *NsqLookupCoordinator) addRetryFailedRpc(topic string, partition int, nid string) {
	failed := RpcFailedInfo{
		nodeID:    nid,
		topic:     topic,
		partition: partition,
		failTime:  time.Now(),
	}
	self.failedRpcMutex.Lock()
	self.failedRpcList = append(self.failedRpcList, failed)
	coordLog.Infof("failed rpc added: %v, total: %v", failed, len(self.failedRpcList))
	self.failedRpcMutex.Unlock()
}

func (self *NsqLookupCoordinator) sendTopicLeaderSessionToNsqd(epoch EpochType, nid string, topicInfo *TopicPartitionMetaInfo,
	leaderSession *TopicLeaderSession, joinSession string) *CoordErr {
	c, err := self.acquireRpcClient(nid)
	if err != nil {
		self.addRetryFailedRpc(topicInfo.Name, topicInfo.Partition, nid)
		return err
	}
	err = c.NotifyTopicLeaderSession(epoch, topicInfo, leaderSession, joinSession)
	if err != nil {
		self.addRetryFailedRpc(topicInfo.Name, topicInfo.Partition, nid)
	}
	return err
}

func (self *NsqLookupCoordinator) sendAcquireTopicLeaderToNsqd(epoch EpochType, nid string,
	topicInfo *TopicPartitionMetaInfo) *CoordErr {
	c, rpcErr := self.acquireRpcClient(nid)
	if rpcErr != nil {
		return rpcErr
	}
	rpcErr = c.NotifyAcquireTopicLeader(epoch, topicInfo)
	return rpcErr
}

func (self *NsqLookupCoordinator) sendReleaseTopicLeaderToNsqd(epoch EpochType, nid string,
	topicInfo *TopicPartitionMetaInfo, leaderSessionEpoch EpochType) *CoordErr {
	c, rpcErr := self.acquireRpcClient(nid)
	if rpcErr != nil {
		return rpcErr
	}
	rpcErr = c.NotifyReleaseTopicLeader(epoch, topicInfo, leaderSessionEpoch)
	return rpcErr
}

func (self *NsqLookupCoordinator) sendTopicInfoToNsqd(epoch EpochType, nid string, topicInfo *TopicPartitionMetaInfo) *CoordErr {
	c, rpcErr := self.acquireRpcClient(nid)
	if rpcErr != nil {
		self.addRetryFailedRpc(topicInfo.Name, topicInfo.Partition, nid)
		return rpcErr
	}
	rpcErr = c.UpdateTopicInfo(epoch, topicInfo)
	if rpcErr != nil {
		coordLog.Infof("failed to update topic info: %v, %v, %v", topicInfo.GetTopicDesp(), nid, rpcErr)
		self.addRetryFailedRpc(topicInfo.Name, topicInfo.Partition, nid)
	}
	return rpcErr
}

// each time change leader or isr list, make sure disable write.
// Because we need make sure the new leader and isr is in sync before accepting the
// write request.
func (self *NsqLookupCoordinator) notifyLeaderDisableTopicWriteFast(topicInfo *TopicPartitionMetaInfo) *CoordErr {
	c, err := self.acquireRpcClient(topicInfo.Leader)
	if err != nil {
		coordLog.Infof("failed to get rpc client: %v, %v", err, topicInfo.Leader)
		return err
	}
	err = c.DisableTopicWriteFast(self.leaderNode.Epoch, topicInfo)
	return err
}

func (self *NsqLookupCoordinator) notifyLeaderDisableTopicWrite(topicInfo *TopicPartitionMetaInfo) *CoordErr {
	c, err := self.acquireRpcClient(topicInfo.Leader)
	if err != nil {
		coordLog.Infof("failed to get rpc client: %v, %v", err, topicInfo.Leader)
		return err
	}
	err = c.DisableTopicWrite(self.leaderNode.Epoch, topicInfo)
	return err
}

func (self *NsqLookupCoordinator) notifyISRDisableTopicWrite(topicInfo *TopicPartitionMetaInfo) *CoordErr {
	for _, node := range topicInfo.ISR {
		if node == topicInfo.Leader {
			continue
		}
		c, err := self.acquireRpcClient(node)
		if err != nil {
			return err
		}
		err = c.DisableTopicWrite(self.leaderNode.Epoch, topicInfo)
		if err != nil {
			return err
		}
	}
	return nil
}

func (self *NsqLookupCoordinator) notifyEnableTopicWrite(topicInfo *TopicPartitionMetaInfo) *CoordErr {
	for _, node := range topicInfo.ISR {
		if node == topicInfo.Leader {
			continue
		}
		c, err := self.acquireRpcClient(node)
		if err != nil {
			return err
		}
		err = c.EnableTopicWrite(self.leaderNode.Epoch, topicInfo)
		if err != nil {
			return err
		}
	}
	c, err := self.acquireRpcClient(topicInfo.Leader)
	if err != nil {
		return err
	}
	err = c.EnableTopicWrite(self.leaderNode.Epoch, topicInfo)
	return err
}

func (self *NsqLookupCoordinator) isTopicWriteDisabled(topicInfo *TopicPartitionMetaInfo) bool {
	c, err := self.acquireRpcClient(topicInfo.Leader)
	if err != nil {
		return false
	}
	return c.IsTopicWriteDisabled(topicInfo)
}

func (self *NsqLookupCoordinator) getNsqdTopicStat(node NsqdNodeInfo) (*NodeTopicStats, error) {
	c, err := self.acquireRpcClient(node.GetID())
	if err != nil {
		return nil, err.ToErrorType()
	}
	return c.GetTopicStats("")
}

func (self *NsqLookupCoordinator) getNsqdLastCommitLogID(nid string, topicInfo *TopicPartitionMetaInfo) (int64, *CoordErr) {
	c, err := self.acquireRpcClient(nid)
	if err != nil {
		return 0, err
	}
	return c.GetLastCommitLogID(topicInfo)
}

func (self *NsqLookupCoordinator) acquireRpcClient(nid string) (*NsqdRpcClient, *CoordErr) {
	currentNodes := self.getCurrentNodes()

	self.rpcMutex.Lock()
	defer self.rpcMutex.Unlock()
	c, _ := self.nsqdRpcClients[nid]
	if c == nil {
		n, ok := currentNodes[nid]
		if !ok {
			coordLog.Infof("rpc node not found: %v", nid)
			return nil, ErrNodeNotFound
		}
		var err error
		c, err = NewNsqdRpcClient(net.JoinHostPort(n.NodeIP, n.RpcPort), RPC_TIMEOUT_FOR_LOOKUP)
		if err != nil {
			coordLog.Infof("rpc node %v client init failed : %v", nid, err)
			return nil, &CoordErr{err.Error(), RpcNoErr, CoordNetErr}
		}
		self.nsqdRpcClients[nid] = c
	}
	return c, nil
}
