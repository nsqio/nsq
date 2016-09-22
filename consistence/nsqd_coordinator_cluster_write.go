package consistence

import (
	"github.com/absolute8511/nsq/internal/levellogger"
	"github.com/absolute8511/nsq/nsqd"
	"strconv"
	"sync/atomic"
	"time"
)

type localWriteFunc func(*coordData) *CoordErr
type localExitFunc func(*CoordErr)
type localCommitFunc func() error
type localRollbackFunc func()
type refreshCoordFunc func(*coordData) *CoordErr
type slaveSyncFunc func(*NsqdRpcClient, string, *coordData) *CoordErr
type slaveAsyncFunc func(*NsqdRpcClient, string, *coordData) *SlaveAsyncWriteResult

type handleSyncResultFunc func(int, *coordData) bool

type checkDupFunc func(*coordData) bool

func (self *NsqdCoordinator) PutMessageToCluster(topic *nsqd.Topic,
	body []byte, traceID uint64) (nsqd.MessageID, nsqd.BackendOffset, int32, nsqd.BackendQueueEnd, error) {
	var commitLog CommitLogData
	var queueEnd nsqd.BackendQueueEnd
	msg := nsqd.NewMessage(0, body)
	msg.TraceID = traceID

	topicName := topic.GetTopicName()
	partition := topic.GetTopicPart()
	coord, checkErr := self.getTopicCoord(topicName, partition)
	if checkErr != nil {
		return msg.ID, nsqd.BackendOffset(commitLog.MsgOffset), commitLog.MsgSize, queueEnd, checkErr.ToErrorType()
	}

	var logMgr *TopicCommitLogMgr

	doLocalWrite := func(d *coordData) *CoordErr {
		logMgr = d.logMgr
		topic.Lock()
		id, offset, writeBytes, qe, localErr := topic.PutMessageNoLock(msg)
		queueEnd = qe
		topic.Unlock()
		if localErr != nil {
			coordLog.Warningf("put message to local failed: %v", localErr)
			return &CoordErr{localErr.Error(), RpcNoErr, CoordLocalErr}
		}
		commitLog.LogID = int64(id)
		// epoch should not be changed.
		// leader epoch change means leadership change, leadership change
		// need disable write which should hold the write lock.
		// However, we are holding write lock while doing the cluster write replication.
		commitLog.Epoch = d.GetTopicEpochForWrite()
		commitLog.LastMsgLogID = commitLog.LogID
		commitLog.MsgOffset = int64(offset)
		commitLog.MsgSize = writeBytes
		commitLog.MsgCnt = queueEnd.TotalMsgCnt()
		commitLog.MsgNum = 1

		return nil
	}
	doLocalExit := func(err *CoordErr) {
		if err != nil {
			coordLog.Infof("topic %v PutMessageToCluster msg %v error: %v", topic.GetFullName(), msg, err)
		}
	}
	doLocalCommit := func() error {
		localErr := logMgr.AppendCommitLog(&commitLog, false)
		if localErr != nil {
			coordLog.Errorf("topic : %v, Generator %v failed write commit log : %v, logmgr: %v, %v",
				topic.GetFullName(), topic.GetMsgGenerator(), localErr, logMgr.pLogID, logMgr.nLogID)
		}
		topic.Lock()
		topic.UpdateCommittedOffset(queueEnd)
		topic.Unlock()
		return localErr
	}
	doLocalRollback := func() {
		coordLog.Warningf("failed write begin rollback : %v, %v", topic.GetFullName(), commitLog)
		topic.Lock()
		topic.RollbackNoLock(nsqd.BackendOffset(commitLog.MsgOffset), 1)
		topic.Unlock()
	}
	doRefresh := func(d *coordData) *CoordErr {
		logMgr = d.logMgr
		if d.GetTopicEpochForWrite() != commitLog.Epoch {
			coordLog.Warningf("write epoch changed during write: %v, %v", d.GetTopicEpochForWrite(), commitLog)
			return ErrEpochMismatch
		}
		self.requestNotifyNewTopicInfo(d.topicInfo.Name, d.topicInfo.Partition)
		return nil
	}
	doSlaveSync := func(c *NsqdRpcClient, nodeID string, tcData *coordData) *CoordErr {
		// should retry if failed, and the slave should keep the last success write to avoid the duplicated
		putErr := c.PutMessage(&tcData.topicLeaderSession, &tcData.topicInfo, commitLog, msg)
		if putErr != nil {
			coordLog.Infof("sync write to replica %v failed: %v. put offset:%v, logmgr: %v, %v",
				nodeID, putErr, commitLog, logMgr.pLogID, logMgr.nLogID)
		}
		return putErr
	}
	handleSyncResult := func(successNum int, tcData *coordData) bool {
		if successNum == len(tcData.topicInfo.ISR) {
			return true
		}
		return false
	}

	clusterErr := self.doSyncOpToCluster(true, coord, doLocalWrite, doLocalExit, doLocalCommit, doLocalRollback,
		doRefresh, doSlaveSync, handleSyncResult)

	var err error
	if clusterErr != nil {
		err = clusterErr.ToErrorType()
	}
	return msg.ID, nsqd.BackendOffset(commitLog.MsgOffset), commitLog.MsgSize, queueEnd, err
}

func (self *NsqdCoordinator) PutMessagesToCluster(topic *nsqd.Topic,
	msgs []*nsqd.Message) (nsqd.MessageID, nsqd.BackendOffset, int32, error) {

	var commitLog CommitLogData
	topicName := topic.GetTopicName()
	partition := topic.GetTopicPart()
	coord, checkErr := self.getTopicCoord(topicName, partition)
	if checkErr != nil {
		return nsqd.MessageID(commitLog.LogID), nsqd.BackendOffset(commitLog.MsgOffset), commitLog.MsgSize, checkErr.ToErrorType()
	}

	var queueEnd nsqd.BackendQueueEnd
	var logMgr *TopicCommitLogMgr

	doLocalWrite := func(d *coordData) *CoordErr {
		topic.Lock()
		logMgr = d.logMgr
		id, offset, writeBytes, totalCnt, qe, localErr := topic.PutMessagesNoLock(msgs)
		queueEnd = qe
		topic.Unlock()
		if localErr != nil {
			coordLog.Warningf("put batch messages to local failed: %v", localErr)
			return &CoordErr{localErr.Error(), RpcNoErr, CoordLocalErr}
		}
		commitLog.LogID = int64(id)
		// epoch should not be changed.
		// leader epoch change means leadership change, leadership change
		// need disable write which should hold the write lock.
		// However, we are holding write lock while doing the cluster write replication.
		commitLog.Epoch = d.GetTopicEpochForWrite()
		commitLog.LastMsgLogID = int64(msgs[len(msgs)-1].ID)
		commitLog.MsgOffset = int64(offset)
		commitLog.MsgSize = writeBytes
		// This MsgCnt is the total count until now (include the current written batch message count)
		commitLog.MsgCnt = totalCnt
		commitLog.MsgNum = int32(len(msgs))
		return nil
	}
	doLocalExit := func(err *CoordErr) {
		if err != nil {
			coordLog.Infof("topic %v PutMessagesToCluster error: %v", topic.GetFullName(), err)
		}
	}
	doLocalCommit := func() error {
		localErr := logMgr.AppendCommitLog(&commitLog, false)
		if localErr != nil {
			coordLog.Errorf("topic : %v, Generator %v failed write commit log : %v, logMgr: %v, %v",
				topic.GetFullName(), topic.GetMsgGenerator(), localErr, logMgr.pLogID, logMgr.nLogID)
		}
		topic.Lock()
		topic.UpdateCommittedOffset(queueEnd)
		topic.Unlock()
		return localErr
	}
	doLocalRollback := func() {
		coordLog.Warningf("failed write begin rollback : %v, %v", topic.GetFullName(), commitLog)
		topic.Lock()
		topic.ResetBackendEndNoLock(nsqd.BackendOffset(commitLog.MsgOffset), commitLog.MsgCnt-1)
		topic.Unlock()
	}
	doRefresh := func(d *coordData) *CoordErr {
		logMgr = d.logMgr
		if d.GetTopicEpochForWrite() != commitLog.Epoch {
			coordLog.Warningf("write epoch changed during write: %v, %v", d.GetTopicEpochForWrite(), commitLog)
			return ErrEpochMismatch
		}
		self.requestNotifyNewTopicInfo(d.topicInfo.Name, d.topicInfo.Partition)
		return nil
	}
	doSlaveSync := func(c *NsqdRpcClient, nodeID string, tcData *coordData) *CoordErr {
		// should retry if failed, and the slave should keep the last success write to avoid the duplicated
		putErr := c.PutMessages(&tcData.topicLeaderSession, &tcData.topicInfo, commitLog, msgs)
		if putErr != nil {
			coordLog.Infof("sync write to replica %v failed: %v, put offset: %v, logmgr: %v, %v",
				nodeID, putErr, commitLog, logMgr.pLogID, logMgr.nLogID)
		}
		return putErr
	}
	handleSyncResult := func(successNum int, tcData *coordData) bool {
		if successNum == len(tcData.topicInfo.ISR) {
			return true
		}
		return false
	}
	clusterErr := self.doSyncOpToCluster(true, coord, doLocalWrite, doLocalExit, doLocalCommit, doLocalRollback,
		doRefresh, doSlaveSync, handleSyncResult)

	var err error
	if clusterErr != nil {
		err = clusterErr.ToErrorType()
	}

	return nsqd.MessageID(commitLog.LogID), nsqd.BackendOffset(commitLog.MsgOffset), commitLog.MsgSize, err
}

func (self *NsqdCoordinator) doSyncOpToCluster(isWrite bool, coord *TopicCoordinator, doLocalWrite localWriteFunc,
	doLocalExit localExitFunc, doLocalCommit localCommitFunc, doLocalRollback localRollbackFunc,
	doRefresh refreshCoordFunc, doSlaveSync slaveSyncFunc, handleSyncResult handleSyncResultFunc) *CoordErr {

	if isWrite {
		coord.writeHold.Lock()
		defer coord.writeHold.Unlock()
	}

	if coord.IsExiting() {
		return ErrTopicExiting
	}
	tcData := coord.GetData()
	if isWrite && coord.IsWriteDisabled() {
		return ErrWriteDisabled
	}
	topicName := tcData.topicInfo.Name
	topicPartition := tcData.topicInfo.Partition
	topicFullName := topicName + strconv.Itoa(topicPartition)

	var clusterWriteErr *CoordErr
	if clusterWriteErr = tcData.checkWriteForLeader(self.myNode.GetID()); clusterWriteErr != nil {
		coordLog.Warningf("topic(%v) check write failed :%v", topicFullName, clusterWriteErr)
		coordErrStats.incWriteErr(clusterWriteErr)
		return clusterWriteErr
	}
	if isWrite && !tcData.IsISRReadyForWrite() {
		coordLog.Infof("topic(%v) operation failed since no enough ISR:%v", topicFullName, tcData.topicInfo)
		coordErrStats.incWriteErr(ErrWriteQuorumFailed)
		return ErrWriteQuorumFailed
	}

	checkCost := coordLog.Level() >= levellogger.LOG_DEBUG
	if self.enableBenchCost {
		checkCost = true
	}

	needRefreshISR := false
	needLeaveISR := false
	success := 0
	failedNodes := make(map[string]struct{})
	retryCnt := uint32(0)
	exitErr := 0

	localErr := doLocalWrite(tcData)
	if localErr != nil {
		clusterWriteErr = localErr
		goto exitsync
	}
	needLeaveISR = true

retrysync:
	if retryCnt > MAX_WRITE_RETRY {
		coordLog.Warningf("retrying times is large: %v", retryCnt)
		needRefreshISR = true
		if coord.IsExiting() {
			clusterWriteErr = ErrTopicExiting
			goto exitsync
		}
		time.Sleep(time.Second * 3)
	}
	if needRefreshISR {
		tcData = coord.GetData()
		if clusterWriteErr = tcData.checkWriteForLeader(self.myNode.GetID()); clusterWriteErr != nil {
			coordLog.Warningf("topic(%v) check operation failed :%v", topicFullName, clusterWriteErr)
			goto exitsync
		}
		if clusterWriteErr = doRefresh(tcData); clusterWriteErr != nil {
			coordLog.Warningf("topic(%v) failed refresh data:%v", topicFullName, clusterWriteErr)
			goto exitsync
		}
		if retryCnt > 3 {
			go self.requestNotifyNewTopicInfo(topicName, topicPartition)
		}
	}
	success = 0
	failedNodes = make(map[string]struct{})
	retryCnt++

	// send message to slaves with current topic epoch
	// replica should check if offset matching. If not matched the replica should leave the ISR list.
	// also, the coordinator should retry on fail until all nodes in ISR success.
	// If failed, should update ISR and retry.
	// write epoch should keep the same (ignore epoch change during write)
	// TODO: optimize send all requests first and then wait all responses
	exitErr = 0
	for _, nodeID := range tcData.topicInfo.ISR {
		if nodeID == self.myNode.GetID() {
			success++
			continue
		}

		c, rpcErr := self.acquireRpcClient(nodeID)
		if rpcErr != nil {
			coordLog.Infof("get rpc client %v failed: %v", nodeID, rpcErr)
			needRefreshISR = true
			failedNodes[nodeID] = struct{}{}
			continue
		}
		var start time.Time
		if checkCost {
			start = time.Now()
		}
		// should retry if failed, and the slave should keep the last success write to avoid the duplicated
		rpcErr = doSlaveSync(c, nodeID, tcData)
		if checkCost {
			cost := time.Since(start)
			if cost > time.Millisecond*3 {
				coordLog.Infof("slave(%v) sync cost long: %v", nodeID, cost)
			}
			if self.enableBenchCost {
				coordLog.Warningf("slave(%v) sync cost: %v, start: %v, end: %v", nodeID, cost, start, time.Now())
			}
		}
		if rpcErr == nil {
			success++
		} else {
			coordLog.Infof("sync operation to replica %v failed: %v", nodeID, rpcErr)
			clusterWriteErr = rpcErr
			failedNodes[nodeID] = struct{}{}
			if !rpcErr.CanRetryWrite(int(retryCnt)) {
				exitErr++
				coordLog.Infof("operation failed and no retry type: %v, %v", rpcErr.ErrType, exitErr)
				if exitErr > len(tcData.topicInfo.ISR)/2 {
					goto exitsync
				}
			}
		}
	}

	if handleSyncResult(success, tcData) {
		localErr := doLocalCommit()
		if localErr != nil {
			coordLog.Errorf("topic : %v failed commit operation: %v", topicFullName, localErr)
			needLeaveISR = true
			clusterWriteErr = &CoordErr{localErr.Error(), RpcCommonErr, CoordLocalErr}
		} else {
			needLeaveISR = false
			clusterWriteErr = nil
		}
	} else {
		coordLog.Warningf("topic %v sync operation failed since no enough success: %v", topicFullName, success)
		if success > tcData.topicInfo.Replica/2 {
			needLeaveISR = false
			if retryCnt > MAX_WRITE_RETRY {
				// request lookup to remove the failed nodes from isr and keep the quorum alive.
				// isr may down or some error.
				// We also need do some work to decide if we
				// should give up my leadership.
				for nid, _ := range failedNodes {
					tmpErr := self.requestLeaveFromISRByLeader(topicName, topicPartition, nid)
					if tmpErr != nil {
						coordLog.Warningf("failed to request remove the failed isr node: %v, %v", nid, tmpErr)
						break
					} else {
						coordLog.Infof("request the failed node: %v to leave topic %v isr", nid, topicFullName)
					}
				}
				time.Sleep(time.Second)
			}
		} else {
			needLeaveISR = true
		}

		if retryCnt > MAX_WRITE_RETRY*2 {
			coordLog.Warningf("topic %v sync write failed due to max retry: %v", topicFullName, retryCnt)
			goto exitsync
		}

		needRefreshISR = true
		sleepTime := time.Millisecond * time.Duration(2<<retryCnt)
		if (sleepTime > 0) && (sleepTime < MaxRetryWait) {
			time.Sleep(sleepTime)
		} else {
			time.Sleep(MaxRetryWait)
		}
		goto retrysync
	}
exitsync:
	if needLeaveISR {
		doLocalRollback()
		coord.dataMutex.Lock()
		newCoordData := coord.coordData.GetCopy()
		newCoordData.topicLeaderSession.LeaderNode = nil
		coord.coordData = newCoordData
		coord.dataMutex.Unlock()
		atomic.StoreInt32(&coord.disableWrite, 1)
		coordLog.Warningf("topic %v failed to sync to isr, need leave isr", tcData.topicInfo.GetTopicDesp())
		// leave isr
		go func() {
			tmpErr := self.requestLeaveFromISR(tcData.topicInfo.Name, tcData.topicInfo.Partition)
			if tmpErr != nil {
				coordLog.Warningf("failed to request leave from isr: %v", tmpErr)
			}
		}()
	}
	doLocalExit(clusterWriteErr)
	if clusterWriteErr == nil {
		// should return nil since the return type error is different with *CoordErr
		return nil
	} else {
		coordLog.Infof("write should be disabled to check log since write failed: %v", clusterWriteErr)
		coordErrStats.incWriteErr(clusterWriteErr)
		atomic.StoreInt32(&coord.disableWrite, 1)
		go self.requestCheckTopicConsistence(topicName, topicPartition)
	}
	return clusterWriteErr
}

func (self *NsqdCoordinator) putMessageOnSlave(coord *TopicCoordinator, logData CommitLogData, msg *nsqd.Message) *CoordErr {
	var logMgr *TopicCommitLogMgr
	var topic *nsqd.Topic
	var queueEnd nsqd.BackendQueueEnd

	checkDupOnSlave := func(tc *coordData) bool {
		if coordLog.Level() >= levellogger.LOG_DETAIL {
			topicName := tc.topicInfo.Name
			coordLog.Debugf("pub on slave : %v, msg %v", topicName, msg.ID)
		}
		logMgr = tc.logMgr
		if logMgr.IsCommitted(logData.LogID) {
			coordLog.Infof("pub the already committed log id : %v", logData.LogID)
			return true
		}
		return false
	}

	doLocalWriteOnSlave := func(tc *coordData) *CoordErr {
		var localErr error
		topicName := tc.topicInfo.Name
		partition := tc.topicInfo.Partition
		topic, localErr = self.localNsqd.GetExistingTopic(topicName, partition)
		if localErr != nil {
			coordLog.Infof("pub on slave missing topic : %v", topicName)
			// leave the isr and try re-sync with leader
			return &CoordErr{localErr.Error(), RpcErrTopicNotExist, CoordSlaveErr}
		}

		if topic.GetTopicPart() != partition {
			coordLog.Errorf("topic on slave has different partition : %v vs %v", topic.GetTopicPart(), partition)
			return &CoordErr{ErrLocalTopicPartitionMismatch.String(), RpcErrTopicNotExist, CoordSlaveErr}
		}

		topic.Lock()
		queueEnd, localErr = topic.PutMessageOnReplica(msg, nsqd.BackendOffset(logData.MsgOffset))
		topic.Unlock()
		if localErr != nil {
			coordLog.Errorf("put message on slave failed: %v", localErr)
			return &CoordErr{localErr.Error(), RpcCommonErr, CoordSlaveErr}
		}
		return nil
	}

	doLocalCommit := func() error {
		localErr := logMgr.AppendCommitLog(&logData, true)
		if localErr != nil {
			coordLog.Errorf("write commit log on slave failed: %v", localErr)
			return localErr
		}
		topic.Lock()
		topic.UpdateCommittedOffset(queueEnd)
		topic.Unlock()
		return nil
	}
	doLocalExit := func(err *CoordErr) {
		if err != nil {
			coordLog.Infof("slave put message %v error: %v", logData, err)
		}
	}

	return self.doWriteOpOnSlave(coord, checkDupOnSlave, doLocalWriteOnSlave, doLocalCommit, doLocalExit)
}

func (self *NsqdCoordinator) putMessagesOnSlave(coord *TopicCoordinator, logData CommitLogData, msgs []*nsqd.Message) *CoordErr {
	if len(msgs) == 0 {
		return ErrPubArgError
	}
	if logData.LogID != int64(msgs[0].ID) {
		return ErrPubArgError
	}
	var logMgr *TopicCommitLogMgr
	// this last log id should be used on slave to avoid the slave switch
	// override the leader's prev mpub message id.
	// While slave is chosen as leader, the next id should be larger than the last logid.
	// Because the mpub maybe already committed after the leader is down, the new leader should begin
	// with the last message id + 1 for next message.
	lastMsgLogID := int64(msgs[len(msgs)-1].ID)
	if logData.LastMsgLogID != lastMsgLogID {
		return ErrPubArgError
	}

	var queueEnd nsqd.BackendQueueEnd
	var topic *nsqd.Topic
	checkDupOnSlave := func(tc *coordData) bool {
		if coordLog.Level() >= levellogger.LOG_DETAIL {
			topicName := tc.topicInfo.Name
			coordLog.Debugf("pub on slave : %v, msg count: %v", topicName, len(msgs))
		}
		logMgr = tc.logMgr
		if logMgr.IsCommitted(logData.LogID) {
			coordLog.Infof("put the already committed log id : %v", logData.LogID)
			return true
		}
		return false
	}

	doLocalWriteOnSlave := func(tc *coordData) *CoordErr {
		var localErr error
		var start time.Time
		checkCost := coordLog.Level() >= levellogger.LOG_DEBUG
		if self.enableBenchCost {
			checkCost = true
		}
		if checkCost {
			start = time.Now()
		}
		topicName := tc.topicInfo.Name
		partition := tc.topicInfo.Partition
		topic, localErr = self.localNsqd.GetExistingTopic(topicName, partition)
		if localErr != nil {
			coordLog.Infof("pub on slave missing topic : %v", topicName)
			// leave the isr and try re-sync with leader
			return &CoordErr{localErr.Error(), RpcErrTopicNotExist, CoordSlaveErr}
		}

		topic.Lock()
		var cost time.Duration
		if checkCost {
			cost = time.Now().Sub(start)
			if cost > time.Millisecond {
				coordLog.Infof("prepare write on slave local cost :%v", cost)
			}
		}

		queueEnd, localErr = topic.PutMessagesOnReplica(msgs, nsqd.BackendOffset(logData.MsgOffset))
		if checkCost {
			cost2 := time.Now().Sub(start)
			if cost2 > time.Millisecond {
				coordLog.Infof("write local on slave cost :%v, %v", cost, cost2)
			}
		}

		topic.Unlock()
		if localErr != nil {
			logIndex, lastLogOffset, lastLog, _ := logMgr.GetLastCommitLogOffsetV2()
			coordLog.Errorf("put messages on slave failed: %v, slave last logid: %v, data: %v:%v, %v",
				localErr, logMgr.GetLastCommitLogID(), logIndex, lastLogOffset, lastLog)
			return &CoordErr{localErr.Error(), RpcCommonErr, CoordSlaveErr}
		}
		return nil
	}

	doLocalCommit := func() error {
		localErr := logMgr.AppendCommitLog(&logData, true)
		if localErr != nil {
			coordLog.Errorf("write commit log on slave failed: %v", localErr)
			return localErr
		}
		topic.Lock()
		topic.UpdateCommittedOffset(queueEnd)
		topic.Unlock()
		return nil
	}

	doLocalExit := func(err *CoordErr) {
		if err != nil {
			coordLog.Warningf("failed to batch put messages on slave: %v", err)
		}
	}
	return self.doWriteOpOnSlave(coord, checkDupOnSlave, doLocalWriteOnSlave, doLocalCommit,
		doLocalExit)
}

func (self *NsqdCoordinator) doWriteOpOnSlave(coord *TopicCoordinator, checkDupOnSlave checkDupFunc,
	doLocalWriteOnSlave localWriteFunc, doLocalCommit localCommitFunc, doLocalExit localExitFunc) *CoordErr {
	var start time.Time

	checkCost := coordLog.Level() >= levellogger.LOG_DEBUG
	if self.enableBenchCost {
		checkCost = true
	}
	if checkCost {
		start = time.Now()
	}

	tc := coord.GetData()
	coord.writeHold.Lock()
	defer coord.writeHold.Unlock()
	// check should be protected by write lock to avoid the next write check during the commit log flushing.
	if checkDupOnSlave(tc) {
		return nil
	}

	if coord.IsExiting() {
		return ErrTopicExitingOnSlave
	}
	if coord.IsWriteDisabled() {
		return ErrWriteDisabled
	}
	if !tc.IsMineISR(self.myNode.GetID()) {
		coordErrStats.incWriteErr(ErrTopicWriteOnNonISR)
		return ErrTopicWriteOnNonISR
	}

	var cost time.Duration
	if checkCost {
		cost = time.Now().Sub(start)
		if cost > time.Millisecond {
			coordLog.Infof("prepare write on slave cost :%v", cost)
		}
	}

	topicName := tc.topicInfo.Name
	partition := tc.topicInfo.Partition

	var slaveErr *CoordErr
	var localErr error
	slaveErr = doLocalWriteOnSlave(tc)
	var cost2 time.Duration
	if checkCost {
		cost2 = time.Now().Sub(start)
		if cost2 > time.Millisecond {
			coordLog.Infof("write local on slave cost :%v, %v", cost, cost2)
		}
	}

	if slaveErr != nil {
		goto exitpubslave
	}
	localErr = doLocalCommit()
	if localErr != nil {
		slaveErr = &CoordErr{localErr.Error(), RpcCommonErr, CoordLocalErr}
		goto exitpubslave
	}
exitpubslave:
	if slaveErr != nil {
		coordErrStats.incWriteErr(slaveErr)
		coordLog.Infof("I am leaving topic %v-%v from isr since write on slave failed: %v", topicName, partition, slaveErr)
		// leave isr
		go func() {
			tmpErr := self.requestLeaveFromISR(topicName, partition)
			if tmpErr != nil {
				coordLog.Warningf("failed to request leave from isr: %v", tmpErr)
			}
		}()
	}
	doLocalExit(slaveErr)

	if checkCost {
		cost3 := time.Now().Sub(start)
		if cost3 > time.Millisecond {
			coordLog.Infof("write local on slave cost :%v, %v, %v", cost, cost2, cost3)
		}
		if self.enableBenchCost {
			coordLog.Warningf("write local on slave cost :%v, start: %v, end: %v", cost3, start, time.Now())
		}
	}

	return slaveErr
}

func (self *NsqdCoordinator) SetChannelConsumeOffsetToCluster(ch *nsqd.Channel, queueOffset int64, cnt int64, force bool) error {
	topicName := ch.GetTopicName()
	partition := ch.GetTopicPart()
	coord, checkErr := self.getTopicCoord(topicName, partition)
	if checkErr != nil {
		return checkErr.ToErrorType()
	}

	var syncOffset ChannelConsumerOffset
	syncOffset.AllowBackward = true
	syncOffset.VCnt = cnt
	syncOffset.VOffset = queueOffset

	doLocalWrite := func(d *coordData) *CoordErr {
		err := ch.SetConsumeOffset(nsqd.BackendOffset(queueOffset), cnt, force)
		if err != nil {
			if err != nsqd.ErrSetConsumeOffsetNotFirstClient {
				coordLog.Infof("failed to set the consume offset: %v, err:%v", queueOffset, err)
				return &CoordErr{err.Error(), RpcNoErr, CoordLocalErr}
			}
			coordLog.Debugf("the consume offset: %v can only be set by the first client", queueOffset)
			return ErrLocalSetChannelOffsetNotFirstClient
		}
		return nil
	}
	doLocalExit := func(err *CoordErr) {}
	doLocalCommit := func() error {
		return nil
	}
	doLocalRollback := func() {}
	doRefresh := func(d *coordData) *CoordErr {
		return nil
	}
	doSlaveSync := func(c *NsqdRpcClient, nodeID string, tcData *coordData) *CoordErr {
		if ch.IsEphemeral() {
			return nil
		}
		rpcErr := c.UpdateChannelOffset(&tcData.topicLeaderSession, &tcData.topicInfo, ch.GetName(), syncOffset)
		if rpcErr != nil {
			coordLog.Infof("sync channel(%v) offset to replica %v failed: %v, offset: %v", ch.GetName(),
				nodeID, rpcErr, syncOffset)
		}
		return rpcErr
	}
	handleSyncResult := func(successNum int, tcData *coordData) bool {
		if successNum == len(tcData.topicInfo.ISR) {
			return true
		}
		return false
	}
	clusterErr := self.doSyncOpToCluster(false, coord, doLocalWrite, doLocalExit, doLocalCommit, doLocalRollback,
		doRefresh, doSlaveSync, handleSyncResult)
	if clusterErr != nil {
		return clusterErr.ToErrorType()
	}
	return nil
}

func (self *NsqdCoordinator) FinishMessageToCluster(channel *nsqd.Channel, clientID int64, clientAddr string, msgID nsqd.MessageID) error {
	topicName := channel.GetTopicName()
	partition := channel.GetTopicPart()
	coord, checkErr := self.getTopicCoord(topicName, partition)
	if checkErr != nil {
		return checkErr.ToErrorType()
	}

	var syncOffset ChannelConsumerOffset
	changed := false
	var confirmed nsqd.BackendQueueEnd
	if channel.IsOrdered() {
		if !coord.GetData().IsISRReadyForWrite() {
			coordLog.Warningf("topic(%v) finish message ordered failed since no enough ISR", topicName)
			coordErrStats.incWriteErr(ErrWriteQuorumFailed)
			return ErrWriteQuorumFailed.ToErrorType()
		}

		confirmed = channel.GetConfirmed()
	}
	// TODO: maybe use channel to aggregate all the sync of message to reduce the rpc call.

	doLocalWrite := func(d *coordData) *CoordErr {
		offset, cnt, tmpChanged, localErr := channel.FinishMessage(clientID, clientAddr, msgID)
		if localErr != nil {
			coordLog.Infof("channel %v finish local msg %v error: %v", channel.GetName(), msgID, localErr)
			return &CoordErr{localErr.Error(), RpcNoErr, CoordLocalErr}
		}
		changed = tmpChanged
		syncOffset.VOffset = int64(offset)
		syncOffset.VCnt = cnt
		return nil
	}
	doLocalExit := func(err *CoordErr) {}
	doLocalCommit := func() error {
		channel.ContinueConsumeForOrder()
		return nil
	}
	doLocalRollback := func() {
		if channel.IsOrdered() && confirmed != nil {
			coordLog.Warningf("rollback channel confirm to : %v", confirmed)
			// reset read to last confirmed
			channel.SetConsumeOffset(confirmed.Offset(), confirmed.TotalMsgCnt(), true)
		}
	}
	doRefresh := func(d *coordData) *CoordErr {
		return nil
	}
	doSlaveSync := func(c *NsqdRpcClient, nodeID string, tcData *coordData) *CoordErr {
		if !changed || channel.IsEphemeral() {
			return nil
		}
		var rpcErr *CoordErr
		if channel.IsOrdered() {
			// if ordered, we need make sure all the consume offset is synced to all replicas
			rpcErr = c.UpdateChannelOffset(&tcData.topicLeaderSession, &tcData.topicInfo, channel.GetName(), syncOffset)
		} else {
			c.NotifyUpdateChannelOffset(&tcData.topicLeaderSession, &tcData.topicInfo, channel.GetName(), syncOffset)
		}
		if rpcErr != nil {
			coordLog.Infof("sync channel(%v) offset to replica %v failed: %v, offset: %v", channel.GetName(),
				nodeID, rpcErr, syncOffset)
		}
		return rpcErr
	}
	handleSyncResult := func(successNum int, tcData *coordData) bool {
		// we can ignore the error if this channel is not ordered. (just sync next time)
		if successNum == len(tcData.topicInfo.ISR) || !channel.IsOrdered() {
			return true
		}
		return false
	}
	clusterErr := self.doSyncOpToCluster(false, coord, doLocalWrite, doLocalExit, doLocalCommit, doLocalRollback,
		doRefresh, doSlaveSync, handleSyncResult)
	if clusterErr != nil {
		return clusterErr.ToErrorType()
	}
	return nil
}

func (self *NsqdCoordinator) updateChannelOffsetOnSlave(tc *coordData, channelName string, offset ChannelConsumerOffset) *CoordErr {
	topicName := tc.topicInfo.Name
	partition := tc.topicInfo.Partition

	if !tc.IsMineISR(self.myNode.GetID()) {
		return ErrTopicWriteOnNonISR
	}

	if coordLog.Level() >= levellogger.LOG_DETAIL {
		coordLog.Debugf("got update channel(%v) offset on slave : %v", channelName, offset)
	}
	coord, coordErr := self.getTopicCoord(topicName, partition)
	if coordErr != nil {
		return ErrMissingTopicCoord
	}

	topic, localErr := self.localNsqd.GetExistingTopic(topicName, partition)
	if localErr != nil {
		coordLog.Warningf("slave missing topic : %v", topicName)
		// TODO: leave the isr and try re-sync with leader
		return &CoordErr{localErr.Error(), RpcCommonErr, CoordSlaveErr}
	}

	if topic.GetTopicPart() != partition {
		coordLog.Errorf("topic on slave has different partition : %v vs %v", topic.GetTopicPart(), partition)
		return ErrLocalMissingTopic
	}
	var ch *nsqd.Channel
	ch, localErr = topic.GetExistingChannel(channelName)
	// if a new channel on slave, we should set the consume offset by force
	if localErr != nil {
		offset.AllowBackward = true
		ch = topic.GetChannel(channelName)
		coordLog.Infof("slave init the channel : %v, %v, offset: %v", topic.GetTopicName(), channelName, ch.GetConfirmed())
	}
	if ch.IsEphemeral() {
		coordLog.Errorf("ephemeral channel %v should not be synced on slave", channelName)
	}
	currentEnd := ch.GetChannelEnd()
	if nsqd.BackendOffset(offset.VOffset) > currentEnd.Offset() {
		coordLog.Debugf("update channel(%v) consume offset exceed end %v on slave : %v", channelName, offset, currentEnd)
		// cache the offset (using map?) to reduce the slave channel flush.
		coord.consumeMgr.Lock()
		cur, ok := coord.consumeMgr.channelConsumeOffset[channelName]
		if !ok || cur.VOffset < offset.VOffset {
			coord.consumeMgr.channelConsumeOffset[channelName] = offset
		}
		coord.consumeMgr.Unlock()

		if offset.Flush {
			topic.ForceFlush()
			currentEnd = ch.GetChannelEnd()
			if nsqd.BackendOffset(offset.VOffset) > currentEnd.Offset() {
				offset.VOffset = int64(currentEnd.Offset())
				offset.VCnt = currentEnd.TotalMsgCnt()
			}
		} else {
			return nil
		}
	}
	err := ch.ConfirmBackendQueueOnSlave(nsqd.BackendOffset(offset.VOffset), offset.VCnt, offset.AllowBackward)
	if err != nil {
		coordLog.Warningf("update local channel(%v) offset %v failed: %v, current channel end: %v, topic end: %v",
			channelName, offset, err, currentEnd, topic.TotalDataSize())
		if err == nsqd.ErrExiting {
			return &CoordErr{err.Error(), RpcNoErr, CoordTmpErr}
		}
		return &CoordErr{err.Error(), RpcCommonErr, CoordSlaveErr}
	}
	return nil
}

func (self *NsqdCoordinator) DeleteChannel(topic *nsqd.Topic, channelName string) error {
	topicName := topic.GetTopicName()
	partition := topic.GetTopicPart()
	coord, checkErr := self.getTopicCoord(topicName, partition)
	if checkErr != nil {
		return checkErr.ToErrorType()
	}

	doLocalWrite := func(d *coordData) *CoordErr {
		localErr := topic.DeleteExistingChannel(channelName)
		if localErr != nil {
			coordLog.Infof("deleteing local channel %v error: %v", channelName, localErr)
		}
		return nil
	}
	doLocalExit := func(err *CoordErr) {}
	doLocalCommit := func() error {
		return nil
	}
	doLocalRollback := func() {
	}
	doRefresh := func(d *coordData) *CoordErr {
		return nil
	}
	doSlaveSync := func(c *NsqdRpcClient, nodeID string, tcData *coordData) *CoordErr {
		rpcErr := c.DeleteChannel(&tcData.topicLeaderSession, &tcData.topicInfo, channelName)
		if rpcErr != nil {
			coordLog.Infof("delete channel(%v) to replica %v failed: %v", channelName,
				nodeID, rpcErr)
		}
		return rpcErr
	}
	handleSyncResult := func(successNum int, tcData *coordData) bool {
		// we can ignore the error if this channel is not ordered. (just sync next time)
		if successNum == len(tcData.topicInfo.ISR) {
			return true
		}
		return false
	}
	clusterErr := self.doSyncOpToCluster(false, coord, doLocalWrite, doLocalExit, doLocalCommit, doLocalRollback,
		doRefresh, doSlaveSync, handleSyncResult)
	if clusterErr != nil {
		return clusterErr.ToErrorType()
	}
	return nil
}

func (self *NsqdCoordinator) deleteChannelOnSlave(tc *coordData, channelName string) *CoordErr {
	topicName := tc.topicInfo.Name
	partition := tc.topicInfo.Partition

	if !tc.IsMineISR(self.myNode.GetID()) {
		return ErrTopicWriteOnNonISR
	}

	coordLog.Logf("got delete channel(%v) offset on slave ", channelName)
	topic, localErr := self.localNsqd.GetExistingTopic(topicName, partition)
	if localErr != nil {
		coordLog.Warningf("slave missing topic : %v", topicName)
		return nil
	}

	localErr = topic.DeleteExistingChannel(channelName)
	if localErr != nil {
		coordLog.Logf("delete channel %v on slave failed: %v ", channelName, localErr)
	}
	return nil
}
