package consistence

import (
	"github.com/absolute8511/nsq/nsqd"
	_ "github.com/valyala/gorpc"
)

type ErrRPCRetCode int

const (
	RpcNoErr ErrRPCRetCode = iota
	RpcCommonErr
)

const (
	RpcErrLeavingISRWait = iota + 10
	RpcErrNoLeader
	RpcErrShouldTryLeader
)

var gRPCRetCodeMap map[error]ErrRPCRetCode

func (self *ErrRPCRetCode) IsMatchedError(e error) bool {
	if elem, ok := gRPCRetCodeMap[e]; ok {
		return elem == *self
	}
	return false
}

func init() {
	gRPCRetCodeMap = make(map[error]ErrRPCRetCode)
	gRPCRetCodeMap[ErrLeavingISRWait] = RpcErrLeavingISRWait
}

func GetRpcErrCode(e error) ErrRPCRetCode {
	if e == nil {
		return RpcNoErr
	}
	code, ok := gRPCRetCodeMap[e]
	if !ok {
		return RpcCommonErr
	}
	return code
}

func FindSlice(in []string, e string) int {
	for i, v := range in {
		if v == e {
			return i
		}
	}
	return -1
}

type NsqdNodeLoadFactor struct {
	nodeLF        float32
	topicLeaderLF map[string]map[int]float32
	topicSlaveLF  map[string]map[int]float32
}

type RpcAdminTopicInfo struct {
	TopicPartionMetaInfo
	LookupdEpoch int
	DisableWrite bool
}

type RpcTopicLeaderSession struct {
	RpcTopicData
	TopicLeaderSession
	LookupdEpoch int
}

func (self *NsqdCoordinator) CheckLookupForWrite(lookupEpoch int) error {
	if lookupEpoch < self.lookupLeader.Epoch {
		coordLog.Warningf("the lookupd epoch is smaller than last: %v", lookupEpoch)
		return ErrEpochMismatch
	}
	return nil
}

func (self *NsqdCoordinator) NotifyTopicLeaderSession(rpcTopicReq RpcTopicLeaderSession, ret *bool) error {
	*ret = true
	if err := self.CheckLookupForWrite(rpcTopicReq.LookupdEpoch); err != nil {
		return err
	}
	if _, ok := self.topicCoords[rpcTopicReq.TopicName]; !ok {
		return ErrTopicNotCreated
	}
	topicPartitionInfo, ok := self.topicCoords[rpcTopicReq.TopicName][rpcTopicReq.TopicPartition]
	if !ok {
		coordLog.Infof("topic partition missing.")
		return ErrTopicNotCreated
	}
	if rpcTopicReq.LeaderEpoch < topicPartitionInfo.topicLeaderSession.LeaderEpoch {
		coordLog.Infof("topic partition leadership epoch error.")
		return ErrEpochLessThanCurrent
	}
	n := rpcTopicReq.TopicLeaderSession
	topicPartitionInfo.topicLeaderSession = rpcTopicReq.TopicLeaderSession
	if n.LeaderNode == nil || n.Session == "" {
		coordLog.Infof("topic leader is missing : %v", rpcTopicReq.RpcTopicData)
	} else if n.LeaderNode.GetID() == self.myNode.GetID() {
		coordLog.Infof("I become the leader for the topic: %v", rpcTopicReq.RpcTopicData)
	} else {
		coordLog.Infof("topic %v leader changed to :%v. epoch: %v", rpcTopicReq.RpcTopicData, n.LeaderNode.GetID(), n.LeaderEpoch)
		// if catching up, pull data from the new leader
		// if isr, make sure sync to the new leader
		if FindSlice(topicPartitionInfo.topicInfo.ISR, self.myNode.GetID()) != -1 {
			self.syncToNewLeader(rpcTopicReq.TopicName, rpcTopicReq.TopicPartition, &n)
		} else if FindSlice(topicPartitionInfo.topicInfo.CatchupList, self.myNode.GetID()) != -1 {
			self.catchupFromLeader(topicPartitionInfo.topicInfo)
		} else {
			// TODO: check if local has the topic data and decide whether to join
			// catchup list
		}
	}

	return nil
}

func (self *NsqdCoordinator) UpdateTopicInfo(rpcTopicReq RpcAdminTopicInfo, ret *bool) error {
	if err := self.CheckLookupForWrite(rpcTopicReq.LookupdEpoch); err != nil {
		return err
	}
	coordLog.Infof("got update request for topic : %v", rpcTopicReq)
	topicData, ok := self.topicCoords[rpcTopicReq.Name]
	if !ok {
		topicData = make(map[int]*TopicCoordinator)
		self.topicCoords[rpcTopicReq.Name] = topicData
	}
	tpMetaInfo, ok := topicData[rpcTopicReq.Partition]
	if !ok {
		tpMetaInfo = &TopicCoordinator{disableWrite: true}
		topicData[rpcTopicReq.Partition] = tpMetaInfo
		rpcTopicReq.DisableWrite = true
	}
	if rpcTopicReq.Epoch < tpMetaInfo.topicInfo.Epoch {
		return ErrEpochLessThanCurrent
	}
	// channels and catchup should only be modified in the separate rpc method.
	rpcTopicReq.Channels = tpMetaInfo.topicInfo.Channels
	rpcTopicReq.CatchupList = tpMetaInfo.topicInfo.CatchupList
	tpMetaInfo.topicInfo = rpcTopicReq.TopicPartionMetaInfo
	self.updateLocalTopic(rpcTopicReq.TopicPartionMetaInfo)
	if rpcTopicReq.Leader == self.myNode.GetID() {
		if !self.IsMineLeaderForTopic(rpcTopicReq.Name, rpcTopicReq.Partition) {
			coordLog.Infof("I am notified to be leader for the topic.")
			// leader switch need disable write until the lookup notify leader
			// to accept write.
			rpcTopicReq.DisableWrite = true
		}
		if rpcTopicReq.DisableWrite {
			topicData[rpcTopicReq.Partition].disableWrite = true
		}
		err := self.acquireTopicLeader(rpcTopicReq.TopicPartionMetaInfo)
		if err != nil {
			coordLog.Infof("acquire topic leader failed.")
		}
	} else if FindSlice(rpcTopicReq.ISR, self.myNode.GetID()) != -1 {
		coordLog.Infof("I am in isr list.")
	} else if FindSlice(rpcTopicReq.CatchupList, self.myNode.GetID()) != -1 {
		coordLog.Infof("I am in catchup list.")
	} else {
		coordLog.Infof("Not a topic related to me.")
		// TODO: check if local has the topic data and decide whether to join
		// catchup list
	}
	return nil
}

func (self *NsqdCoordinator) EnableTopicWrite(rpcTopicReq RpcAdminTopicInfo, ret *bool) error {
	// set the topic as not writable.
	if err := self.CheckLookupForWrite(rpcTopicReq.LookupdEpoch); err != nil {
		return err
	}
	if t, ok := self.topicCoords[rpcTopicReq.Name]; ok {
		if tp, ok := t[rpcTopicReq.Partition]; ok {
			tp.disableWrite = false
			return nil
		}
	}

	*ret = true
	return ErrMissingTopicCoord
}

func (self *NsqdCoordinator) DisableTopicWrite(rpcTopicReq RpcAdminTopicInfo, ret *bool) error {
	if err := self.CheckLookupForWrite(rpcTopicReq.LookupdEpoch); err != nil {
		return err
	}

	if t, ok := self.topicCoords[rpcTopicReq.Name]; ok {
		if tp, ok := t[rpcTopicReq.Partition]; ok {
			tp.disableWrite = true
			//TODO: wait until the current write finished.
			return nil
		}
	}
	*ret = true
	return ErrMissingTopicCoord
}

func (self *NsqdCoordinator) GetTopicStats(topic string, stat *NodeTopicStats) error {
	if topic == "" {
		// all topic status
	}
	// the status of specific topic
	return nil
}

func (self *NsqdCoordinator) UpdateCatchupForTopic(rpcTopicReq RpcAdminTopicInfo, ret *bool) error {
	if err := self.CheckLookupForWrite(rpcTopicReq.LookupdEpoch); err != nil {
		return err
	}
	t, ok := self.topicCoords[rpcTopicReq.Name]
	if !ok {
		return ErrMissingTopicCoord
	}
	tp, ok := t[rpcTopicReq.Partition]
	if !ok {
		return ErrMissingTopicCoord
	}

	tp.topicInfo.CatchupList = rpcTopicReq.CatchupList
	if FindSlice(tp.topicInfo.CatchupList, self.myNode.GetID()) != -1 {
		go self.catchupFromLeader(tp.topicInfo)
	}

	return nil
}

func (self *NsqdCoordinator) UpdateChannelsForTopic(rpcTopicReq RpcAdminTopicInfo, ret *bool) error {
	if err := self.CheckLookupForWrite(rpcTopicReq.LookupdEpoch); err != nil {
		return err
	}
	t, ok := self.topicCoords[rpcTopicReq.Name]
	if !ok {
		return ErrMissingTopicCoord
	}
	tp, ok := t[rpcTopicReq.Partition]
	if !ok {
		return ErrMissingTopicCoord
	}
	tp.topicInfo.Channels = rpcTopicReq.Channels
	err := self.updateLocalTopicChannels(tp.topicInfo)
	return err
}

type RpcTopicData struct {
	TopicName        string
	TopicPartition   int
	TopicSession     string
	TopicEpoch       int
	TopicLeaderEpoch int32
}

type RpcChannelOffsetArg struct {
	RpcTopicData
	Channel string
	// position file + file offset
	ChannelOffset ChannelConsumerOffset
}

type RpcPutMessages struct {
	RpcTopicData
	LogList       []CommitLogData
	TopicMessages []*nsqd.Message
}

type RpcPutMessage struct {
	RpcTopicData
	LogData      CommitLogData
	TopicMessage *nsqd.Message
}

type RpcCommitLogReq struct {
	RpcTopicData
	LogOffset int64
}

type RpcCommitLogRsp struct {
	LogOffset int64
	LogData   CommitLogData
}

type RpcPullCommitLogsReq struct {
	RpcTopicData
	StartLogOffset int64
	LogMaxNum      int
}

type RpcPullCommitLogsRsp struct {
	Logs     []CommitLogData
	DataList [][]byte
}

func (self *NsqdCoordinator) checkForRpcCall(rpcData RpcTopicData) (*TopicLeaderSession, *CoordErr) {
	if v, ok := self.topicCoords[rpcData.TopicName]; ok {
		if topicInfo, ok := v[rpcData.TopicPartition]; ok {
			if topicInfo.GetLeaderEpoch() != rpcData.TopicLeaderEpoch {
				coordLog.Infof("rpc call with wrong epoch :%v", rpcData)
				return nil, ErrEpochMismatch
			}
			if topicInfo.GetLeaderSession() != rpcData.TopicSession {
				coordLog.Infof("rpc call with wrong session:%v", rpcData)
				return nil, ErrLeaderSessionMismatch
			}
			if !self.localDataStates[topicInfo.topicInfo.Name][topicInfo.topicInfo.Partition] {
				coordLog.Infof("local data is still loading. %v", topicInfo.topicInfo.GetTopicDesp())
				return nil, ErrLocalNotReadyForWrite
			}
			return &topicInfo.topicLeaderSession, nil
		}
	}
	coordLog.Infof("rpc call with missing topic :%v", rpcData)
	return nil, ErrMissingTopicCoord
}

func (self *NsqdCoordinator) UpdateChannelOffset(info RpcChannelOffsetArg, ret *bool) error {
	_, err := self.checkForRpcCall(info.RpcTopicData)
	if err != nil {
		return err
	}
	// update local channel offset
	*ret = true
	err = self.updateChannelOffsetLocal(info.TopicName, info.TopicPartition, info.Channel, info.ChannelOffset)
	return err
}

// receive from leader
func (self *NsqdCoordinator) PutMessage(info RpcPutMessage, retErr *CoordErr) error {
	_, retErr = self.checkForRpcCall(info.RpcTopicData)
	if retErr != nil {
		return retErr
	}
	// do local pub message
	retErr = self.putMessageOnSlave(info.TopicName, info.TopicPartition, info.LogData, info.TopicMessage)
	return retErr
}

func (self *NsqdCoordinator) GetLastCommitLogID(req RpcCommitLogReq, ret *int64) {
	*ret = 0
	logMgr := self.getLogMgrWithoutCreate(req.TopicName, req.TopicPartition)
	if logMgr == nil {
		return
	}
	*ret = logMgr.GetLastCommitLogID()
}

// return the logdata from offset, if the offset is larger than local,
// then return the last logdata on local.
func (self *NsqdCoordinator) GetCommitLogFromOffset(req RpcCommitLogReq, ret *RpcCommitLogRsp) error {
	logMgr := self.getLogMgrWithoutCreate(req.TopicName, req.TopicPartition)
	if logMgr == nil {
		return ErrMissingTopicLog
	}
	logData, err := logMgr.GetCommmitLogFromOffset(req.LogOffset)
	if err != nil {
		if err != ErrCommitLogOutofBound {
			return err
		}
		ret.LogOffset, err = logMgr.GetLastLogOffset()
		if err != nil {
			return err
		}
		logData, err = logMgr.GetCommmitLogFromOffset(ret.LogOffset)

		if err != nil {
			return err
		}
		ret.LogData = *logData
		return ErrCommitLogOutofBound
	} else {
		ret.LogOffset = req.LogOffset
		ret.LogData = *logData
	}
	return nil
}

func (self *NsqdCoordinator) PullCommitLogsAndData(req RpcPullCommitLogsReq, ret *RpcPullCommitLogsRsp) error {
	logMgr := self.getLogMgrWithoutCreate(req.TopicName, req.TopicPartition)
	if logMgr != nil {
		return ErrMissingTopicLog
	}

	var err error
	ret.Logs, err = logMgr.GetCommitLogs(req.StartLogOffset, req.LogMaxNum)
	ret.DataList = make([][]byte, 0, len(ret.Logs))
	for _, l := range ret.Logs {
		d, err := self.readMessageData(l.LogID, l.MsgOffset)
		if err != nil {
			return err
		}
		ret.DataList = append(ret.DataList, d)
	}
	return err
}
