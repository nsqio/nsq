package consistence

import (
	"github.com/absolute8511/nsq/nsqd"
	_ "github.com/valyala/gorpc"
	"net"
	"net/rpc"
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

type NsqdCoordRpcServer struct {
	nsqdCoord   *NsqdCoordinator
	rpcListener net.Listener
}

func NewNsqdCoordRpcServer(coord *NsqdCoordinator) *NsqdCoordRpcServer {
	return &NsqdCoordRpcServer{
		nsqdCoord: coord,
	}
}

func (self *NsqdCoordRpcServer) start(ip, port string) error {
	e := rpc.Register(self)
	if e != nil {
		panic(e)
	}
	self.rpcListener, e = net.Listen("tcp4", ip+":"+port)
	if e != nil {
		coordLog.Warningf("listen rpc error : %v", e.Error())
		return e
	}

	coordLog.Infof("nsqd coordinator rpc listen at : %v", self.rpcListener.Addr())
	rpc.Accept(self.rpcListener)
	return nil
}

func (self *NsqdCoordRpcServer) stop() {
	if self.rpcListener != nil {
		self.rpcListener.Close()
	}
}

func (self *NsqdCoordinator) checkLookupForWrite(lookupEpoch int) error {
	if lookupEpoch < self.lookupLeader.Epoch {
		coordLog.Warningf("the lookupd epoch is smaller than last: %v", lookupEpoch)
		return ErrEpochMismatch
	}
	return nil
}

func (self *NsqdCoordRpcServer) NotifyTopicLeaderSession(rpcTopicReq RpcTopicLeaderSession, ret *bool) error {
	*ret = true
	if err := self.nsqdCoord.checkLookupForWrite(rpcTopicReq.LookupdEpoch); err != nil {
		return err
	}
	topicCoord, err := self.nsqdCoord.getTopicCoord(rpcTopicReq.TopicName, rpcTopicReq.TopicPartition)
	if err != nil {
		coordLog.Infof("topic partition missing.")
		return err
	}
	err = self.nsqdCoord.updateTopicLeaderSession(topicCoord, &rpcTopicReq.TopicLeaderSession)
	return err
}

func (self *NsqdCoordRpcServer) UpdateTopicInfo(rpcTopicReq RpcAdminTopicInfo, ret *bool) error {
	if err := self.nsqdCoord.checkLookupForWrite(rpcTopicReq.LookupdEpoch); err != nil {
		return err
	}
	coordLog.Infof("got update request for topic : %v", rpcTopicReq)
	coords, ok := self.nsqdCoord.topicCoords[rpcTopicReq.Name]
	if !ok {
		coords = make(map[int]*TopicCoordinator)
		self.nsqdCoord.topicCoords[rpcTopicReq.Name] = coords
	}
	tpCoord, ok := coords[rpcTopicReq.Partition]
	if !ok {
		tpCoord = &TopicCoordinator{disableWrite: true}
		coords[rpcTopicReq.Partition] = tpCoord
		rpcTopicReq.DisableWrite = true
	}
	return self.nsqdCoord.updateTopicInfo(tpCoord, rpcTopicReq.DisableWrite, &rpcTopicReq.TopicPartionMetaInfo)
}

func (self *NsqdCoordRpcServer) EnableTopicWrite(rpcTopicReq RpcAdminTopicInfo, ret *bool) error {
	// set the topic as not writable.
	if err := self.nsqdCoord.checkLookupForWrite(rpcTopicReq.LookupdEpoch); err != nil {
		return err
	}
	tp, err := self.nsqdCoord.getTopicCoord(rpcTopicReq.Name, rpcTopicReq.Partition)
	if err != nil {
		return err
	}
	tp.disableWrite = false
	*ret = true
	return nil
}

func (self *NsqdCoordRpcServer) DisableTopicWrite(rpcTopicReq RpcAdminTopicInfo, ret *bool) error {
	if err := self.nsqdCoord.checkLookupForWrite(rpcTopicReq.LookupdEpoch); err != nil {
		return err
	}

	tp, err := self.nsqdCoord.getTopicCoord(rpcTopicReq.Name, rpcTopicReq.Partition)
	if err != nil {
		return err
	}
	tp.disableWrite = true
	*ret = true
	return nil
}

func (self *NsqdCoordinator) GetTopicStats(topic string, stat *NodeTopicStats) error {
	if topic == "" {
		// all topic status
	}
	// the status of specific topic
	return nil
}

func (self *NsqdCoordRpcServer) UpdateCatchupForTopic(rpcTopicReq RpcAdminTopicInfo, ret *bool) error {
	if err := self.nsqdCoord.checkLookupForWrite(rpcTopicReq.LookupdEpoch); err != nil {
		return err
	}
	tp, err := self.nsqdCoord.getTopicCoord(rpcTopicReq.Name, rpcTopicReq.Partition)
	if err != nil {
		return err
	}

	tp.topicInfo.CatchupList = rpcTopicReq.CatchupList
	if FindSlice(tp.topicInfo.CatchupList, self.nsqdCoord.myNode.GetID()) != -1 {
		go self.nsqdCoord.catchupFromLeader(tp.topicInfo)
	}

	return nil
}

func (self *NsqdCoordRpcServer) UpdateChannelsForTopic(rpcTopicReq RpcAdminTopicInfo, ret *bool) error {
	if err := self.nsqdCoord.checkLookupForWrite(rpcTopicReq.LookupdEpoch); err != nil {
		return err
	}
	tp, err := self.nsqdCoord.getTopicCoord(rpcTopicReq.Name, rpcTopicReq.Partition)
	if err != nil {
		return err
	}
	tp.topicInfo.Channels = rpcTopicReq.Channels
	err = self.nsqdCoord.updateLocalTopicChannels(tp.topicInfo)
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

type RpcTestReq struct {
	Data string
}

type RpcTestRsp struct {
	RspData string
	RetErr  *CoordErr
}

func (self *NsqdCoordinator) checkForRpcCall(rpcData RpcTopicData) (*TopicLeaderSession, *CoordErr) {
	if v, ok := self.topicCoords[rpcData.TopicName]; ok {
		if topicCoord, ok := v[rpcData.TopicPartition]; ok {
			if topicCoord.GetLeaderEpoch() != rpcData.TopicLeaderEpoch {
				coordLog.Infof("rpc call with wrong epoch :%v", rpcData)
				return nil, ErrEpochMismatch
			}
			if topicCoord.GetLeaderSession() != rpcData.TopicSession {
				coordLog.Infof("rpc call with wrong session:%v", rpcData)
				return nil, ErrLeaderSessionMismatch
			}
			if !topicCoord.localDataLoaded {
				coordLog.Infof("local data is still loading. %v", topicCoord.topicInfo.GetTopicDesp())
				return nil, ErrLocalNotReadyForWrite
			}
			return &topicCoord.topicLeaderSession, nil
		}
	}
	coordLog.Infof("rpc call with missing topic :%v", rpcData)
	return nil, ErrMissingTopicCoord
}

func (self *NsqdCoordRpcServer) UpdateChannelOffset(info RpcChannelOffsetArg, retErr *CoordErr) error {
	_, retErr = self.nsqdCoord.checkForRpcCall(info.RpcTopicData)
	if retErr != nil {
		return retErr
	}
	// update local channel offset
	retErr = self.nsqdCoord.updateChannelOffsetLocal(info.TopicName, info.TopicPartition, info.Channel, info.ChannelOffset)
	return retErr
}

// receive from leader
func (self *NsqdCoordRpcServer) PutMessage(info RpcPutMessage, retErr *CoordErr) error {
	_, retErr = self.nsqdCoord.checkForRpcCall(info.RpcTopicData)
	if retErr != nil {
		return retErr
	}
	// do local pub message
	retErr = self.nsqdCoord.putMessageOnSlave(info.TopicName, info.TopicPartition, info.LogData, info.TopicMessage)
	return retErr
}

func (self *NsqdCoordRpcServer) GetLastCommitLogID(req RpcCommitLogReq, ret *int64) error {
	*ret = 0
	logMgr := self.nsqdCoord.getLogMgrWithoutCreate(req.TopicName, req.TopicPartition)
	if logMgr == nil {
		return nil
	}
	*ret = logMgr.GetLastCommitLogID()
	return nil
}

// return the logdata from offset, if the offset is larger than local,
// then return the last logdata on local.
func (self *NsqdCoordRpcServer) GetCommitLogFromOffset(req RpcCommitLogReq, ret *RpcCommitLogRsp) error {
	logMgr := self.nsqdCoord.getLogMgrWithoutCreate(req.TopicName, req.TopicPartition)
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

func (self *NsqdCoordRpcServer) PullCommitLogsAndData(req RpcPullCommitLogsReq, ret *RpcPullCommitLogsRsp) error {
	logMgr := self.nsqdCoord.getLogMgrWithoutCreate(req.TopicName, req.TopicPartition)
	if logMgr != nil {
		return ErrMissingTopicLog
	}

	var err error
	ret.Logs, err = logMgr.GetCommitLogs(req.StartLogOffset, req.LogMaxNum)
	ret.DataList = make([][]byte, 0, len(ret.Logs))
	for _, l := range ret.Logs {
		d, err := self.nsqdCoord.readMessageData(l.LogID, l.MsgOffset)
		if err != nil {
			return err
		}
		ret.DataList = append(ret.DataList, d)
	}
	return err
}

func (self *NsqdCoordRpcServer) TestRpcError(req RpcTestReq, ret *RpcTestRsp) error {
	ret.RspData = req.Data
	ret.RetErr = &CoordErr{
		ErrMsg:  req.Data,
		ErrCode: RpcNoErr,
		ErrType: CommonErr,
	}
	return nil
}
