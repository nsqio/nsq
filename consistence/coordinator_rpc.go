package consistence

import (
	"github.com/absolute8511/nsq/nsqd"
	_ "github.com/valyala/gorpc"
	"io"
	"net"
	"net/rpc"
)

type ErrRPCRetCode int

const (
	RpcNoErr ErrRPCRetCode = iota
	RpcCommonErr
)

const (
	RpcErrLeavingISRWait ErrRPCRetCode = iota + 10
	RpcErrNotTopicLeader
	RpcErrNoLeader
	RpcErrEpochMismatch
	RpcErrEpochLessThanCurrent
	RpcErrWriteQuorumFailed
	RpcErrCommitLogIDDup
	RpcErrCommitLogEOF
	RpcErrCommitLogOutofBound
	RpcErrMissingTopicLeaderSession
	RpcErrLeaderSessionMismatch
	RpcErrWriteDisabled
	RpcErrTopicNotExist
	RpcErrMissingTopicCoord
	RpcErrTopicCoordExistingAndMismatch
	RpcErrTopicLeaderChanged
	RpcErrTopicLoading
)

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
	LeaderNode   *NsqdNodeInfo
	LookupdEpoch int
	JoinSession  string
}

type NsqdCoordRpcServer struct {
	nsqdCoord    *NsqdCoordinator
	rpcListener  net.Listener
	rpcServer    *rpc.Server
	dataRootPath string
}

func NewNsqdCoordRpcServer(coord *NsqdCoordinator, rootPath string) *NsqdCoordRpcServer {
	return &NsqdCoordRpcServer{
		nsqdCoord:    coord,
		rpcServer:    rpc.NewServer(),
		dataRootPath: rootPath,
	}
}

func (self *NsqdCoordRpcServer) start(ip, port string) error {
	e := self.rpcServer.Register(self)
	if e != nil {
		panic(e)
	}
	self.rpcListener, e = net.Listen("tcp4", ip+":"+port)
	if e != nil {
		coordLog.Warningf("listen rpc error : %v", e.Error())
		return e
	}

	coordLog.Infof("nsqd coordinator rpc listen at : %v", self.rpcListener.Addr())
	self.rpcServer.Accept(self.rpcListener)
	return nil
}

func (self *NsqdCoordRpcServer) stop() {
	if self.rpcListener != nil {
		self.rpcListener.Close()
	}
}

func (self *NsqdCoordinator) checkLookupForWrite(lookupEpoch int) *CoordErr {
	if lookupEpoch < self.lookupLeader.Epoch {
		coordLog.Warningf("the lookupd epoch is smaller than last: %v", lookupEpoch)
		return ErrEpochMismatch
	}
	return nil
}

func (self *NsqdCoordRpcServer) NotifyTopicLeaderSession(rpcTopicReq RpcTopicLeaderSession, ret *CoordErr) error {
	if err := self.nsqdCoord.checkLookupForWrite(rpcTopicReq.LookupdEpoch); err != nil {
		*ret = *err
		return nil
	}
	coordLog.Infof("got leader session notify : %v, leader node info:%v", rpcTopicReq, rpcTopicReq.LeaderNode)
	topicCoord, err := self.nsqdCoord.getTopicCoord(rpcTopicReq.TopicName, rpcTopicReq.TopicPartition)
	if err != nil {
		coordLog.Infof("topic partition missing.")
		*ret = *err
		return nil
	}
	if rpcTopicReq.JoinSession != "" && !topicCoord.disableWrite {
		coordLog.Errorf("join session should disable write first")
		*ret = *ErrTopicCoordStateInvalid
		return nil
	}
	newSession := &TopicLeaderSession{
		LeaderNode:  rpcTopicReq.LeaderNode,
		Session:     rpcTopicReq.TopicLeaderSession,
		LeaderEpoch: rpcTopicReq.TopicLeaderEpoch,
	}
	err = self.nsqdCoord.updateTopicLeaderSession(topicCoord, newSession, rpcTopicReq.JoinSession)
	if err != nil {
		*ret = *err
	}
	return nil
}

func (self *NsqdCoordRpcServer) UpdateTopicInfo(rpcTopicReq RpcAdminTopicInfo, ret *bool) error {
	*ret = true
	if err := self.nsqdCoord.checkLookupForWrite(rpcTopicReq.LookupdEpoch); err != nil {
		return err
	}
	coordLog.Infof("got update request for topic : %v", rpcTopicReq)
	self.nsqdCoord.coordMutex.Lock()
	coords, ok := self.nsqdCoord.topicCoords[rpcTopicReq.Name]
	for pid, tc := range coords {
		if pid != rpcTopicReq.Partition {
			coordLog.Infof("found another partition %v already exist for this topic %v", pid, rpcTopicReq.Name)
			if _, err := self.nsqdCoord.localNsqd.GetExistingTopic(rpcTopicReq.Name); err != nil {
				coordLog.Infof("local no such topic, we can just remove this coord")
				tc.logMgr.Close()
				delete(coords, pid)
				continue
			}
			self.nsqdCoord.coordMutex.Unlock()
			return ErrTopicCoordExistingAndMismatch
		}
	}
	myID := self.nsqdCoord.myNode.GetID()
	if rpcTopicReq.Leader != myID &&
		FindSlice(rpcTopicReq.ISR, myID) == -1 &&
		FindSlice(rpcTopicReq.CatchupList, myID) == -1 {
		// a topic info not belong to me,
		// check if we need to delete local
		coordLog.Infof("Not a topic(%s) related to me. isr is : %v", rpcTopicReq.Name, rpcTopicReq.ISR)
		if ok {
			tc, ok := coords[rpcTopicReq.Partition]
			if ok {
				self.nsqdCoord.localNsqd.CloseExistingTopic(rpcTopicReq.Name, rpcTopicReq.Partition)
				coordLog.Infof("topic(%s) is removing from local node since not related", rpcTopicReq.Name)
				tc.logMgr.Close()
				delete(coords, rpcTopicReq.Partition)
			}
		}
		self.nsqdCoord.coordMutex.Unlock()
		return nil
	}
	if !ok {
		coords = make(map[int]*TopicCoordinator)
		self.nsqdCoord.topicCoords[rpcTopicReq.Name] = coords
	}
	tpCoord, ok := coords[rpcTopicReq.Partition]
	if !ok {
		var localErr error
		tpCoord, localErr = NewTopicCoordinator(rpcTopicReq.Name, rpcTopicReq.Partition,
			GetTopicPartitionBasePath(self.dataRootPath, rpcTopicReq.Name, rpcTopicReq.Partition))
		if localErr != nil || tpCoord == nil {
			self.nsqdCoord.coordMutex.Unlock()
			return ErrLocalInitTopicCoordFailed
		}
		tpCoord.disableWrite = true
		coords[rpcTopicReq.Partition] = tpCoord
		rpcTopicReq.DisableWrite = true
		coordLog.Infof("A new topic coord init on the node: %v", rpcTopicReq.GetTopicDesp())
	}

	self.nsqdCoord.coordMutex.Unlock()
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
	tp.DisableWrite(false)
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
	tp.DisableWrite(true)
	*ret = true
	return nil
}

func (self *NsqdCoordinator) GetTopicStats(topic string, stat *NodeTopicStats) error {
	// TODO: get local coordinator stats and errors, get local topic data stats
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
	tp, err := self.nsqdCoord.getTopicCoordData(rpcTopicReq.Name, rpcTopicReq.Partition)
	if err != nil {
		return err
	}

	tp.topicInfo.CatchupList = rpcTopicReq.CatchupList
	if FindSlice(tp.topicInfo.CatchupList, self.nsqdCoord.myNode.GetID()) != -1 {
		select {
		case self.nsqdCoord.tryCheckUnsynced <- true:
		default:
		}
	}

	return nil
}

type RpcTopicData struct {
	TopicName          string
	TopicPartition     int
	TopicEpoch         int32
	TopicLeaderEpoch   int32
	TopicLeaderSession string
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
	ErrInfo   CoordErr
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

func (self *NsqdCoordinator) checkForRpcCall(rpcData RpcTopicData) (*TopicCoordinator, *CoordErr) {
	if v, ok := self.topicCoords[rpcData.TopicName]; ok {
		if topicCoord, ok := v[rpcData.TopicPartition]; ok {
			if topicCoord.GetLeaderEpoch() != rpcData.TopicLeaderEpoch {
				coordLog.Infof("rpc call with wrong epoch :%v", rpcData)
				return nil, ErrEpochMismatch
			}
			if topicCoord.GetLeaderSession() != rpcData.TopicLeaderSession {
				coordLog.Infof("rpc call with wrong session:%v", rpcData)
				return nil, ErrLeaderSessionMismatch
			}
			if !topicCoord.localDataLoaded {
				coordLog.Infof("local data is still loading. %v", topicCoord.topicInfo.GetTopicDesp())
				return nil, ErrTopicLoading
			}
			return topicCoord, nil
		}
	}
	coordLog.Infof("rpc call with missing topic :%v", rpcData)
	return nil, ErrMissingTopicCoord
}

func (self *NsqdCoordRpcServer) UpdateChannelOffset(info RpcChannelOffsetArg, retErr *CoordErr) error {
	tc, err := self.nsqdCoord.checkForRpcCall(info.RpcTopicData)
	if err != nil {
		*retErr = *err
		return nil
	}
	// update local channel offset
	err = self.nsqdCoord.updateChannelOffsetOnSlave(tc.GetData(), info.Channel, info.ChannelOffset)
	if err != nil {
		*retErr = *err
	}
	return nil
}

// receive from leader
func (self *NsqdCoordRpcServer) PutMessage(info RpcPutMessage, retErr *CoordErr) error {
	tc, err := self.nsqdCoord.checkForRpcCall(info.RpcTopicData)
	if err != nil {
		*retErr = *err
		return nil
	}
	// do local pub message
	err = self.nsqdCoord.putMessageOnSlave(tc, info.LogData, info.TopicMessage)
	if err != nil {
		*retErr = *err
	}
	return nil
}

func (self *NsqdCoordRpcServer) GetLastCommitLogID(req RpcCommitLogReq, ret *int64) error {
	*ret = 0
	tc, err := self.nsqdCoord.getTopicCoordData(req.TopicName, req.TopicPartition)
	if err != nil {
		return err
	}
	*ret = tc.logMgr.GetLastCommitLogID()
	return nil
}

// return the logdata from offset, if the offset is larger than local,
// then return the last logdata on local.
func (self *NsqdCoordRpcServer) GetCommitLogFromOffset(req RpcCommitLogReq, ret *RpcCommitLogRsp) error {
	tcData, coorderr := self.nsqdCoord.getTopicCoordData(req.TopicName, req.TopicPartition)
	if coorderr != nil {
		ret.ErrInfo = *coorderr
		return nil
	}
	logData, err := tcData.logMgr.GetCommitLogFromOffset(req.LogOffset)
	if err != nil {
		var err2 error
		ret.LogOffset, err2 = tcData.logMgr.GetLastLogOffset()
		if err2 != nil {
			ret.ErrInfo = *NewCoordErr(err2.Error(), CoordCommonErr)
			return nil
		}
		logData, err2 = tcData.logMgr.GetCommitLogFromOffset(ret.LogOffset)

		if err2 != nil {
			if err2 == ErrCommitLogEOF {
				ret.ErrInfo = *ErrTopicCommitLogEOF
			} else {
				ret.ErrInfo = *NewCoordErr(err.Error(), CoordCommonErr)
			}
			return nil
		}
		ret.LogData = *logData
		if err == ErrCommitLogOutofBound {
			ret.ErrInfo = *ErrTopicCommitLogOutofBound
		} else if err == ErrCommitLogEOF {
			ret.ErrInfo = *ErrTopicCommitLogEOF
		} else {
			ret.ErrInfo = *NewCoordErr(err.Error(), CoordCommonErr)
		}
		return nil
	} else {
		ret.LogOffset = req.LogOffset
		ret.LogData = *logData
	}
	return nil
}

func (self *NsqdCoordRpcServer) PullCommitLogsAndData(req RpcPullCommitLogsReq, ret *RpcPullCommitLogsRsp) error {
	tcData, err := self.nsqdCoord.getTopicCoordData(req.TopicName, req.TopicPartition)
	if err != nil {
		return err
	}

	ret.DataList = make([][]byte, 0, len(ret.Logs))
	var localErr error
	ret.Logs, localErr = tcData.logMgr.GetCommitLogs(req.StartLogOffset, req.LogMaxNum)
	if localErr != nil {
		if localErr == io.EOF {
			return nil
		}
		return localErr
	}
	for _, l := range ret.Logs {
		d, err := self.nsqdCoord.readTopicRawData(tcData.topicInfo.Name, tcData.topicInfo.Partition, l.MsgOffset, l.MsgSize)
		if err != nil {
			return err
		}
		ret.DataList = append(ret.DataList, d)
	}
	return nil
}

func (self *NsqdCoordRpcServer) TestRpcError(req RpcTestReq, ret *RpcTestRsp) error {
	ret.RspData = req.Data
	ret.RetErr = &CoordErr{
		ErrMsg:  req.Data,
		ErrCode: RpcNoErr,
		ErrType: CoordCommonErr,
	}
	return nil
}
