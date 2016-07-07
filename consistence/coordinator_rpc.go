package consistence

import (
	"github.com/absolute8511/gorpc"
	"github.com/absolute8511/nsq/internal/levellogger"
	"github.com/absolute8511/nsq/nsqd"
	"net"
	"os"
	"runtime"
	"sync/atomic"
	"time"
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
	RpcErrTopicCoordConflicted
	RpcErrTopicLeaderChanged
	RpcErrTopicLoading
	RpcErrSlaveStateInvalid
	RpcErrTopicCoordStateInvalid
	RpcErrWriteOnNonISR
)

type SlaveAsyncWriteResult struct {
	ret gorpc.AsyncResult
}

func (self *SlaveAsyncWriteResult) Wait() {
	<-self.ret.Done
}

func (self *SlaveAsyncWriteResult) GetResult() *CoordErr {
	coordErr, ok := self.ret.Response.(*CoordErr)
	if ok {
		return convertRpcError(self.ret.Error, coordErr)
	} else {
		return convertRpcError(self.ret.Error, nil)
	}
}

type NsqdNodeLoadFactor struct {
	nodeLF        float32
	topicLeaderLF map[string]map[int]float32
	topicSlaveLF  map[string]map[int]float32
}

type RpcAdminTopicInfo struct {
	TopicPartitionMetaInfo
	LookupdEpoch EpochType
	DisableWrite bool
}

type RpcAcquireTopicLeaderReq struct {
	RpcTopicData
	LeaderNodeID string
	LookupdEpoch EpochType
}

type RpcTopicLeaderSession struct {
	RpcTopicData
	LeaderNode   NsqdNodeInfo
	LookupdEpoch EpochType
	JoinSession  string
}

type NsqdCoordRpcServer struct {
	nsqdCoord      *NsqdCoordinator
	rpcDispatcher  *gorpc.Dispatcher
	rpcServer      *gorpc.Server
	dataRootPath   string
	disableRpcTest bool
}

func NewNsqdCoordRpcServer(coord *NsqdCoordinator, rootPath string) *NsqdCoordRpcServer {
	return &NsqdCoordRpcServer{
		nsqdCoord:     coord,
		rpcDispatcher: gorpc.NewDispatcher(),
		dataRootPath:  rootPath,
	}
}

// used only for test
func (self *NsqdCoordRpcServer) toggleDisableRpcTest(disable bool) {
	self.disableRpcTest = disable
	coordLog.Infof("rpc is disabled on node: %v", self.nsqdCoord.myNode.GetID())
}

func (self *NsqdCoordRpcServer) start(ip, port string) error {
	self.rpcDispatcher.AddService("NsqdCoordRpcServer", self)
	self.rpcServer = gorpc.NewTCPServer(net.JoinHostPort(ip, port), self.rpcDispatcher.NewHandlerFunc())

	e := self.rpcServer.Start()
	if e != nil {
		coordLog.Errorf("start rpc server error : %v", e)
		os.Exit(1)
	}

	coordLog.Infof("nsqd coordinator rpc listen at : %v", self.rpcServer.Listener.ListenAddr())
	return nil
}

func (self *NsqdCoordRpcServer) stop() {
	if self.rpcServer != nil {
		self.rpcServer.Stop()
	}
}

func (self *NsqdCoordRpcServer) checkLookupForWrite(lookupEpoch EpochType) *CoordErr {
	if lookupEpoch < self.nsqdCoord.lookupLeader.Epoch {
		coordLog.Warningf("the lookupd epoch is smaller than last: %v", lookupEpoch)
		return ErrEpochMismatch
	}
	if self.disableRpcTest {
		return &CoordErr{"rpc disabled", RpcCommonErr, CoordNetErr}
	}
	return nil
}

func (self *NsqdCoordRpcServer) NotifyTopicLeaderSession(rpcTopicReq *RpcTopicLeaderSession) *CoordErr {
	s := time.Now().Unix()
	defer func() {
		e := time.Now().Unix()
		if e-s > int64(RPC_TIMEOUT/2) {
			coordLog.Infof("rpc call used: %v", e-s)
		}
	}()

	var ret CoordErr
	if err := self.checkLookupForWrite(rpcTopicReq.LookupdEpoch); err != nil {
		ret = *err
		return &ret
	}
	coordLog.Infof("got leader session notify : %v, leader node info:%v on node: %v", rpcTopicReq, rpcTopicReq.LeaderNode, self.nsqdCoord.myNode.GetID())
	topicCoord, err := self.nsqdCoord.getTopicCoord(rpcTopicReq.TopicName, rpcTopicReq.TopicPartition)
	if err != nil {
		coordLog.Infof("topic partition missing.")
		ret = *err
		return &ret
	}
	if rpcTopicReq.JoinSession != "" && !topicCoord.IsWriteDisabled() {
		if FindSlice(topicCoord.topicInfo.ISR, self.nsqdCoord.myNode.GetID()) != -1 {
			coordLog.Errorf("join session should disable write first")
			ret = *ErrTopicCoordStateInvalid
			return &ret
		}
	}
	newSession := &TopicLeaderSession{
		LeaderNode:  &rpcTopicReq.LeaderNode,
		Session:     rpcTopicReq.TopicLeaderSession,
		LeaderEpoch: rpcTopicReq.TopicLeaderSessionEpoch,
	}
	return self.nsqdCoord.updateTopicLeaderSession(topicCoord, newSession, rpcTopicReq.JoinSession)
}

func (self *NsqdCoordRpcServer) NotifyAcquireTopicLeader(rpcTopicReq *RpcAcquireTopicLeaderReq) *CoordErr {
	s := time.Now().Unix()
	defer func() {
		e := time.Now().Unix()
		if e-s > int64(RPC_TIMEOUT/2) {
			coordLog.Infof("rpc call used: %v", e-s)
		}
	}()

	var ret CoordErr
	if err := self.checkLookupForWrite(rpcTopicReq.LookupdEpoch); err != nil {
		ret = *err
		return &ret
	}
	if rpcTopicReq.TopicPartition < 0 || rpcTopicReq.TopicName == "" {
		return ErrTopicArgError
	}
	topicCoord, err := self.nsqdCoord.getTopicCoord(rpcTopicReq.TopicName, rpcTopicReq.TopicPartition)
	if err != nil {
		coordLog.Infof("topic partition missing.")
		ret = *err
		return &ret
	}
	tcData := topicCoord.GetData()
	if tcData.topicInfo.Epoch != rpcTopicReq.Epoch ||
		tcData.GetLeader() != rpcTopicReq.LeaderNodeID ||
		tcData.GetLeader() != self.nsqdCoord.myNode.GetID() {
		coordLog.Infof("not topic leader while acquire leader: %v, %v", tcData, rpcTopicReq)
		return nil
	}
	return self.nsqdCoord.notifyAcquireTopicLeader(tcData)
}

func (self *NsqdCoordRpcServer) UpdateTopicInfo(rpcTopicReq *RpcAdminTopicInfo) *CoordErr {
	s := time.Now().Unix()
	defer func() {
		e := time.Now().Unix()
		if e-s > int64(RPC_TIMEOUT/2) {
			coordLog.Infof("UpdateTopicInfo rpc call used: %v", e-s)
		}
	}()

	var ret CoordErr
	if err := self.checkLookupForWrite(rpcTopicReq.LookupdEpoch); err != nil {
		ret = *err
		return &ret
	}
	coordLog.Infof("got update request for topic : %v on node: %v", rpcTopicReq, self.nsqdCoord.myNode.GetID())
	if rpcTopicReq.Partition < 0 || rpcTopicReq.Name == "" {
		return ErrTopicArgError
	}

	self.nsqdCoord.coordMutex.Lock()
	coords, ok := self.nsqdCoord.topicCoords[rpcTopicReq.Name]
	myID := self.nsqdCoord.myNode.GetID()
	if rpcTopicReq.Leader == myID {
		for pid, tc := range coords {
			if tc.GetData().GetLeader() != myID {
				continue
			}
			if pid != rpcTopicReq.Partition {
				self.nsqdCoord.coordMutex.Unlock()
				coordLog.Infof("found another partition %v already exist master for this topic %v", pid, rpcTopicReq.Name)
				tmpInfo, err := self.nsqdCoord.leadership.GetTopicInfo(rpcTopicReq.Name, pid)
				if err != nil {
					if err == ErrKeyNotFound {
						self.nsqdCoord.requestLeaveFromISR(rpcTopicReq.Name, pid)
					} else {
						return &CoordErr{err.Error(), RpcCommonErr, CoordNetErr}
					}
				} else if tmpInfo.Leader != myID {
					coordLog.Infof("this conflict topic leader is not mine, but the data is wrong: %v, %v", tc.GetData(), tmpInfo)
					self.nsqdCoord.requestLeaveFromISR(rpcTopicReq.Name, pid)
				} else {
					ret = *ErrTopicCoordExistingAndMismatch
					return &ret
				}
				if _, err := self.nsqdCoord.localNsqd.GetExistingTopic(rpcTopicReq.Name, rpcTopicReq.Partition); err != nil {
					coordLog.Infof("local no such topic, we can just remove this coord")
					tc.logMgr.Close()
					delete(coords, pid)
					continue
				}
				return ErrMissingTopicCoord
			}
		}
	}
	if rpcTopicReq.Leader != myID &&
		FindSlice(rpcTopicReq.ISR, myID) == -1 &&
		FindSlice(rpcTopicReq.CatchupList, myID) == -1 {
		// a topic info not belong to me,
		// check if we need to delete local
		coordLog.Infof("Not a topic(%s) related to me. isr is : %v", rpcTopicReq.Name, rpcTopicReq.ISR)
		if ok {
			tc, ok := coords[rpcTopicReq.Partition]
			if ok {
				if tc.GetData().topicInfo.Leader == myID {
					self.nsqdCoord.releaseTopicLeader(&tc.GetData().topicInfo, &tc.topicLeaderSession)
				}
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
		self.nsqdCoord.checkLocalTopicMagicCode(&rpcTopicReq.TopicPartitionMetaInfo, true)

		var localErr error
		tpCoord, localErr = NewTopicCoordinator(rpcTopicReq.Name, rpcTopicReq.Partition,
			GetTopicPartitionBasePath(self.dataRootPath, rpcTopicReq.Name, rpcTopicReq.Partition), rpcTopicReq.SyncEvery)
		if localErr != nil || tpCoord == nil {
			self.nsqdCoord.coordMutex.Unlock()
			ret = *ErrLocalInitTopicCoordFailed
			return &ret
		}
		tpCoord.DisableWrite(true)

		_, coordErr := self.nsqdCoord.updateLocalTopic(&rpcTopicReq.TopicPartitionMetaInfo, tpCoord.GetData().logMgr)
		if coordErr != nil {
			coordLog.Warningf("init local topic failed: %v", coordErr)
			self.nsqdCoord.coordMutex.Unlock()
			ret = *coordErr
			return &ret
		}

		coords[rpcTopicReq.Partition] = tpCoord
		rpcTopicReq.DisableWrite = true
		coordLog.Infof("A new topic coord init on the node: %v", rpcTopicReq.GetTopicDesp())
	}

	self.nsqdCoord.coordMutex.Unlock()
	return self.nsqdCoord.updateTopicInfo(tpCoord, rpcTopicReq.DisableWrite, &rpcTopicReq.TopicPartitionMetaInfo)
}

func (self *NsqdCoordRpcServer) EnableTopicWrite(rpcTopicReq *RpcAdminTopicInfo) *CoordErr {
	s := time.Now().Unix()
	defer func() {
		e := time.Now().Unix()
		if e-s > int64(RPC_TIMEOUT/2) {
			coordLog.Infof("rpc call used: %v", e-s)
		}
	}()

	var ret CoordErr
	// set the topic as not writable.
	coordLog.Infof("got enable write for topic : %v", rpcTopicReq)
	if err := self.checkLookupForWrite(rpcTopicReq.LookupdEpoch); err != nil {
		ret = *err
		return &ret
	}
	tp, err := self.nsqdCoord.getTopicCoord(rpcTopicReq.Name, rpcTopicReq.Partition)
	if err != nil {
		ret = *err
		return &ret
	}
	begin := time.Now()
	tp.writeHold.Lock()
	if time.Since(begin) > time.Second*3 {
		// timeout for waiting
		coordLog.Infof("timeout while enable write for topic: %v", tp.GetData().topicInfo.GetTopicDesp())
		err = ErrOperationExpired
	} else {
		atomic.StoreInt32(&tp.disableWrite, 0)

		tcData := tp.GetData()
		if tcData.IsMineLeaderSessionReady(self.nsqdCoord.myNode.GetID()) {
			topicData, err := self.nsqdCoord.localNsqd.GetExistingTopic(tcData.topicInfo.Name, tcData.topicInfo.Partition)
			if err != nil {
				coordLog.Infof("no topic on local: %v, %v", tcData.topicInfo.GetTopicDesp(), err)
			} else {
				self.nsqdCoord.switchStateForMaster(tp, topicData, true, true)
			}
		}
	}
	tp.writeHold.Unlock()

	if err != nil {
		ret = *err
		return &ret
	}
	return nil
}

func (self *NsqdCoordRpcServer) DisableTopicWrite(rpcTopicReq *RpcAdminTopicInfo) *CoordErr {
	s := time.Now().Unix()
	defer func() {
		e := time.Now().Unix()
		if e-s > int64(RPC_TIMEOUT/2) {
			coordLog.Infof("rpc call used: %v", e-s)
		}
	}()

	var ret CoordErr
	coordLog.Infof("got disable write for topic : %v", rpcTopicReq)
	if err := self.checkLookupForWrite(rpcTopicReq.LookupdEpoch); err != nil {
		ret = *err
		return &ret
	}

	tp, err := self.nsqdCoord.getTopicCoord(rpcTopicReq.Name, rpcTopicReq.Partition)
	if err != nil {
		ret = *err
		return &ret
	}
	begin := time.Now()
	tp.writeHold.Lock()
	if time.Since(begin) > time.Second*3 {
		// timeout for waiting
		err = ErrOperationExpired
	} else {
		atomic.StoreInt32(&tp.disableWrite, 1)

		tcData := tp.GetData()
		localTopic, localErr := self.nsqdCoord.localNsqd.GetExistingTopic(tcData.topicInfo.Name, tcData.topicInfo.Partition)
		if localErr != nil {
			coordLog.Infof("no topic on local: %v, %v", tcData.topicInfo.GetTopicDesp(), localErr)
		} else {
			self.nsqdCoord.switchStateForMaster(tp, localTopic, false, false)
		}
	}
	tp.writeHold.Unlock()
	if err != nil {
		ret = *err
		return &ret
	}
	return nil
}

func (self *NsqdCoordRpcServer) IsTopicWriteDisabled(rpcTopicReq *RpcAdminTopicInfo) bool {
	tp, err := self.nsqdCoord.getTopicCoord(rpcTopicReq.Name, rpcTopicReq.Partition)
	if err != nil {
		return false
	}
	return tp.IsWriteDisabled()
}

func (self *NsqdCoordRpcServer) DeleteNsqdTopic(rpcTopicReq *RpcAdminTopicInfo) *CoordErr {
	var ret CoordErr
	if err := self.checkLookupForWrite(rpcTopicReq.LookupdEpoch); err != nil {
		ret = *err
		return &ret
	}

	coordLog.Infof("removing the local topic: %v", rpcTopicReq)
	_, err := self.nsqdCoord.removeTopicCoord(rpcTopicReq.Name, rpcTopicReq.Partition, true)
	if err != nil {
		ret = *err
		coordLog.Infof("delete topic %v failed : %v", rpcTopicReq.GetTopicDesp(), err)
		return &ret
	}
	localErr := self.nsqdCoord.localNsqd.DeleteExistingTopic(rpcTopicReq.Name, rpcTopicReq.Partition)
	if localErr != nil {
		ret = CoordErr{localErr.Error(), RpcCommonErr, CoordLocalErr}
		return &ret
	}
	return nil
}

func (self *NsqdCoordRpcServer) GetTopicStats(topic string) *NodeTopicStats {
	s := time.Now().Unix()
	defer func() {
		e := time.Now().Unix()
		if e-s > int64(RPC_TIMEOUT/2) {
			coordLog.Infof("GetTopicStats rpc call used: %v", e-s)
		}
	}()

	var stat NodeTopicStats
	var topicStats []nsqd.TopicStats
	// TODO: get local coordinator stats and errors, get local topic data stats
	if topic == "" {
		// all topic status
		topicStats = self.nsqdCoord.localNsqd.GetStats()
	} else {
		topicStats = self.nsqdCoord.localNsqd.GetTopicStats(topic)
	}
	stat.NodeID = self.nsqdCoord.myNode.GetID()
	stat.ChannelDepthData = make(map[string]int64, len(topicStats))
	stat.ChannelHourlyConsumedData = make(map[string]int64, len(topicStats))
	stat.TopicLeaderDataSize = make(map[string]int64, len(topicStats))
	stat.TopicTotalDataSize = make(map[string]int64, len(topicStats))
	stat.TopicPubQPS = nil
	stat.TopicChannelSubQPS = nil
	stat.NodeCPUs = runtime.NumCPU()
	for _, ts := range topicStats {
		// plus 1 to handle the empty topic/channel
		stat.TopicTotalDataSize[ts.TopicName] += ts.BackendDepth/1024/1024 + 1
		if ts.IsLeader {
			stat.TopicLeaderDataSize[ts.TopicName] += ts.BackendDepth/1024/1024 + 1
			for _, chStat := range ts.Channels {
				stat.ChannelDepthData[ts.TopicName] += chStat.BackendDepth/1024/1024 + 1
				stat.ChannelHourlyConsumedData[ts.TopicName] += chStat.HourlySubSize / 1024 / 1024
			}
		}
	}
	// the status of specific topic
	return &stat
}

type RpcTopicData struct {
	TopicName               string
	TopicPartition          int
	Epoch                   EpochType
	TopicWriteEpoch         EpochType
	TopicLeaderSessionEpoch EpochType
	TopicLeaderSession      string
}

type RpcChannelOffsetArg struct {
	RpcTopicData
	Channel string
	// position file + file offset
	ChannelOffset ChannelConsumerOffset
}

type RpcPutMessages struct {
	RpcTopicData
	LogData       CommitLogData
	TopicMessages []*nsqd.Message
}

type RpcPutMessage struct {
	RpcTopicData
	LogData      CommitLogData
	TopicMessage *nsqd.Message
}

type RpcCommitLogReq struct {
	RpcTopicData
	LogOffset     int64
	LogStartIndex int64
}

type RpcCommitLogRsp struct {
	LogStartIndex int64
	LogOffset     int64
	LogData       CommitLogData
	ErrInfo       CoordErr
}

type RpcPullCommitLogsReq struct {
	RpcTopicData
	StartLogOffset int64
	LogMaxNum      int
	StartIndexCnt  int64
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

func (self *NsqdCoordinator) checkWriteForRpcCall(rpcData RpcTopicData) (*TopicCoordinator, *CoordErr) {
	topicCoord, err := self.getTopicCoord(rpcData.TopicName, rpcData.TopicPartition)
	if err != nil || topicCoord == nil {
		coordLog.Infof("rpc call with missing topic :%v", rpcData)
		return nil, ErrMissingTopicCoord
	}
	tcData := topicCoord.GetData()
	if tcData.GetTopicEpochForWrite() != rpcData.TopicWriteEpoch {
		coordLog.Infof("rpc call with wrong epoch :%v, current: %v", rpcData, tcData.GetTopicEpochForWrite())
		return nil, ErrEpochMismatch
	}
	if tcData.GetLeaderSession() != rpcData.TopicLeaderSession {
		coordLog.Infof("rpc call with wrong session:%v, local: %v", rpcData, tcData.GetLeaderSession())
		return nil, ErrLeaderSessionMismatch
	}
	//if !tcData.localDataLoaded {
	//	coordLog.Infof("local data is still loading. %v", tcData.topicInfo.GetTopicDesp())
	//	return nil, ErrTopicLoading
	//}
	return topicCoord, nil
}

func (self *NsqdCoordRpcServer) UpdateChannelOffset(info *RpcChannelOffsetArg) *CoordErr {
	tc, err := self.nsqdCoord.checkWriteForRpcCall(info.RpcTopicData)
	if err != nil {
		return err
	}
	// update local channel offset
	return self.nsqdCoord.updateChannelOffsetOnSlave(tc.GetData(), info.Channel, info.ChannelOffset)
}

// receive from leader
func (self *NsqdCoordRpcServer) PutMessage(info *RpcPutMessage) *CoordErr {
	if coordLog.Level() >= levellogger.LOG_DEBUG {
		s := time.Now().Unix()
		defer func() {
			e := time.Now().Unix()
			if e-s > int64(RPC_TIMEOUT/2) {
				coordLog.Infof("PutMessage rpc call used: %v", e-s)
			}
		}()
	}

	tc, err := self.nsqdCoord.checkWriteForRpcCall(info.RpcTopicData)
	if err != nil {
		return err
	}
	// do local pub message
	return self.nsqdCoord.putMessageOnSlave(tc, info.LogData, info.TopicMessage)
}

func (self *NsqdCoordRpcServer) PutMessages(info *RpcPutMessages) *CoordErr {
	if coordLog.Level() >= levellogger.LOG_DEBUG {
		s := time.Now().Unix()
		defer func() {
			e := time.Now().Unix()
			if e-s > int64(RPC_TIMEOUT/2) {
				coordLog.Infof("PutMessages rpc call used: %v", e-s)
			}
		}()
	}

	tc, err := self.nsqdCoord.checkWriteForRpcCall(info.RpcTopicData)
	if err != nil {
		return err
	}
	// do local pub message
	return self.nsqdCoord.putMessagesOnSlave(tc, info.LogData, info.TopicMessages)
}

func (self *NsqdCoordRpcServer) GetLastCommitLogID(req *RpcCommitLogReq) (int64, error) {
	var ret int64
	tc, err := self.nsqdCoord.getTopicCoordData(req.TopicName, req.TopicPartition)
	if err != nil {
		return ret, err
	}
	ret = tc.logMgr.GetLastCommitLogID()
	return ret, nil
}

// return the logdata from offset, if the offset is larger than local,
// then return the last logdata on local.
func (self *NsqdCoordRpcServer) GetCommitLogFromOffset(req *RpcCommitLogReq) *RpcCommitLogRsp {
	var ret RpcCommitLogRsp
	tcData, coorderr := self.nsqdCoord.getTopicCoordData(req.TopicName, req.TopicPartition)
	if coorderr != nil {
		ret.ErrInfo = *coorderr
		return &ret
	}
	logData, err := tcData.logMgr.GetCommitLogFromOffsetV2(req.LogStartIndex, req.LogOffset)
	if err != nil {
		var err2 error
		ret.LogStartIndex, ret.LogOffset, logData, err2 = tcData.logMgr.GetLastCommitLogOffsetV2()
		if err2 != nil {
			if err2 == ErrCommitLogEOF {
				ret.ErrInfo = *ErrTopicCommitLogEOF
			} else {
				ret.ErrInfo = *NewCoordErr(err.Error(), CoordCommonErr)
			}
			return &ret
		}
		ret.LogData = *logData
		if err == ErrCommitLogOutofBound {
			ret.ErrInfo = *ErrTopicCommitLogOutofBound
		} else if err == ErrCommitLogEOF {
			ret.ErrInfo = *ErrTopicCommitLogEOF
		} else {
			ret.ErrInfo = *NewCoordErr(err.Error(), CoordCommonErr)
		}
		return &ret
	} else {
		ret.LogStartIndex = req.LogStartIndex
		ret.LogOffset = req.LogOffset
		ret.LogData = *logData
	}
	return &ret
}

func (self *NsqdCoordRpcServer) PullCommitLogsAndData(req *RpcPullCommitLogsReq) (*RpcPullCommitLogsRsp, error) {
	var ret RpcPullCommitLogsRsp
	tcData, err := self.nsqdCoord.getTopicCoordData(req.TopicName, req.TopicPartition)
	if err != nil {
		return nil, err
	}

	var localErr error
	ret.Logs, localErr = tcData.logMgr.GetCommitLogsV2(req.StartIndexCnt, req.StartLogOffset, req.LogMaxNum)
	if localErr != nil {
		if localErr != ErrCommitLogEOF {
			return nil, localErr
		}
	}
	offsetList := make([]int64, len(ret.Logs))
	sizeList := make([]int32, len(ret.Logs))
	totalSize := int32(0)
	for i, l := range ret.Logs {
		offsetList[i] = l.MsgOffset
		sizeList[i] = l.MsgSize
		totalSize += l.MsgSize
		// note: this should be large than the max message body size
		if totalSize > MAX_LOG_PULL_BYTES {
			coordLog.Warningf("pulling too much log data at one time: %v, %v", totalSize, i)
			offsetList = offsetList[:i]
			sizeList = sizeList[:i]
			break
		}
	}

	ret.DataList, err = self.nsqdCoord.readTopicRawData(tcData.topicInfo.Name, tcData.topicInfo.Partition, offsetList, sizeList)
	ret.Logs = ret.Logs[:len(ret.DataList)]
	if err != nil {
		return nil, err
	}
	return &ret, nil
}

func (self *NsqdCoordRpcServer) TestRpcError(req *RpcTestReq) *RpcTestRsp {
	var ret RpcTestRsp
	ret.RspData = req.Data
	ret.RetErr = &CoordErr{
		ErrMsg:  req.Data,
		ErrCode: RpcNoErr,
		ErrType: CoordCommonErr,
	}
	return &ret
}

func (self *NsqdCoordRpcServer) TestRpcTimeout() error {
	time.Sleep(time.Second)
	return nil
}

type CoordStats struct {
	RpcStats gorpc.ConnStats
}
