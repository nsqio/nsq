package consistence

import (
	"github.com/absolute8511/gorpc"
	"github.com/absolute8511/nsq/internal/levellogger"
	"github.com/absolute8511/nsq/nsqd"
	"net"
	"runtime"
	"strconv"
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
	RpcErrCommitLogLessThanSegmentStart
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

type RpcReleaseTopicLeaderReq struct {
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

func (self *NsqdCoordRpcServer) start(ip, port string) (string, error) {
	self.rpcDispatcher.AddService("NsqdCoordRpcServer", self)
	self.rpcServer = gorpc.NewTCPServer(net.JoinHostPort(ip, port), self.rpcDispatcher.NewHandlerFunc())

	e := self.rpcServer.Start()
	if e != nil {
		coordLog.Errorf("start rpc server error : %v", e)
		panic(e)
	}

	coordLog.Infof("nsqd coordinator rpc listen at : %v", self.rpcServer.Listener.ListenAddr())
	return self.rpcServer.Listener.ListenAddr().String(), nil
}

func (self *NsqdCoordRpcServer) stop() {
	if self.rpcServer != nil {
		self.rpcServer.Stop()
	}
}

func (self *NsqdCoordRpcServer) checkLookupForWrite(lookupEpoch EpochType) *CoordErr {
	if lookupEpoch < self.nsqdCoord.lookupLeader.Epoch {
		coordLog.Warningf("the lookupd epoch is smaller than last: %v", lookupEpoch)
		coordErrStats.incRpcCheckFailed()
		return ErrEpochMismatch
	}
	if self.disableRpcTest {
		return &CoordErr{"rpc disabled", RpcCommonErr, CoordNetErr}
	}
	return nil
}

func (self *NsqdCoordRpcServer) NotifyReleaseTopicLeader(rpcTopicReq *RpcReleaseTopicLeaderReq) *CoordErr {
	var ret CoordErr
	if err := self.checkLookupForWrite(rpcTopicReq.LookupdEpoch); err != nil {
		ret = *err
		return &ret
	}
	coordLog.Infof("got release leader session notify : %v, on node: %v", rpcTopicReq, self.nsqdCoord.myNode.GetID())
	topicCoord, err := self.nsqdCoord.getTopicCoord(rpcTopicReq.TopicName, rpcTopicReq.TopicPartition)
	if err != nil {
		coordLog.Infof("topic partition missing.")
		ret = *err
		return &ret
	}
	coordData := topicCoord.GetData()
	if coordData.GetLeaderSessionID() != "" && coordData.GetLeader() != coordData.GetLeaderSessionID() {
		// check if any old leader session acquired by mine is not released
		if self.nsqdCoord.GetMyID() == coordData.GetLeaderSessionID() {
			coordLog.Warningf("old leader session acquired by me is not released, my leader should release: %v", coordData)
			self.nsqdCoord.releaseTopicLeader(&coordData.topicInfo, &coordData.topicLeaderSession)
			return &ret
		}
	}
	if !topicCoord.IsWriteDisabled() {
		coordLog.Errorf("topic %v release leader should disable write first", coordData.topicInfo.GetTopicDesp())
		ret = *ErrTopicCoordStateInvalid
		return &ret
	}
	if rpcTopicReq.Epoch < coordData.topicInfo.Epoch ||
		coordData.topicLeaderSession.LeaderEpoch != rpcTopicReq.TopicLeaderSessionEpoch {
		coordLog.Warningf("topic info epoch mismatch while release leader: %v, %v", coordData,
			rpcTopicReq)
		ret = *ErrEpochMismatch
		return &ret
	}
	if coordData.GetLeader() == self.nsqdCoord.myNode.GetID() {
		coordLog.Infof("my leader should release: %v", coordData)
		self.nsqdCoord.releaseTopicLeader(&coordData.topicInfo, &coordData.topicLeaderSession)
	} else {
		coordLog.Infof("Leader is not me while release: %v", coordData)
	}
	return &ret
}

func (self *NsqdCoordRpcServer) NotifyTopicLeaderSession(rpcTopicReq *RpcTopicLeaderSession) *CoordErr {
	s := time.Now().Unix()
	var ret CoordErr
	defer func() {
		coordErrStats.incCoordErr(&ret)
		e := time.Now().Unix()
		if e-s > int64(RPC_TIMEOUT/2) {
			coordLog.Infof("rpc call used: %v", e-s)
		}
	}()

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
	if !topicCoord.IsWriteDisabled() {
		if rpcTopicReq.JoinSession != "" {
			if FindSlice(topicCoord.topicInfo.ISR, self.nsqdCoord.myNode.GetID()) != -1 {
				coordLog.Errorf("join session should disable write first")
				ret = *ErrTopicCoordStateInvalid
				return &ret
			}
		}
		if rpcTopicReq.LeaderNode.ID != topicCoord.topicInfo.Leader {
			// Not allow to change the leader session to another during write
			// but we can be notified to know the lost leader session
			if rpcTopicReq.TopicLeaderSession != "" {
				coordLog.Errorf("change leader session to another should disable write first")
				ret = *ErrTopicCoordStateInvalid
				return &ret
			}
		}
	}
	newSession := &TopicLeaderSession{
		Topic:       rpcTopicReq.TopicName,
		Partition:   rpcTopicReq.TopicPartition,
		LeaderNode:  &rpcTopicReq.LeaderNode,
		Session:     rpcTopicReq.TopicLeaderSession,
		LeaderEpoch: rpcTopicReq.TopicLeaderSessionEpoch,
	}
	err = self.nsqdCoord.updateTopicLeaderSession(topicCoord, newSession, rpcTopicReq.JoinSession)
	if err != nil {
		ret = *err
	}
	return &ret
}

func (self *NsqdCoordRpcServer) NotifyAcquireTopicLeader(rpcTopicReq *RpcAcquireTopicLeaderReq) *CoordErr {
	s := time.Now().Unix()
	var ret CoordErr
	defer func() {
		coordErrStats.incCoordErr(&ret)
		e := time.Now().Unix()
		if e-s > int64(RPC_TIMEOUT/2) {
			coordLog.Infof("rpc call used: %v", e-s)
		}
	}()

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
		return &ret
	}
	err = self.nsqdCoord.notifyAcquireTopicLeader(tcData)
	if err != nil {
		ret = *err
		return &ret
	}
	return &ret

}

func (self *NsqdCoordRpcServer) UpdateTopicInfo(rpcTopicReq *RpcAdminTopicInfo) *CoordErr {
	s := time.Now().Unix()
	var ret CoordErr
	defer func() {
		coordErrStats.incCoordErr(&ret)
		e := time.Now().Unix()
		if e-s > int64(RPC_TIMEOUT/2) {
			coordLog.Infof("UpdateTopicInfo rpc call used: %v", e-s)
		}
	}()

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
		return &ret
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
	err := self.nsqdCoord.updateTopicInfo(tpCoord, rpcTopicReq.DisableWrite, &rpcTopicReq.TopicPartitionMetaInfo)
	if err != nil {
		ret = *err
		return &ret
	}
	return &ret
}

func (self *NsqdCoordRpcServer) EnableTopicWrite(rpcTopicReq *RpcAdminTopicInfo) *CoordErr {
	s := time.Now().Unix()
	var ret CoordErr
	defer func() {
		coordErrStats.incCoordErr(&ret)
		e := time.Now().Unix()
		if e-s > int64(RPC_TIMEOUT/2) {
			coordLog.Infof("rpc call used: %v", e-s)
		}
	}()

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
				self.nsqdCoord.switchStateForMaster(tp, topicData, true)
			}
		}
	}
	tp.writeHold.Unlock()

	if err != nil {
		ret = *err
		return &ret
	}
	return &ret
}

func (self *NsqdCoordRpcServer) DisableTopicWrite(rpcTopicReq *RpcAdminTopicInfo) *CoordErr {
	s := time.Now().Unix()
	var ret CoordErr
	defer func() {
		coordErrStats.incCoordErr(&ret)
		e := time.Now().Unix()
		if e-s > int64(RPC_TIMEOUT/2) {
			coordLog.Infof("rpc call used: %v", e-s)
		}
	}()

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
			self.nsqdCoord.switchStateForMaster(tp, localTopic, false)
		}
	}
	tp.writeHold.Unlock()
	if err != nil {
		ret = *err
		return &ret
	}
	return &ret
}

func (self *NsqdCoordRpcServer) IsTopicWriteDisabled(rpcTopicReq *RpcAdminTopicInfo) bool {
	tp, err := self.nsqdCoord.getTopicCoord(rpcTopicReq.Name, rpcTopicReq.Partition)
	if err != nil {
		return false
	}
	if tp.IsWriteDisabled() {
		return true
	}
	localTopic, localErr := self.nsqdCoord.localNsqd.GetExistingTopic(rpcTopicReq.Name, rpcTopicReq.Partition)
	if localErr != nil {
		coordLog.Infof("no topic on local: %v, %v", rpcTopicReq.Name, localErr)
		return true
	}
	return localTopic.IsWriteDisabled()
}

func (self *NsqdCoordRpcServer) DeleteNsqdTopic(rpcTopicReq *RpcAdminTopicInfo) *CoordErr {
	var ret CoordErr
	defer coordErrStats.incCoordErr(&ret)
	if err := self.checkLookupForWrite(rpcTopicReq.LookupdEpoch); err != nil {
		ret = *err
		return &ret
	}

	coordLog.Infof("removing the local topic: %v", rpcTopicReq)
	_, err := self.nsqdCoord.removeTopicCoord(rpcTopicReq.Name, rpcTopicReq.Partition, true)
	if err != nil {
		ret = *err
		coordLog.Infof("delete topic %v failed : %v", rpcTopicReq.GetTopicDesp(), err)
	}
	return &ret
}

func (self *NsqdCoordRpcServer) GetTopicStats(topic string) *NodeTopicStats {
	s := time.Now().Unix()
	defer func() {
		e := time.Now().Unix()
		if e-s > int64(RPC_TIMEOUT/2) {
			coordLog.Infof("GetTopicStats rpc call used: %v", e-s)
		}
	}()

	var topicStats []nsqd.TopicStats
	if topic == "" {
		// all topic status
		topicStats = self.nsqdCoord.localNsqd.GetStats()
	} else {
		topicStats = self.nsqdCoord.localNsqd.GetTopicStats(topic)
	}
	stat := NewNodeTopicStats(self.nsqdCoord.myNode.GetID(), len(topicStats)*2, runtime.NumCPU())
	for _, ts := range topicStats {
		pid, _ := strconv.Atoi(ts.TopicPartition)
		// filter the catchup node
		coordData, coordErr := self.nsqdCoord.getTopicCoordData(ts.TopicName, pid)
		if coordErr != nil {
			continue
		}
		if !coordData.IsMineISR(self.nsqdCoord.myNode.GetID()) {
			continue
		}
		// plus 1 to handle the empty topic/channel
		stat.TopicTotalDataSize[ts.TopicFullName] += (ts.BackendDepth-ts.BackendStart)/1024/1024 + 1
		localTopic, err := self.nsqdCoord.localNsqd.GetExistingTopic(ts.TopicName, pid)
		if err != nil {
			coordLog.Infof("get local topic %v, %v failed: %v", ts.TopicFullName, pid, err)
		} else {
			pubhs := localTopic.GetDetailStats().GetHourlyStats()
			stat.TopicHourlyPubDataList[ts.TopicFullName] = pubhs
		}
		if ts.IsLeader {
			stat.TopicLeaderDataSize[ts.TopicFullName] += (ts.BackendDepth-ts.BackendStart)/1024/1024 + 1
			stat.ChannelNum[ts.TopicFullName] = len(ts.Channels)
			chList := stat.ChannelList[ts.TopicFullName]
			for _, chStat := range ts.Channels {
				stat.ChannelDepthData[ts.TopicFullName] += chStat.DepthSize/1024/1024 + 1
				chList = append(chList, chStat.ChannelName)
			}
			stat.ChannelList[ts.TopicFullName] = chList
		}
	}
	// the status of specific topic
	return stat
}

type RpcTopicData struct {
	TopicName               string
	TopicPartition          int
	Epoch                   EpochType
	TopicWriteEpoch         EpochType
	TopicLeaderSessionEpoch EpochType
	TopicLeaderSession      string
	TopicLeader             string
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
	LogOffset        int64
	LogStartIndex    int64
	LogCountNumIndex int64
	UseCountIndex    bool
}

type RpcCommitLogRsp struct {
	LogStartIndex    int64
	LogOffset        int64
	LogData          CommitLogData
	ErrInfo          CoordErr
	LogCountNumIndex int64
	UseCountIndex    bool
}

type RpcPullCommitLogsReq struct {
	RpcTopicData
	StartLogOffset   int64
	LogMaxNum        int
	StartIndexCnt    int64
	LogCountNumIndex int64
	UseCountIndex    bool
}

type RpcPullCommitLogsRsp struct {
	Logs     []CommitLogData
	DataList [][]byte
}

type RpcGetFullSyncInfoReq struct {
	RpcTopicData
}

type RpcGetFullSyncInfoRsp struct {
	FirstLogData CommitLogData
	StartInfo    LogStartInfo
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
		return nil, err
	}
	tcData := topicCoord.GetData()
	if tcData.GetTopicEpochForWrite() != rpcData.TopicWriteEpoch {
		coordLog.Infof("rpc call with wrong epoch :%v, current: %v", rpcData, tcData.GetTopicEpochForWrite())
		self.requestNotifyNewTopicInfo(rpcData.TopicName, rpcData.TopicPartition)
		coordErrStats.incRpcCheckFailed()
		return nil, ErrEpochMismatch
	}
	if rpcData.TopicLeader != "" && tcData.GetLeader() != rpcData.TopicLeader {
		coordLog.Warningf("rpc call with wrong leader:%v, local: %v", rpcData, tcData.GetLeader())
		self.requestNotifyNewTopicInfo(rpcData.TopicName, rpcData.TopicPartition)
		coordErrStats.incRpcCheckFailed()
		return nil, ErrNotTopicLeader
	}
	if rpcData.TopicLeaderSession != tcData.GetLeaderSession() {
		coordLog.Warningf("call write with mismatch session: %v, loca %v", rpcData, tcData.GetLeaderSession())
	}
	return topicCoord, nil
}

func (self *NsqdCoordRpcServer) UpdateChannelOffset(info *RpcChannelOffsetArg) *CoordErr {
	var ret CoordErr
	defer coordErrStats.incCoordErr(&ret)
	tc, err := self.nsqdCoord.checkWriteForRpcCall(info.RpcTopicData)
	if err != nil {
		ret = *err
		return &ret
	}
	// update local channel offset
	err = self.nsqdCoord.updateChannelOffsetOnSlave(tc.GetData(), info.Channel, info.ChannelOffset)
	if err != nil {
		ret = *err
		return &ret
	}
	return &ret
}

func (self *NsqdCoordRpcServer) DeleteChannel(info *RpcChannelOffsetArg) *CoordErr {
	var ret CoordErr
	defer coordErrStats.incCoordErr(&ret)
	tc, err := self.nsqdCoord.checkWriteForRpcCall(info.RpcTopicData)
	if err != nil {
		ret = *err
		return &ret
	}
	// update local channel offset
	err = self.nsqdCoord.deleteChannelOnSlave(tc.GetData(), info.Channel)
	if err != nil {
		ret = *err
		return &ret
	}
	return &ret
}

// receive from leader
func (self *NsqdCoordRpcServer) PutMessage(info *RpcPutMessage) *CoordErr {
	if self.nsqdCoord.enableBenchCost || coordLog.Level() >= levellogger.LOG_DEBUG {
		s := time.Now()
		defer func() {
			e := time.Now()
			if e.Sub(s) > time.Second*time.Duration(RPC_TIMEOUT/2) {
				coordLog.Infof("PutMessage rpc call used: %v, start: %v, end: %v", e.Sub(s), s, e)
			}
			coordLog.Warningf("PutMessage rpc call used: %v, start: %v, end: %v", e.Sub(s), s, e)
		}()
	}

	var ret CoordErr
	defer coordErrStats.incCoordErr(&ret)
	tc, err := self.nsqdCoord.checkWriteForRpcCall(info.RpcTopicData)
	if err != nil {
		ret = *err
		return &ret
	}
	// do local pub message
	err = self.nsqdCoord.putMessageOnSlave(tc, info.LogData, info.TopicMessage)
	if err != nil {
		ret = *err
		return &ret
	}
	return &ret
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

	var ret CoordErr
	defer coordErrStats.incCoordErr(&ret)
	tc, err := self.nsqdCoord.checkWriteForRpcCall(info.RpcTopicData)
	if err != nil {
		ret = *err
		return &ret
	}
	// do local pub message
	err = self.nsqdCoord.putMessagesOnSlave(tc, info.LogData, info.TopicMessages)
	if err != nil {
		ret = *err
		return &ret
	}
	return &ret
}

func (self *NsqdCoordRpcServer) GetLastCommitLogID(req *RpcCommitLogReq) (int64, error) {
	var ret int64
	tc, err := self.nsqdCoord.getTopicCoordData(req.TopicName, req.TopicPartition)
	if err != nil {
		return ret, err.ToErrorType()
	}
	ret = tc.logMgr.GetLastCommitLogID()
	return ret, nil
}

func handleCommitLogError(err error, logMgr *TopicCommitLogMgr, req *RpcCommitLogReq, ret *RpcCommitLogRsp) {
	var err2 error
	var logData *CommitLogData
	ret.LogStartIndex, ret.LogOffset, logData, err2 = logMgr.GetLastCommitLogOffsetV2()
	// if last err is ErrCommitLogEOF or OutOfBound, we need to check the log end again,
	// because the log end maybe changed. and if the second new log end is large than request data,
	// it means the log is changed during this
	// return ErrCommitLogEOF or OutOfBound means the leader has less log data than the others.
	if err2 != nil {
		if err2 == ErrCommitLogEOF {
			if err == ErrCommitLogEOF {
				ret.ErrInfo = *ErrTopicCommitLogEOF
			} else if err == ErrCommitLogLessThanSegmentStart {
				ret.ErrInfo = *ErrTopicCommitLogLessThanSegmentStart
			}
		} else {
			ret.ErrInfo = *NewCoordErr(err.Error(), CoordCommonErr)
			return
		}
	}
	ret.LogCountNumIndex, _ = logMgr.ConvertToCountIndex(ret.LogStartIndex, ret.LogOffset)
	ret.UseCountIndex = true
	if logData != nil {
		ret.LogData = *logData
	}
	if err == ErrCommitLogEOF || err == ErrCommitLogOutofBound {
		if req.UseCountIndex {
			if ret.LogCountNumIndex > req.LogCountNumIndex {
				ret.ErrInfo = *NewCoordErr("commit log changed", CoordCommonErr)
				return
			}
		} else {
			if ret.LogStartIndex > req.LogStartIndex ||
				(ret.LogStartIndex == req.LogStartIndex && ret.LogOffset > req.LogOffset) {
				ret.ErrInfo = *NewCoordErr("commit log changed", CoordCommonErr)
				return
			}
		}
	}
	if err2 == ErrCommitLogEOF && err == ErrCommitLogEOF {
		ret.ErrInfo = *ErrTopicCommitLogEOF
	} else if err == ErrCommitLogOutofBound {
		ret.ErrInfo = *ErrTopicCommitLogOutofBound
	} else if err == ErrCommitLogEOF {
		ret.ErrInfo = *ErrTopicCommitLogEOF
	} else if err == ErrCommitLogLessThanSegmentStart {
		ret.ErrInfo = *ErrTopicCommitLogLessThanSegmentStart
	} else {
		ret.ErrInfo = *NewCoordErr(err.Error(), CoordCommonErr)
	}
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
	var err error
	if req.UseCountIndex {
		newFileNum, newOffset, err := tcData.logMgr.ConvertToOffsetIndex(req.LogCountNumIndex)
		if err != nil {
			coordLog.Warningf("failed to convert to offset index: %v, err:%v", req.LogCountNumIndex, err)
			handleCommitLogError(err, tcData.logMgr, req, &ret)
			return &ret
		}
		req.LogStartIndex = newFileNum
		req.LogOffset = newOffset
	}

	logData, err := tcData.logMgr.GetCommitLogFromOffsetV2(req.LogStartIndex, req.LogOffset)
	if err != nil {
		handleCommitLogError(err, tcData.logMgr, req, &ret)
		return &ret
	} else {
		ret.LogStartIndex = req.LogStartIndex
		ret.LogOffset = req.LogOffset
		ret.LogData = *logData
		ret.LogCountNumIndex, _ = tcData.logMgr.ConvertToCountIndex(ret.LogStartIndex, ret.LogOffset)
	}
	return &ret
}

func (self *NsqdCoordRpcServer) PullCommitLogsAndData(req *RpcPullCommitLogsReq) (*RpcPullCommitLogsRsp, error) {
	var ret RpcPullCommitLogsRsp
	tcData, err := self.nsqdCoord.getTopicCoordData(req.TopicName, req.TopicPartition)
	if err != nil {
		return nil, err.ToErrorType()
	}

	var localErr error
	if req.UseCountIndex {
		newFileNum, newOffset, localErr := tcData.logMgr.ConvertToOffsetIndex(req.LogCountNumIndex)
		if localErr != nil {
			coordLog.Warningf("topic %v failed to convert to offset index: %v, err:%v", req.TopicName, req.LogCountNumIndex, localErr)
			if localErr != ErrCommitLogEOF {
				return nil, localErr
			}
		}
		req.StartIndexCnt = newFileNum
		req.StartLogOffset = newOffset
	}
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
		return nil, err.ToErrorType()
	}
	return &ret, nil
}

func (self *NsqdCoordRpcServer) GetFullSyncInfo(req *RpcGetFullSyncInfoReq) (*RpcGetFullSyncInfoRsp, error) {
	var ret RpcGetFullSyncInfoRsp
	tcData, err := self.nsqdCoord.getTopicCoordData(req.TopicName, req.TopicPartition)
	if err != nil {
		return nil, err.ToErrorType()
	}

	var localErr error
	startInfo, firstLog, localErr := tcData.logMgr.GetLogStartInfo()
	if localErr != nil {
		if localErr != ErrCommitLogEOF {
			return nil, localErr
		}
	}
	coordLog.Infof("topic %v get log start info : %v", tcData.topicInfo.GetTopicDesp(), startInfo)

	ret.StartInfo = *startInfo
	if firstLog != nil {
		ret.FirstLogData = *firstLog
	} else {
		// we need to get the disk queue end info
		localTopic, err := self.nsqdCoord.localNsqd.GetExistingTopic(req.TopicName, req.TopicPartition)
		if err != nil {
			return nil, err
		}
		ret.FirstLogData.MsgOffset = localTopic.TotalDataSize()
		ret.FirstLogData.MsgCnt = int64(localTopic.TotalMessageCnt())
	}
	return &ret, nil
}

func (self *NsqdCoordRpcServer) TriggerLookupChanged() error {
	self.nsqdCoord.localNsqd.TriggerOptsNotification()
	coordLog.Infof("got lookup changed trigger")
	return nil
}

func (self *NsqdCoordRpcServer) TestRpcCoordErr(req *RpcTestReq) *CoordErr {
	var ret CoordErr
	ret.ErrMsg = req.Data
	ret.ErrCode = RpcCommonErr
	ret.ErrType = CoordCommonErr
	return &ret
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
