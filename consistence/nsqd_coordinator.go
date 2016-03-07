package consistence

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"github.com/absolute8511/nsq/nsqd"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	MAX_WRITE_RETRY   = 10
	MAX_CATCHUP_RETRY = 5
)

type CoordErrType int

const (
	CoordNoErr CoordErrType = iota
	CoordCommonErr
	CoordNetErr
	CoordElectionErr
	CoordElectionTmpErr
	CoordClusterErr
	CoordLocalErr
	CoordTmpErr
)

type CoordErr struct {
	ErrMsg  string
	ErrCode ErrRPCRetCode
	ErrType CoordErrType
}

func NewCoordErr(msg string, etype CoordErrType) *CoordErr {
	return &CoordErr{
		ErrMsg:  msg,
		ErrType: etype,
		ErrCode: RpcCommonErr,
	}
}

func NewCoordErrWithCode(msg string, etype CoordErrType, code ErrRPCRetCode) *CoordErr {
	return &CoordErr{
		ErrMsg:  msg,
		ErrType: etype,
		ErrCode: code,
	}
}

func (self *CoordErr) Error() string {
	return self.ErrMsg
}

func (self *CoordErr) HasError() bool {
	if self.ErrType == CoordNoErr && self.ErrCode == RpcNoErr {
		return false
	}
	return true
}

func (self *CoordErr) IsEqual(other *CoordErr) bool {
	if other == nil || self == nil {
		return false
	}

	if self == other {
		return true
	}

	if other.ErrCode != self.ErrCode || other.ErrType != self.ErrType {
		return false
	}

	if other.ErrCode != RpcCommonErr {
		return true
	}
	// only common error need to check if errmsg is equal
	if other.ErrMsg == self.ErrMsg {
		return true
	}
	return false
}

func (self *CoordErr) IsNetErr() bool {
	return self.ErrType == CoordNetErr
}

func (self *CoordErr) CanRetry() bool {
	return self.ErrType == CoordTmpErr || self.ErrType == CoordElectionTmpErr
}

func (self *CoordErr) IsNeedCheckSync() bool {
	return self.ErrType == CoordElectionErr
}

var (
	ErrTopicInfoNotFound = NewCoordErr("topic info not found", CoordClusterErr)

	ErrNotTopicLeader                = NewCoordErrWithCode("not topic leader", CoordElectionErr, RpcErrNotTopicLeader)
	ErrEpochMismatch                 = NewCoordErrWithCode("commit epoch not match", CoordElectionErr, RpcErrEpochMismatch)
	ErrEpochLessThanCurrent          = NewCoordErrWithCode("epoch should be increased", CoordElectionErr, RpcErrEpochLessThanCurrent)
	ErrWriteQuorumFailed             = NewCoordErrWithCode("write to quorum failed.", CoordElectionTmpErr, RpcErrWriteQuorumFailed)
	ErrCommitLogIDDup                = NewCoordErrWithCode("commit id duplicated", CoordElectionErr, RpcErrCommitLogIDDup)
	ErrMissingTopicLeaderSession     = NewCoordErrWithCode("missing topic leader session", CoordElectionErr, RpcErrMissingTopicLeaderSession)
	ErrLeaderSessionMismatch         = NewCoordErrWithCode("leader session mismatch", CoordElectionErr, RpcErrLeaderSessionMismatch)
	ErrWriteDisabled                 = NewCoordErrWithCode("write is disabled on the topic", CoordElectionTmpErr, RpcErrWriteDisabled)
	ErrLeavingISRWait                = NewCoordErrWithCode("leaving isr need wait.", CoordElectionTmpErr, RpcErrLeavingISRWait)
	ErrTopicCoordExistingAndMismatch = NewCoordErrWithCode("topic coordinator existing with a different partition", CoordClusterErr, RpcErrTopicCoordExistingAndMismatch)
	ErrTopicLeaderChanged            = NewCoordErrWithCode("topic leader changed", CoordElectionTmpErr, RpcErrTopicLeaderChanged)
	ErrTopicCommitLogEOF             = NewCoordErrWithCode("topic commit log end of file", CoordCommonErr, RpcErrCommitLogEOF)
	ErrTopicCommitLogOutofBound      = NewCoordErrWithCode("topic commit log offset out of bound", CoordCommonErr, RpcErrCommitLogOutofBound)
	ErrMissingTopicCoord             = NewCoordErrWithCode("missing topic coordinator", CoordClusterErr, RpcErrMissingTopicCoord)
	ErrTopicLoading                  = NewCoordErrWithCode("topic is still loading data", CoordLocalErr, RpcErrTopicLoading)

	ErrPubArgError     = NewCoordErr("pub argument error", CoordCommonErr)
	ErrTopicNotRelated = NewCoordErr("topic not related to me", CoordCommonErr)

	ErrMissingTopicLog             = NewCoordErr("missing topic log ", CoordLocalErr)
	ErrLocalTopicPartitionMismatch = NewCoordErr("local topic partition not match", CoordLocalErr)
	ErrLocalFallBehind             = NewCoordErr("local data fall behind", CoordElectionErr)
	ErrLocalForwardThanLeader      = NewCoordErr("local data is more than leader", CoordElectionErr)
	ErrLocalWriteFailed            = NewCoordErr("write data to local failed", CoordLocalErr)
	ErrLocalMissingTopic           = NewCoordErr("local topic missing", CoordLocalErr)
	ErrLocalNotReadyForWrite       = NewCoordErr("local topic is not ready for write.", CoordLocalErr)
	ErrLocalGetTopicFailed         = NewCoordErr("local topic init failed", CoordLocalErr)
	ErrLocalInitTopicCoordFailed   = NewCoordErr("topic coordinator init failed", CoordLocalErr)
	ErrLocalTopicDataCorrupt       = NewCoordErr("local topic data corrupt", CoordLocalErr)
)

func GetTopicPartitionFileName(topic string, partition int, suffix string) string {
	var tmpbuf bytes.Buffer
	tmpbuf.WriteString(topic)
	tmpbuf.WriteString("_")
	tmpbuf.WriteString(strconv.Itoa(partition))
	tmpbuf.WriteString(suffix)
	return tmpbuf.String()
}

func GetTopicPartitionBasePath(rootPath string, topic string, partition int) string {
	return filepath.Join(rootPath, topic)
}

func GenNsqdNodeID(n *NsqdNodeInfo, extra string) string {
	var tmpbuf bytes.Buffer
	tmpbuf.WriteString(n.NodeIp)
	tmpbuf.WriteString(":")
	tmpbuf.WriteString(n.RpcPort)
	tmpbuf.WriteString(":")
	tmpbuf.WriteString(n.TcpPort)
	tmpbuf.WriteString(":")
	tmpbuf.WriteString(extra)
	return tmpbuf.String()
}

func ExtractRpcAddrFromID(nid string) string {
	pos1 := strings.Index(nid, ":")
	pos2 := strings.Index(nid[pos1+1:], ":")
	return nid[:pos1+pos2+1]
}

type TopicPartitionID struct {
	TopicName      string
	TopicPartition int
}

func DecodeMessagesFromRaw(bufReader *bufio.Reader, msgs []*nsqd.Message, tmpbuf []byte) ([]*nsqd.Message, error) {
	var msgSize int32
	buf := tmpbuf[:0]
	msgs = msgs[:0]
	for {
		err := binary.Read(bufReader, binary.BigEndian, &msgSize)
		if err != nil {
			if err == io.EOF {
				return msgs, nil
			}
			return nil, err
		}
		_, err = io.ReadFull(bufReader, buf)
		msg, err := nsqd.DecodeMessage(buf)
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, msg)
	}
	return msgs, nil
}

type NsqdCoordinator struct {
	leadership             NSQDLeadership
	lookupLeader           *NsqLookupdNodeInfo
	lookupRemoteCreateFunc nsqlookupRemoteProxyCreateFunc
	topicCoords            map[string]map[int]*TopicCoordinator
	coordMutex             sync.RWMutex
	myNode                 NsqdNodeInfo
	nsqdRpcClients         map[string]*NsqdRpcClient
	flushNotifyChan        chan TopicPartitionID
	stopChan               chan struct{}
	dataRootPath           string
	localDataStates        map[string]map[int]bool
	localNsqd              *nsqd.NSQD
	rpcServer              *NsqdCoordRpcServer
	tryCheckUnsynced       chan bool
}

func NewNsqdCoordinator(ip, tcpport, rpcport, extraID string, rootPath string, nsqd *nsqd.NSQD) *NsqdCoordinator {
	nodeInfo := NsqdNodeInfo{
		NodeIp:  ip,
		TcpPort: tcpport,
		RpcPort: rpcport,
	}
	nodeInfo.ID = GenNsqdNodeID(&nodeInfo, extraID)
	nsqdCoord := &NsqdCoordinator{
		leadership:             nil,
		topicCoords:            make(map[string]map[int]*TopicCoordinator),
		myNode:                 nodeInfo,
		nsqdRpcClients:         make(map[string]*NsqdRpcClient),
		flushNotifyChan:        make(chan TopicPartitionID, 2),
		stopChan:               make(chan struct{}),
		dataRootPath:           rootPath,
		localDataStates:        make(map[string]map[int]bool),
		localNsqd:              nsqd,
		tryCheckUnsynced:       make(chan bool, 1),
		lookupRemoteCreateFunc: NewNsqLookupRpcClient,
	}

	nsqdCoord.rpcServer = NewNsqdCoordRpcServer(nsqdCoord, rootPath)
	return nsqdCoord
}

func (self *NsqdCoordinator) acquireRpcClient(nid string) (*NsqdRpcClient, *CoordErr) {
	c, ok := self.nsqdRpcClients[nid]
	var err error
	if !ok {
		addr := ExtractRpcAddrFromID(nid)
		c, err = NewNsqdRpcClient(addr, RPC_TIMEOUT_SHORT)
		if err != nil {
			return nil, NewCoordErr(err.Error(), CoordNetErr)
		}
		self.nsqdRpcClients[nid] = c
	}
	return c, nil
}

func (self *NsqdCoordinator) Start() error {
	err := self.loadLocalTopicData()
	if err != nil {
		return err
	}
	go self.watchNsqLookupd()
	go self.checkForUnsyncedTopics()
	// for each topic, wait other replicas and sync data with leader,
	// begin accept client request.
	go self.rpcServer.start(self.myNode.NodeIp, self.myNode.RpcPort)
	return nil
}

func (self *NsqdCoordinator) Stop() {
	// give up the leadership on the topic to
	// allow other isr take over to avoid electing.
	close(self.stopChan)
	self.rpcServer.stop()
	self.rpcServer = nil
}

func (self *NsqdCoordinator) getLookupRemoteProxy() (INsqlookupRemoteProxy, *CoordErr) {
	c, err := self.lookupRemoteCreateFunc(net.JoinHostPort(self.lookupLeader.NodeIp, self.lookupLeader.RpcPort), time.Second)
	if err == nil {
		return c, nil
	}
	return c, NewCoordErr(err.Error(), CoordNetErr)
}

func (self *NsqdCoordinator) watchNsqLookupd() {
	// watch the leader of nsqlookupd, always check the leader before response
	// to the nsqlookup admin operation.
	nsqlookupLeaderChan := make(chan *NsqLookupdNodeInfo, 1)
	go self.leadership.WatchLookupdLeader("nsqlookup-leader", nsqlookupLeaderChan, self.stopChan)
	for {
		select {
		case <-self.stopChan:
			return
		case n, ok := <-nsqlookupLeaderChan:
			if !ok {
				return
			}
			if n.GetID() != self.lookupLeader.GetID() ||
				n.Epoch != self.lookupLeader.Epoch {
				coordLog.Infof("nsqlookup leader changed: %v", n)
				self.lookupLeader = n
			}
		}
	}
}

func (self *NsqdCoordinator) markLocalDataState(topicName string, partition int, loaded bool) {
	states, ok := self.localDataStates[topicName]
	if !ok {
		states := make(map[int]bool)
		self.localDataStates[topicName] = states
	}
	states[partition] = loaded
}

func (self *NsqdCoordinator) loadLocalTopicData() error {
	if self.localNsqd == nil {
		return nil
	}
	self.localNsqd.RLock()
	topicMap := make(map[string]*nsqd.Topic)
	for name, t := range self.localNsqd.GetTopicMapRef() {
		topicMap[name] = t
	}
	self.localNsqd.RUnlock()

	for topicName, topic := range topicMap {
		partition := topic.GetTopicPart()
		if tc, err := self.getTopicCoord(topicName, partition); err == nil && tc != nil {
			// already loaded
			if tc.topicLeaderSession.LeaderNode == nil || tc.topicLeaderSession.Session == "" {
				if tc.topicInfo.Leader == self.myNode.GetID() {
					err := self.acquireTopicLeader(&tc.topicInfo)
					if err != nil {
						coordLog.Warningf("failed to acquire leader : %v", err)
					}
				} else if FindSlice(tc.topicInfo.ISR, self.myNode.GetID()) != -1 {
					// this will trigger the lookup send the current leadership to me
					self.notifyReadyForTopicISR(&tc.topicInfo, &tc.topicLeaderSession)
				}
			}
			continue
		}
		coordLog.Infof("loading topic: %v-%v", topicName, partition)
		if _, err := os.Stat(GetTopicPartitionLogPath(GetTopicPartitionBasePath(self.dataRootPath, topicName, partition), topicName, partition)); os.IsNotExist(err) {
			coordLog.Warningf("no commit log file under topic: %v-%v", topicName, partition)
			return ErrLocalTopicDataCorrupt
		}
		topicInfo, err := self.leadership.GetTopicInfo(topicName, partition)
		if err != nil {
			coordLog.Infof("failed to get topic info:%v-%v, err:%v", topicName, partition, err)
			if err == ErrTopicInfoNotFound {
				self.localNsqd.CloseExistingTopic(topicName, partition)
			}
			continue
		}
		shouldLoad := false
		if topicInfo.Leader == self.myNode.GetID() {
			coordLog.Infof("topic starting as leader.")
			err := self.acquireTopicLeader(topicInfo)
			if err != nil {
				coordLog.Warningf("failed to acquire leader while start as leader: %v", err)
			}
			shouldLoad = true
		} else if FindSlice(topicInfo.ISR, self.myNode.GetID()) != -1 {
			coordLog.Infof("topic starting as isr .")
			err := self.notifyReadyForTopicISR(topicInfo, nil)
			if err != nil {
				coordLog.Warningf("failed to notify ready for isr: %v", err)
			}
			shouldLoad = true
		} else if FindSlice(topicInfo.CatchupList, self.myNode.GetID()) != -1 {
			coordLog.Infof("topic starting as catchup")
			shouldLoad = true
		} else {
			coordLog.Infof("topic starting as not relevant")
			if len(topicInfo.ISR) >= topicInfo.Replica {
				coordLog.Infof("no need load the local topic since the replica is enough: %v-%v", topicName, partition)
				self.localNsqd.CloseExistingTopic(topicName, partition)
			} else if len(topicInfo.ISR)+len(topicInfo.CatchupList) < topicInfo.Replica {
				self.requestJoinCatchup(topicName, partition)
			}
		}
		if shouldLoad {
			self.coordMutex.Lock()
			coords, ok := self.topicCoords[topicInfo.Name]
			if !ok {
				coords = make(map[int]*TopicCoordinator)
				self.topicCoords[topicInfo.Name] = coords
			}
			basepath := GetTopicPartitionBasePath(self.dataRootPath, topicInfo.Name, topicInfo.Partition)
			coords[topicInfo.Partition] = NewTopicCoordinator(topicInfo.Name, topicInfo.Partition, basepath)
			self.coordMutex.Unlock()
			self.markLocalDataState(topicInfo.Name, topicInfo.Partition, true)
		}
	}
	return nil
}

func (self *NsqdCoordinator) checkLocalTopicForISR(tc *TopicCoordinator) *CoordErr {
	if tc.topicInfo.Leader == self.myNode.GetID() {
		// leader should always has the newest local data
		return nil
	}
	logMgr := tc.logMgr
	logid := logMgr.GetLastCommitLogID()
	c, err := self.acquireRpcClient(tc.topicInfo.Leader)
	if err != nil {
		return err
	}
	leaderID, err := c.GetLastCommmitLogID(&tc.topicInfo)
	if err != nil {
		return err
	}
	coordLog.Infof("checking if ISR synced, logid leader: %v, myself:%v", leaderID, logid)
	if leaderID > logid {
		return ErrLocalFallBehind
	} else if logid > leaderID {
		coordLog.Infof("this node has more data than leader, should rejoin.")
		return ErrLocalForwardThanLeader
	}
	return nil
}

func (self *NsqdCoordinator) checkForUnsyncedTopics() {
	ticker := time.NewTicker(time.Minute * 10)
	doWork := func() {
		// check local topic for coordinator
		self.loadLocalTopicData()

		// check coordinator with cluster
		tmpChecks := make(map[string]map[int]bool, len(self.topicCoords))
		self.coordMutex.Lock()
		for topic, info := range self.topicCoords {
			for pid, _ := range info {
				if _, ok := tmpChecks[topic]; !ok {
					tmpChecks[topic] = make(map[int]bool)
				}
				tmpChecks[topic][pid] = true
			}
		}
		self.coordMutex.Unlock()
		for topic, info := range tmpChecks {
			for pid, _ := range info {
				topicMeta, err := self.leadership.GetTopicInfo(topic, pid)
				if err != nil {
					continue
				}
				if FindSlice(topicMeta.CatchupList, self.myNode.GetID()) != -1 {
					self.catchupFromLeader(*topicMeta, false)
				} else if FindSlice(topicMeta.ISR, self.myNode.GetID()) == -1 {
					if len(topicMeta.ISR)+len(topicMeta.CatchupList) >= topicMeta.Replica {
						coordLog.Infof("the topic should be clean since not relevance to me: %v", topicMeta)
						self.localNsqd.CloseExistingTopic(topicMeta.Name, topicMeta.Partition)
						delete(self.topicCoords[topic], pid)
					} else {
						self.requestJoinCatchup(topicMeta.Name, topicMeta.Partition)
					}
				}
			}
		}
	}
	for {
		select {
		case <-self.stopChan:
			return
		case <-self.tryCheckUnsynced:
			doWork()
		case <-ticker.C:
			doWork()
		}
	}
}

func (self *NsqdCoordinator) acquireTopicLeader(topicInfo *TopicPartionMetaInfo) *CoordErr {
	err := self.leadership.AcquireTopicLeader(topicInfo.Name, topicInfo.Partition, self.myNode)
	if err != nil {
		coordLog.Infof("failed to acquire leader for topic(%v): %v", topicInfo.Name, err)
		return &CoordErr{err.Error(), RpcNoErr, CoordElectionErr}
	}
	return nil
}

func (self *NsqdCoordinator) isMineLeaderForTopic(tp *TopicCoordinator) bool {
	return tp.GetLeaderID() == self.myNode.GetID()
}

// for isr node to check with leader
func (self *NsqdCoordinator) syncToNewLeader(topicCoord *TopicCoordinator, waitReady bool) {
	// If leadership changed, all isr nodes should sync to new leader and check
	// consistent with leader, after all isr nodes notify ready, the leader can
	// accept new write.
	err := self.checkLocalTopicForISR(topicCoord)
	if err == ErrLocalFallBehind || err == ErrLocalForwardThanLeader {
		if waitReady {
			coordLog.Infof("isr begin sync with new leader")
			err = self.catchupFromLeader(topicCoord.topicInfo, true)
			if err != nil {
				coordLog.Infof("isr sync with new leader error: %v", err)
			}
		} else {
			coordLog.Infof("isr not synced with new leader, should retry catchup")
			err := self.requestLeaveFromISR(topicCoord.topicInfo.Name, topicCoord.topicInfo.Partition)
			if err != nil {
				coordLog.Infof("request leave isr failed: %v", err)
			} else {
				self.requestJoinCatchup(topicCoord.topicInfo.Name, topicCoord.topicInfo.Partition)
			}
		}
		return
	} else if err != nil {
		coordLog.Infof("check isr with leader err: %v", err)
	}
	if waitReady && err == nil {
		self.notifyReadyForTopicISR(&topicCoord.topicInfo, &topicCoord.topicLeaderSession)
	}
}

func (self *NsqdCoordinator) requestJoinCatchup(topic string, partition int) *CoordErr {
	coordLog.Infof("try to join catchup for topic: %v-%v", topic, partition)
	c, err := self.getLookupRemoteProxy()
	if err != nil {
		coordLog.Infof("get lookup failed: %v", err)
		return err
	}
	err = c.RequestJoinCatchup(topic, partition, self.myNode.GetID())
	if err != nil {
		coordLog.Infof("request join catchup failed: %v", err)
	}
	return err
}

func (self *NsqdCoordinator) requestJoinTopicISR(topicInfo *TopicPartionMetaInfo) *CoordErr {
	// request change catchup to isr list and wait for nsqlookupd response to temp disable all new write.
	c, err := self.getLookupRemoteProxy()
	if err != nil {
		return err
	}
	err = c.RequestJoinTopicISR(topicInfo.Name, topicInfo.Partition, self.myNode.GetID())
	return err
}

func (self *NsqdCoordinator) notifyReadyForTopicISR(topicInfo *TopicPartionMetaInfo, leaderSession *TopicLeaderSession) *CoordErr {
	// notify myself is ready for isr list for current session and can accept new write.
	// The empty session will trigger the lookup send the current leader session.
	// leader session should contain the (isr list, current leader session, leader epoch), to identify the
	// the different session stage.
	c, err := self.getLookupRemoteProxy()
	if err != nil {
		return err
	}

	return c.ReadyForTopicISR(topicInfo.Name, topicInfo.Partition, self.myNode.GetID(), leaderSession)
}

func (self *NsqdCoordinator) prepareLeaveFromISR(topic string, partition int) *CoordErr {
	c, err := self.getLookupRemoteProxy()
	if err != nil {
		return err
	}
	return c.PrepareLeaveFromISR(topic, partition, self.myNode.GetID())
}

func (self *NsqdCoordinator) requestLeaveFromISR(topic string, partition int) *CoordErr {
	c, err := self.getLookupRemoteProxy()
	if err != nil {
		return err
	}
	c.RequestLeaveFromISR(topic, partition, self.myNode.GetID())

	return nil
}

// this should only be called by leader to remove slow node in isr.
// Be careful to avoid removing most of the isr nodes, should only remove while
// only small part of isr is slow.
// TODO: If most of nodes is slow, the leader should check the leader itself and
// maybe giveup the leadership.
func (self *NsqdCoordinator) requestLeaveFromISRByLeader(topic string, partition int, nid string) *CoordErr {
	topicCoord, err := self.getTopicCoord(topic, partition)
	if err != nil {
		return err
	}
	if err = topicCoord.checkWriteForLeader(self.myNode.GetID()); err != nil {
		return err
	}
	// send request with leader session, so lookup can check the valid of session.
	c, err := self.getLookupRemoteProxy()
	if err != nil {
		return err
	}
	return c.RequestLeaveFromISRByLeader(topic, partition, self.myNode.GetID(), &topicCoord.topicLeaderSession)
}

func (self *NsqdCoordinator) catchupFromLeader(topicInfo TopicPartionMetaInfo, isISR bool) *CoordErr {
	coordLog.Infof("local topic begin catchup : %v, isISR: %v", topicInfo.GetTopicDesp(), isISR)
	// get local commit log from check point , and pull newer logs from leader
	tc, err := self.getTopicCoord(topicInfo.Name, topicInfo.Partition)
	if err != nil {
		coordLog.Warningf("topic(%v) catching failed since topic coordinator missing: %v", topicInfo.Name, err)
		return ErrMissingTopicCoord
	}
	logMgr := tc.logMgr
	offset, logErr := logMgr.GetLastLogOffset()
	if logErr != nil {
		coordLog.Warningf("catching failed since log offset read error: %v", logErr)
		return ErrLocalTopicDataCorrupt
	}
	// pull logdata from leader at the offset.
	c, err := self.acquireRpcClient(topicInfo.Leader)
	if err != nil {
		coordLog.Warningf("failed to get rpc client while catchup: %v", err)
		return err
	}

	retryCnt := 0
	for offset > 0 {
		// if leader changed we abort and wait next time
		if tc.GetLeaderID() != topicInfo.Leader {
			coordLog.Warningf("topic leader changed, abort current catchup: %v", topicInfo.GetTopicDesp())
			return ErrTopicLeaderChanged
		}
		localLogData, localErr := logMgr.GetCommmitLogFromOffset(offset)
		if localErr != nil {
			offset -= int64(GetLogDataSize())
			continue
		}

		leaderOffset, leaderLogData, err := c.GetCommmitLogFromOffset(&topicInfo, offset)
		if err.IsEqual(ErrTopicCommitLogOutofBound) || err.IsEqual(ErrTopicCommitLogEOF) {
			coordLog.Infof("local commit log is more than leader while catchup: %v vs %v", offset, leaderOffset)
			// local log is ahead of the leader, must truncate local data.
			// truncate commit log and truncate the data file to last log
			// commit offset.
			offset = leaderOffset
		} else if err != nil {
			coordLog.Warningf("something wrong while get leader logdata while catchup: %v", err)
			if retryCnt > MAX_CATCHUP_RETRY {
				return err
			}
			retryCnt++
			time.Sleep(time.Second)
		} else {
			if *localLogData == leaderLogData {
				break
			}
			offset -= int64(GetLogDataSize())
		}
	}
	coordLog.Infof("local commit log match leader at: %v", offset)
	localTopic, localErr := self.localNsqd.GetExistingTopic(topicInfo.Name)
	if localErr != nil {
		coordLog.Errorf("get local topic failed:%v", localErr)
		return ErrLocalMissingTopic
	}
	if localTopic.GetTopicPart() != topicInfo.Partition {
		coordLog.Errorf("local topic partition mismatch:%v vs %v", topicInfo.Partition, localTopic.GetTopicPart())
		return ErrLocalTopicPartitionMismatch
	}
	lastLog, localErr := logMgr.GetCommmitLogFromOffset(offset)
	if localErr != nil {
		coordLog.Errorf("failed to truncate local commit log: %v", localErr)
		return &CoordErr{localErr.Error(), RpcNoErr, CoordLocalErr}
	}
	localTopic.Lock()
	defer localTopic.Unlock()
	// reset the data file to (lastLog.LogID, lastLog.MsgOffset),
	// and the next message write position should be updated.
	localErr = localTopic.ResetBackendEndNoLock(nsqd.BackendOffset(lastLog.MsgOffset), lastLog.MsgCnt-1)
	if err != nil {
		coordLog.Errorf("failed to reset local topic data: %v", err)
		return &CoordErr{localErr.Error(), RpcNoErr, CoordLocalErr}
	}
	lastLog, localErr = logMgr.TruncateToOffset(offset)
	if localErr != nil {
		coordLog.Errorf("failed to truncate local commit log: %v", localErr)
		return &CoordErr{localErr.Error(), RpcNoErr, CoordLocalErr}
	}

	synced := false
	newMsgs := make([]*nsqd.Message, 0)
	tmpBuf := make([]byte, 1000)
	leaderSession := tc.topicLeaderSession
	for {
		if tc.GetLeaderID() != topicInfo.Leader {
			coordLog.Warningf("topic leader changed, abort current catchup: %v", topicInfo.GetTopicDesp())
			return ErrTopicLeaderChanged
		}
		logs, dataList, rpcErr := c.PullCommitLogsAndData(topicInfo.Name, topicInfo.Partition, offset, 100)
		if err != nil {
			// if not network error, something wrong with commit log file, we need return to abort.
			coordLog.Infof("error while get logs :%v", err)
			if retryCnt > MAX_CATCHUP_RETRY {
				return &CoordErr{rpcErr.Error(), RpcCommonErr, CoordNetErr}
			}
			retryCnt++
			time.Sleep(time.Second)
			continue
		} else if len(logs) == 0 {
			synced = true
		}
		for i, l := range logs {
			d := dataList[i]
			// read and decode all messages
			newMsgs, localErr = DecodeMessagesFromRaw(bufio.NewReader(bytes.NewReader(d)), newMsgs, tmpBuf)
			if localErr != nil {
				coordLog.Infof("Failed to decode message: %v", localErr)
				return &CoordErr{localErr.Error(), RpcNoErr, CoordLocalErr}
			}
			if len(newMsgs) == 1 {
				localErr = localTopic.PutMessageOnReplica(newMsgs[0], nsqd.BackendOffset(l.MsgOffset))
				if localErr != nil {
					coordLog.Infof("Failed to put message on slave: %v, offset: %v", localErr, l.MsgOffset)
					return &CoordErr{localErr.Error(), RpcNoErr, CoordLocalErr}
				}
			} else {
				// TODO: do batch put
			}
			localErr = logMgr.AppendCommitLog(&l, true)
			if localErr != nil {
				coordLog.Infof("Failed to append local log: %v", localErr)
				return &CoordErr{localErr.Error(), RpcNoErr, CoordLocalErr}
			}
		}
		offset += int64(len(logs) * GetLogDataSize())

		if synced && !isISR {
			// notify nsqlookupd coordinator to add myself to isr list.
			// if success, the topic leader will disable new write.
			err := self.requestJoinTopicISR(&topicInfo)
			if err != nil {
				coordLog.Infof("request join isr failed: %v", err)
				if retryCnt > MAX_CATCHUP_RETRY {
					return err
				}
				retryCnt++
				time.Sleep(time.Second)
			} else {
				// request done, and the new isr and leader will be notified,
				// after we got the notify, we will re-enter with isISR = true
				return nil
			}
		} else if synced && isISR {
			logMgr.FlushCommitLogs()
			coordLog.Infof("local topic is ready for isr: %v", topicInfo.GetTopicDesp())
			err := RetryWithTimeout(func() error {
				return self.notifyReadyForTopicISR(&topicInfo, &leaderSession)
			})
			if err != nil {
				coordLog.Infof("notify ready for isr failed: %v", err)
			} else {
				coordLog.Infof("isr synced: %v", topicInfo.GetTopicDesp())
			}
			break
		}
	}
	coordLog.Infof("local topic catchup done: %v", topicInfo.GetTopicDesp())
	return nil
}

func (self *NsqdCoordinator) updateTopicInfo(topicCoord *TopicCoordinator, shouldDisableWrite bool, newTopicInfo *TopicPartionMetaInfo) *CoordErr {
	if newTopicInfo.Epoch < topicCoord.topicInfo.Epoch {
		coordLog.Warningf("topic (%v) info epoch is less while update: %v vs %v",
			topicCoord.topicInfo.GetTopicDesp(), newTopicInfo.Epoch, topicCoord.topicInfo.Epoch)
		return ErrEpochLessThanCurrent
	}
	// channels and catchup should only be modified in the separate rpc method.
	newTopicInfo.Channels = topicCoord.topicInfo.Channels
	newTopicInfo.CatchupList = topicCoord.topicInfo.CatchupList
	topicCoord.topicInfo = *newTopicInfo
	err := self.updateLocalTopic(topicCoord)
	if err != nil {
		coordLog.Warningf("init local topic failed: %v", err)
		return err
	}
	topicCoord.localDataLoaded = true
	if newTopicInfo.Leader == self.myNode.GetID() {
		// not leader before and became new leader
		if topicCoord.GetLeaderID() != self.myNode.GetID() {
			coordLog.Infof("I am notified to be leader for the topic.")
			// leader switch need disable write until the lookup notify leader
			// to accept write.
			shouldDisableWrite = true
		}
		if shouldDisableWrite {
			topicCoord.disableWrite = true
		}
		err := self.acquireTopicLeader(newTopicInfo)
		if err != nil {
			coordLog.Infof("acquire topic leader failed.")
		}
	} else if FindSlice(newTopicInfo.ISR, self.myNode.GetID()) != -1 {
		coordLog.Infof("I am in isr list.")
	} else if FindSlice(newTopicInfo.CatchupList, self.myNode.GetID()) != -1 {
		coordLog.Infof("I am in catchup list.")
	}
	return nil
}

func (self *NsqdCoordinator) updateTopicLeaderSession(topicCoord *TopicCoordinator, newLeaderSession *TopicLeaderSession, waitReady bool) *CoordErr {
	n := newLeaderSession
	if n.LeaderEpoch < topicCoord.GetLeaderEpoch() {
		coordLog.Infof("topic partition leadership epoch error.")
		return ErrEpochLessThanCurrent
	}
	topicCoord.topicLeaderSession = *newLeaderSession

	topicData, err := self.localNsqd.GetExistingTopic(topicCoord.topicInfo.Name)
	if err != nil {
		coordLog.Infof("no topic on local: %v, %v", topicCoord.topicInfo.GetTopicDesp(), err)
		return ErrLocalMissingTopic
	}

	if topicCoord.IsMineLeaderSessionReady(self.myNode.GetID()) {
		coordLog.Infof("I become the leader for the topic: %v", topicCoord.topicInfo.GetTopicDesp())
		topicData.EnableForMaster()
	} else {
		topicData.DisableForSlave()
		if n.LeaderNode == nil || n.Session == "" {
			coordLog.Infof("topic leader is missing : %v", topicCoord.topicInfo.GetTopicDesp())
		} else {
			coordLog.Infof("topic %v leader changed to :%v. epoch: %v", topicCoord.topicInfo.GetTopicDesp(), n.LeaderNode.GetID(), n.LeaderEpoch)
			// if catching up, pull data from the new leader
			// if isr, make sure sync to the new leader
			if FindSlice(topicCoord.topicInfo.ISR, self.myNode.GetID()) != -1 {
				self.syncToNewLeader(topicCoord, waitReady)
			} else {
				self.tryCheckUnsynced <- true
			}
		}
	}
	return nil
}

// any modify operation on the topic should check for topic leader.
func (self *NsqdCoordinator) getTopicCoord(topic string, partition int) (*TopicCoordinator, *CoordErr) {
	self.coordMutex.RLock()
	defer self.coordMutex.RUnlock()
	if v, ok := self.topicCoords[topic]; ok {
		if topicCoord, ok := v[partition]; ok {
			return topicCoord, nil
		}
	}
	return nil, ErrMissingTopicCoord
}

func (self *NsqdCoordinator) PutMessageToCluster(topic *nsqd.Topic, body []byte) error {
	topicName := topic.GetTopicName()
	partition := topic.GetTopicPart()
	topicCoord, checkErr := self.getTopicCoord(topicName, partition)
	if checkErr != nil {
		return checkErr
	}
	if checkErr = topicCoord.checkWriteForLeader(self.myNode.GetID()); checkErr != nil {
		coordLog.Warningf("topic(%v) check write failed :%v", topicName, checkErr)
		return checkErr
	}
	logMgr := topicCoord.logMgr
	var err error
	var commitLog CommitLogData
	needRefreshISR := false
	needRollback := false
	success := 0
	retryCnt := 0
	isrList := topicCoord.topicInfo.ISR

	topic.Lock()
	msg := nsqd.NewMessage(0, body)
	id, offset, writeBytes, totalCnt, putErr := topic.PutMessageNoLock(msg)
	if putErr != nil {
		coordLog.Warningf("put message to local failed: %v", err)
		err = ErrLocalWriteFailed
		goto exitpub
	}
	needRollback = true
	commitLog.LogID = int64(id)
	commitLog.Epoch = topicCoord.GetLeaderEpoch()
	commitLog.MsgOffset = int64(offset)
	commitLog.MsgSize = writeBytes
	commitLog.MsgCnt = totalCnt

retrypub:
	if retryCnt > MAX_WRITE_RETRY {
		goto exitpub
	}
	if needRefreshISR {
		topicCoord.refreshTopicCoord()
		err = topicCoord.checkWriteForLeader(self.myNode.GetID())
		if err != nil {
			coordLog.Debugf("check write failed: %v", err)
			goto exitpub
		}
		commitLog.Epoch = topicCoord.GetLeaderEpoch()
		isrList = topicCoord.topicInfo.ISR
		coordLog.Debugf("isr refreshed while write: %v", topicCoord.topicLeaderSession)
	}
	success = 0
	retryCnt++

	// send message to slaves with current topic epoch
	// replica should check if offset matching. If not matched the replica should leave the ISR list.
	// also, the coordinator should retry on fail until all nodes in ISR success.
	// If failed, should update ISR and retry.
	// TODO: optimize send all requests first and then wait all responses
	for _, nodeID := range isrList {
		if nodeID == self.myNode.GetID() {
			success++
			continue
		}
		c, rpcErr := self.acquireRpcClient(nodeID)
		if rpcErr != nil {
			coordLog.Infof("get rpc client %v failed: %v", nodeID, rpcErr)
			needRefreshISR = true
			time.Sleep(time.Millisecond * time.Duration(retryCnt))
			goto retrypub
		}
		// should retry if failed, and the slave should keep the last success write to avoid the duplicated
		putErr := c.PutMessage(&topicCoord.topicLeaderSession, &topicCoord.topicInfo, commitLog, msg)
		if putErr == nil {
			success++
		} else {
			// check if we need to remove this replica from isr
			// isr may down or some error.
			// We also need do some work to decide if we
			// should give up my leadership.
			coordLog.Infof("sync write to replica %v failed: %v", nodeID, putErr)
			err = putErr
			if !putErr.CanRetry() {
				goto exitpub
			}
		}
	}

	if success == len(isrList) {
		err := logMgr.AppendCommitLog(&commitLog, false)
		if err != nil {
			panic(err)
		}
	} else {
		coordLog.Warningf("topic %v sync write message (id:%v) failed: %v", topic.GetFullName(), msg.ID, err)
		needRefreshISR = true
		time.Sleep(time.Millisecond * time.Duration(retryCnt))
		goto retrypub
	}
exitpub:
	if err != nil && needRollback {
		resetErr := topic.RollbackNoLock(offset, 1)
		if resetErr != nil {
			coordLog.Errorf("rollback local topic %v to offset %v failed: %v", topic.GetFullName(), offset, resetErr)
		}
		// TODO: check all replicas to make sure all are good, if needed rollback replicas
	}
	topic.Unlock()
	if err != nil {
		if coordErr, ok := err.(*CoordErr); ok {
			coordLog.Infof("PutMessageToCluster error: %v", coordErr)
			if coordErr.IsNeedCheckSync() {
				// TODO: notify to check the sync of this topic
				// self.syncCheckChan <- topic.GetTopicName()
			}
		}
	} else {
		coordLog.Debugf("topic(%v) PutMessageToCluster done: %v", topic.GetFullName(), msg.ID)
	}
	return err
}

func (self *NsqdCoordinator) PutMessagesToCluster(topic string, partition int, messages []string) error {
	_, err := self.getTopicCoord(topic, partition)
	if err != nil {
		return err
	}
	//TODO:
	return ErrWriteQuorumFailed
}

func (self *NsqdCoordinator) putMessageOnSlave(tc *TopicCoordinator, logData CommitLogData, msg *nsqd.Message) *CoordErr {
	topicName := tc.topicInfo.Name
	partition := tc.topicInfo.Partition

	coordLog.Debugf("pub on slave : %v, msg %v", topicName, msg.ID)
	logMgr := tc.logMgr
	if logMgr.IsCommitted(logData.LogID) {
		coordLog.Infof("pub the already committed log id : %v", logData.LogID)
		return nil
	}
	topic, localErr := self.localNsqd.GetExistingTopic(topicName)
	if localErr != nil {
		coordLog.Infof("pub on slave missing topic : %v", topicName)
		// TODO: leave the isr and try re-sync with leader
		return &CoordErr{localErr.Error(), RpcCommonErr, CoordLocalErr}
	}

	if topic.GetTopicPart() != partition {
		coordLog.Errorf("topic on slave has different partition : %v vs %v", topic.GetTopicPart(), partition)
		return ErrLocalMissingTopic
	}

	topic.Lock()
	defer topic.Unlock()
	putErr := topic.PutMessageOnReplica(msg, nsqd.BackendOffset(logData.MsgOffset))
	if putErr != nil {
		coordLog.Errorf("put message on slave failed: %v", putErr)
		return ErrLocalWriteFailed
	}
	logErr := logMgr.AppendCommitLog(&logData, true)
	if logErr != nil {
		coordLog.Errorf("write commit log on slave failed: %v", logErr)
		// TODO: leave the isr and try re-sync with leader
		return &CoordErr{logErr.Error(), RpcCommonErr, CoordLocalErr}
	}
	return nil
}

func (self *NsqdCoordinator) putMessagesOnSlave(topicName string, partition int, logData CommitLogData, msgs []*nsqd.Message) *CoordErr {
	if len(msgs) == 0 {
		return ErrPubArgError
	}
	if logData.LogID != int64(msgs[0].ID) {
		return ErrPubArgError
	}
	tc, err := self.getTopicCoord(topicName, partition)
	if err != nil {
		return err
	}
	logMgr := tc.logMgr
	topic, localErr := self.localNsqd.GetExistingTopic(topicName)
	if localErr != nil {
		// TODO: leave the isr and try re-sync with leader
		return NewCoordErr(err.Error(), CoordLocalErr)
	}

	if topic.GetTopicPart() != partition {
		return ErrLocalMissingTopic
	}

	if logMgr.IsCommitted(logData.LogID) {
		coordLog.Infof("already commited log id : %v", logData.LogID)
		return ErrCommitLogIDDup
	}

	putErr := topic.PutMessagesOnReplica(msgs, nsqd.BackendOffset(logData.MsgOffset))
	if putErr != nil {
		coordLog.Warningf("pub on slave failed: %v", putErr)
		err = ErrLocalWriteFailed
		return err
	}

	logErr := logMgr.AppendCommitLog(&logData, true)
	if logErr != nil {
		coordLog.Errorf("write commit log on slave failed: %v", logErr)
		// TODO: leave isr
		return NewCoordErr(logErr.Error(), CoordLocalErr)

	}
	return nil
}

func (self *NsqdCoordinator) FinishMessageToCluster(channel *nsqd.Channel, clientID int64, msgID nsqd.MessageID) error {
	topicCoord, err := self.getTopicCoord(channel.GetTopicName(), channel.GetTopicPart())
	if err != nil {
		return err
	}
	if err = topicCoord.checkWriteForLeader(self.myNode.GetID()); err != nil {
		return err
	}

	offset, localErr := channel.FinishMessage(clientID, msgID)
	if localErr != nil {
		return localErr
	}
	var syncOffset ChannelConsumerOffset
	syncOffset.OffsetID = int64(msgID)
	syncOffset.VOffset = int64(offset)
	// rpc call to slaves
	successNum := 0
	isrList := topicCoord.topicInfo.ISR
	for _, nodeID := range isrList {
		if nodeID == self.myNode.GetID() {
			successNum++
			continue
		}
		c, err := self.acquireRpcClient(nodeID)
		if err != nil {
			coordLog.Infof("get rpc client failed: %v", err)
			continue
		}

		err = c.UpdateChannelOffset(&topicCoord.topicLeaderSession, &topicCoord.topicInfo, channel.GetName(), syncOffset)
		if err != nil {
			coordLog.Warningf("node %v update offset failed %v.", nodeID, err)
		} else {
			successNum++
		}
	}
	if successNum != len(isrList) {
		coordLog.Warningf("some nodes in isr is not synced with channel consumer offset.")
	}
	// we allow the offset not synced, since it can be sync later without losing any data.
	// only cause some repeated message consume.
	return nil
}

func (self *NsqdCoordinator) updateChannelOffsetOnSlave(tc *TopicCoordinator, channelName string, offset ChannelConsumerOffset) *CoordErr {
	topicName := tc.topicInfo.Name
	partition := tc.topicInfo.Partition
	topic, localErr := self.localNsqd.GetExistingTopic(topicName)
	if localErr != nil {
		coordLog.Infof("slave missing topic : %v", topicName)
		// TODO: leave the isr and try re-sync with leader
		return &CoordErr{localErr.Error(), RpcCommonErr, CoordLocalErr}
	}

	if topic.GetTopicPart() != partition {
		coordLog.Errorf("topic on slave has different partition : %v vs %v", topic.GetTopicPart(), partition)
		return ErrLocalMissingTopic
	}
	ch := topic.GetChannel(channelName)
	ch.DisableConsume(true)
	err := ch.ConfirmBackendQueueOnSlave(nsqd.BackendOffset(offset.VOffset))
	if err != nil {
		coordLog.Warningf("update local channel(%v) offset failed: %v", channelName, err)
		return &CoordErr{err.Error(), RpcCommonErr, CoordLocalErr}
	}
	return nil
}

func (self *NsqdCoordinator) readTopicRawData(tc *TopicCoordinator, offset int64, size int32) ([]byte, *CoordErr) {
	// read directly from local topic data used for pulling data by replicas
	return nil, nil
}

// flush cached data to disk. This should be called when topic isr list
// changed or leader changed.
func (self *NsqdCoordinator) notifyFlushData(topic string, partition int) {
	if len(self.flushNotifyChan) > 1 {
		return
	}
	self.flushNotifyChan <- TopicPartitionID{topic, partition}
}

func (self *NsqdCoordinator) updateLocalTopic(topicCoord *TopicCoordinator) *CoordErr {
	// check topic exist and prepare on local.
	t := self.localNsqd.GetTopic(topicCoord.topicInfo.Name, topicCoord.topicInfo.Partition)
	if t == nil {
		return ErrLocalGetTopicFailed
	}
	if t.MsgIDCursor == nil {
		t.MsgIDCursor = topicCoord.logMgr
	}
	return nil
}

// before shutdown, we transfer the leader to others to reduce
// the unavailable time.
func (self *NsqdCoordinator) prepareLeavingCluster() {
	coordLog.Infof("I am prepare leaving the cluster.")
	for topicName, topicData := range self.topicCoords {
		for pid, tpData := range topicData {
			if FindSlice(tpData.topicInfo.ISR, self.myNode.GetID()) == -1 {
				continue
			}
			if len(tpData.topicInfo.ISR)-1 <= tpData.topicInfo.Replica/2 {
				coordLog.Infof("The isr nodes in topic %v is not enough, waiting...", tpData.topicInfo.GetTopicDesp())
				// we need notify lookup to add new isr since I am leaving.
				// wait until isr is enough or timeout.
				time.Sleep(time.Second * 3)
			}

			// prepare will handle the leader transfer.
			err := self.prepareLeaveFromISR(topicName, pid)
			if err != nil {
				coordLog.Infof("failed to prepare the leave request: %v", err)
			}

			if tpData.topicLeaderSession.LeaderNode.GetID() == self.myNode.GetID() {
				// leader
				self.leadership.ReleaseTopicLeader(topicName, pid)
				coordLog.Infof("The leader for topic %v is transfered.", tpData.topicInfo.GetTopicDesp())
			}
			self.requestLeaveFromISR(topicName, pid)
		}
	}
	coordLog.Infof("prepare leaving finished.")
}
