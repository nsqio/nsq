package consistence

import (
	"bytes"
	"github.com/absolute8511/nsq/nsqd"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	MAX_WRITE_RETRY = 10
)

type CoordErrType int

const (
	CoordNoErr CoordErrType = iota
	CoordCommonErr
	CoordNetErr
	CoordElectionErr
	CoordElectionTmpErr
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
	ErrNotTopicLeader            = NewCoordErr("not topic leader", CoordElectionErr)
	ErrEpochMismatch             = NewCoordErr("commit epoch not match", CoordElectionErr)
	ErrEpochLessThanCurrent      = NewCoordErr("epoch should be increased", CoordElectionErr)
	ErrWriteQuorumFailed         = NewCoordErr("write to quorum failed.", CoordElectionTmpErr)
	ErrCommitLogIDDup            = NewCoordErr("commit id duplicated", CoordElectionErr)
	ErrMissingTopicLeaderSession = NewCoordErr("missing topic leader session", CoordElectionErr)
	ErrLeaderSessionMismatch     = NewCoordErr("leader session mismatch", CoordElectionErr)
	ErrWriteDisabled             = NewCoordErr("write is disabled on the topic", CoordElectionTmpErr)
	ErrLeavingISRWait            = NewCoordErr("leaving isr need wait.", CoordElectionTmpErr)

	ErrPubArgError       = NewCoordErr("pub argument error", CoordCommonErr)
	ErrMissingTopicCoord = NewCoordErr("missing topic coordinator", CoordCommonErr)

	ErrMissingTopicLog           = NewCoordErr("missing topic log ", CoordLocalErr)
	ErrLocalFallBehind           = NewCoordErr("local data fall behind", CoordElectionErr)
	ErrLocalForwardThanLeader    = NewCoordErr("local data is more than leader", CoordElectionErr)
	ErrLocalWriteFailed          = NewCoordErr("write data to local failed", CoordLocalErr)
	ErrLocalMissingTopic         = NewCoordErr("local topic missing", CoordLocalErr)
	ErrLocalNotReadyForWrite     = NewCoordErr("local topic is not ready for write.", CoordLocalErr)
	ErrLocalGetTopicFailed       = NewCoordErr("local topic init failed", CoordLocalErr)
	ErrLocalInitTopicCoordFailed = NewCoordErr("topic coordinator init failed", CoordLocalErr)
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

type NsqdCoordinator struct {
	leadership      NSQDLeadership
	lookupLeader    *NsqLookupdNodeInfo
	topicCoords     map[string]map[int]*TopicCoordinator
	myNode          NsqdNodeInfo
	nsqdRpcClients  map[string]*NsqdRpcClient
	flushNotifyChan chan TopicPartitionID
	stopChan        chan struct{}
	dataRootPath    string
	localDataStates map[string]map[int]bool
	localNsqd       *nsqd.NSQD
	rpcServer       *NsqdCoordRpcServer
}

func NewNsqdCoordinator(ip, tcpport, rpcport, extraID string, rootPath string, nsqd *nsqd.NSQD) *NsqdCoordinator {
	nodeInfo := NsqdNodeInfo{
		NodeIp:  ip,
		TcpPort: tcpport,
		RpcPort: rpcport,
	}
	nodeInfo.ID = GenNsqdNodeID(&nodeInfo, extraID)
	nsqdCoord := &NsqdCoordinator{
		leadership:      nil,
		topicCoords:     make(map[string]map[int]*TopicCoordinator),
		myNode:          nodeInfo,
		nsqdRpcClients:  make(map[string]*NsqdRpcClient),
		flushNotifyChan: make(chan TopicPartitionID, 2),
		stopChan:        make(chan struct{}),
		dataRootPath:    rootPath,
		localDataStates: make(map[string]map[int]bool),
		localNsqd:       nsqd,
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
	go self.watchNsqLookupd()
	go self.loadLocalTopicData()
	go self.checkForUnusedTopics()
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

func (self *NsqdCoordinator) getLookupConn() (*NsqLookupRpcClient, *CoordErr) {
	c, err := NewNsqLookupRpcClient(net.JoinHostPort(self.lookupLeader.NodeIp, self.lookupLeader.RpcPort), time.Second)
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

// for each topic data on local, check if necessary keep sync with the leader
// If no leader available, then join catchup list and wait for at least half
// ISR nodes.
func (self *NsqdCoordinator) loadLocalTopicData() {
	pathList := make([]string, 0)
	pathList, _ = filepath.Glob(self.dataRootPath + "/*")
	for _, topicName := range pathList {
		partitionList, err := filepath.Glob(filepath.Join(self.dataRootPath, topicName) + "/*")
		if err != nil {
			coordLog.Infof("read topic partition file failed: %v", err)
		}
		for _, partitionStr := range partitionList {
			partition, err := strconv.Atoi(partitionStr)
			if err != nil {
				continue
			}
			coordLog.Infof("load topic: %v-%v", topicName, partition)
			if _, err := os.Stat(GetTopicPartitionLogPath(self.dataRootPath, topicName, partition)); os.IsNotExist(err) {
				coordLog.Infof("no commit log file under topic: %v-%v", topicName, partition)
				continue
			}
			//scan local File
			topicInfo, err := self.leadership.GetTopicInfo(topicName, partition)
			if err != nil {
				coordLog.Infof("failed to get topic info:%v-%v", topicName, partition)
				continue
			}
			if FindSlice(topicInfo.ISR, self.myNode.GetID()) != -1 {
				coordLog.Infof("I am starting as isr node.")
				// check local data with leader.
				err := self.checkLocalTopicForISR(topicInfo)
				if err != nil {
					self.requestLeaveFromISR(topicInfo.Name, topicInfo.Partition)
				} else {
					states, ok := self.localDataStates[topicInfo.Name]
					if !ok {
						states := make(map[int]bool)
						self.localDataStates[topicInfo.Name] = states
					}
					states[topicInfo.Partition] = true
					self.notifyReadyForTopicISR(topicInfo, "")
					continue
				}
			}
			if len(topicInfo.ISR) >= topicInfo.Replica {
				coordLog.Infof("no need load the local topic since the replica is enough: %v-%v", topicName, partition)
				continue
			}
			err = RetryWithTimeout(func() error {
				err := self.requestJoinCatchup(topicName, partition)
				return err
			})
			if err != nil {
				coordLog.Infof("failed to request join catchup")
				continue
			}
			go self.catchupFromLeader(*topicInfo)
		}
	}
}

func (self *NsqdCoordinator) checkLocalTopicForISR(topicInfo *TopicPartionMetaInfo) *CoordErr {
	tc, err := self.getTopicCoord(topicInfo.Name, topicInfo.Partition)
	if err != nil {
		coordLog.Warningf("missing topic coordinator: %v", topicInfo.Name)
		return err
	}

	logMgr := tc.logMgr
	if topicInfo.Leader == self.myNode.GetID() {
		// leader should always has the newest local data
		return nil
	}
	logid := logMgr.GetLastCommitLogID()
	c, err := self.acquireRpcClient(topicInfo.Leader)
	if err != nil {
		return err
	}
	leaderID, logErr := c.GetLastCommmitLogID(topicInfo)
	if logErr != nil {
		return NewCoordErr(logErr.Error(), CoordLocalErr)
	}
	if leaderID > logid {
		coordLog.Infof("this node is out of date, should rejoin.")
		// TODO: request the lookup to remove myself from isr
		return ErrLocalFallBehind
	}

	if logid > leaderID+1 {
		coordLog.Infof("this node has more data than leader, should rejoin.")
		return ErrLocalForwardThanLeader
	}
	return nil
}

func (self *NsqdCoordinator) checkForUnusedTopics() {
	ticker := time.NewTicker(time.Minute)
	for {
		select {
		case <-ticker.C:
			tmpChecks := make(map[string]map[int]bool, len(self.topicCoords))
			for topic, info := range self.topicCoords {
				for pid, _ := range info {
					if _, ok := tmpChecks[topic]; !ok {
						tmpChecks[topic] = make(map[int]bool)
					}
					tmpChecks[topic][pid] = true
				}
			}
			for topic, info := range tmpChecks {
				for pid, _ := range info {
					topicMeta, err := self.leadership.GetTopicInfo(topic, pid)
					if err != nil {
						continue
					}
					if FindSlice(topicMeta.ISR, self.myNode.GetID()) == -1 &&
						FindSlice(topicMeta.CatchupList, self.myNode.GetID()) == -1 {
						coordLog.Infof("the topic should be clean since not relevance to me: %v", topicMeta)
						delete(self.topicCoords[topic], pid)
					}
				}
			}
		}
	}
}

func (self *NsqdCoordinator) acquireTopicLeader(topicInfo TopicPartionMetaInfo) error {
	err := self.leadership.AcquireTopicLeader(topicInfo.Name, topicInfo.Partition, self.myNode)
	return err
}

func (self *NsqdCoordinator) isMineLeaderForTopic(tp *TopicCoordinator) bool {
	return tp.GetLeaderID() == self.myNode.GetID()
}

func (self *NsqdCoordinator) syncToNewLeader(topic string, partition int, leader *TopicLeaderSession) {
	// check last commit log.
}

func (self *NsqdCoordinator) requestJoinCatchup(topic string, partition int) *CoordErr {
	//
	c, err := self.getLookupConn()
	if err != nil {
		return err
	}
	err = c.RequestJoinCatchup(topic, partition, self.myNode.GetID())
	return err
}

func (self *NsqdCoordinator) requestJoinTopicISR(topicInfo *TopicPartionMetaInfo) (string, *CoordErr) {
	// request change catchup to isr list and wait for nsqlookupd response to temp disable all new write.
	c, err := self.getLookupConn()
	if err != nil {
		return "", err
	}
	session, err := c.RequestJoinTopicISR(topicInfo.Name, topicInfo.Partition, self.myNode.GetID())
	return session, err
}

func (self *NsqdCoordinator) notifyReadyForTopicISR(topicInfo *TopicPartionMetaInfo, session string) *CoordErr {
	// notify myself is ready for isr list and can accept new write.
	c, err := self.getLookupConn()
	if err != nil {
		return err
	}

	return c.ReadyForTopicISR(topicInfo.Name, topicInfo.Partition, self.myNode.GetID(), session)
}

func (self *NsqdCoordinator) prepareLeaveFromISR(topic string, partition int) *CoordErr {
	c, err := self.getLookupConn()
	if err != nil {
		return err
	}
	return c.PrepareLeaveFromISR(topic, partition, self.myNode.GetID())
}

func (self *NsqdCoordinator) requestLeaveFromISR(topic string, partition int) *CoordErr {
	c, err := self.getLookupConn()
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
	c, err := self.getLookupConn()
	if err != nil {
		return err
	}
	return c.RequestLeaveFromISRByLeader(topic, partition, self.myNode.GetID(), &topicCoord.topicLeaderSession)
}

func (self *NsqdCoordinator) catchupFromLeader(topicInfo TopicPartionMetaInfo) {
	// get local commit log from check point , and pull newer logs from leader
	tc, err := self.getTopicCoord(topicInfo.Name, topicInfo.Partition)
	if err != nil {
		coordLog.Warningf("topic(%v) catching failed since topic coordinator missing: %v", topicInfo.Name, err)
		return
	}
	logMgr := tc.logMgr
	offset, logErr := logMgr.GetLastLogOffset()
	if logErr != nil {
		coordLog.Warningf("catching failed since log offset read error: %v", logErr)
		return
	}
	// pull logdata from leader at the offset.
	c, err := self.acquireRpcClient(topicInfo.Leader)
	if err != nil {
		coordLog.Warningf("failed to get rpc client while catchup: %v", err)
		return
	}

	for offset > 0 {
		// TODO: check if leader changed
		localLogData, err := logMgr.GetCommmitLogFromOffset(offset)
		if err != nil {
			offset -= int64(GetLogDataSize())
			continue
		}

		leaderOffset, leaderLogData, err := c.GetCommmitLogFromOffset(&topicInfo, offset)
		if err == ErrCommitLogOutofBound || leaderOffset < offset {
			coordLog.Infof("local commit log is more than leader while catchup: %v vs %v", offset, leaderOffset)
			// local log is ahead of the leader, must truncate local data.
			// truncate commit log and truncate the data file to last log
			// commit offset.
			lastLog, err := logMgr.TruncateToOffset(leaderOffset + int64(GetLogDataSize()))
			if err != nil {
				coordLog.Infof("failed to truncate local commit log: %v", err)
				return
			}
			offset = leaderOffset
			// TODO: reset the data file to (lastLog.LogID, lastLog.MsgOffset) +
			// messageSize,
			// and the next message write position should be updated.
			if *lastLog == leaderLogData {
				// the log is synced with leader.
				break
			}
		} else if err != nil {
			coordLog.Warningf("something wrong while get leader logdata while catchup: %v", err)
		} else {
			if *localLogData == leaderLogData {
				break
			}
		}
		offset -= int64(GetLogDataSize())
	}
	coordLog.Infof("local commit log match leader at: %v", offset)
	synced := false
	readyJoinISR := false
	joinSession := ""
	for {
		// TODO: check if leader changed
		logs, dataList, err := c.PullCommitLogsAndData(topicInfo.Name, topicInfo.Partition, offset, 100)
		if err == ErrCommitLogEOF {
			synced = true
		} else if err != nil {
			// if not network error, something wrong with commit log file, we need return to abort.
			coordLog.Infof("error while get logs :%v", err)
			time.Sleep(time.Second)
			continue
		} else if len(logs) == 0 {
			synced = true
		}
		for i, l := range logs {
			d := dataList[i]
			_ = d
			// append data to data file at l.MsgOffset
			//
			err := logMgr.AppendCommitLog(&l, true)
			if err != nil {
				coordLog.Infof("Failed to append local log: %v", err)
				return
			}
		}
		offset += int64(len(logs) * GetLogDataSize())

		if synced && !readyJoinISR {
			// notify nsqlookupd coordinator to add myself to isr list.
			s, err := self.requestJoinTopicISR(&topicInfo)
			if err != nil {
				coordLog.Infof("request join isr failed: %v", err)
				time.Sleep(time.Second)
			} else {
				joinSession = s
				logMgr.FlushCommitLogs()
				synced = false
				readyJoinISR = true
			}
		} else if synced && readyJoinISR {
			logMgr.FlushCommitLogs()
			coordLog.Infof("local topic is ready for isr: %v", topicInfo.GetTopicDesp())
			err := RetryWithTimeout(func() error {
				return self.notifyReadyForTopicISR(&topicInfo, joinSession)
			})
			if err != nil {
				coordLog.Infof("notify ready for isr failed: %v", err)
			} else {
				states, ok := self.localDataStates[topicInfo.Name]
				if !ok {
					states := make(map[int]bool)
					self.localDataStates[topicInfo.Name] = states
				}
				states[topicInfo.Partition] = true
			}
			break
		}
	}
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
	err := self.updateLocalTopic(*newTopicInfo)
	if err != nil {
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
		err := self.acquireTopicLeader(*newTopicInfo)
		if err != nil {
			coordLog.Infof("acquire topic leader failed.")
		}
	} else if FindSlice(newTopicInfo.ISR, self.myNode.GetID()) != -1 {
		coordLog.Infof("I am in isr list.")
	} else if FindSlice(newTopicInfo.CatchupList, self.myNode.GetID()) != -1 {
		coordLog.Infof("I am in catchup list.")
	} else {
		coordLog.Infof("Not a topic related to me.")
		// TODO: check if local has the topic data and decide whether to join
		// catchup list
	}
	return nil

}

func (self *NsqdCoordinator) updateTopicLeaderSession(topicCoord *TopicCoordinator, newLeaderSession *TopicLeaderSession) *CoordErr {
	n := newLeaderSession
	if n.LeaderEpoch < topicCoord.GetLeaderEpoch() {
		coordLog.Infof("topic partition leadership epoch error.")
		return ErrEpochLessThanCurrent
	}
	topicCoord.topicLeaderSession = *newLeaderSession
	if n.LeaderNode == nil || n.Session == "" {
		coordLog.Infof("topic leader is missing : %v", topicCoord.topicInfo.GetTopicDesp())
	} else if n.LeaderNode.GetID() == self.myNode.GetID() {
		coordLog.Infof("I become the leader for the topic: %v", topicCoord.topicInfo.GetTopicDesp())
	} else {
		coordLog.Infof("topic %v leader changed to :%v. epoch: %v", topicCoord.topicInfo.GetTopicDesp(), n.LeaderNode.GetID(), n.LeaderEpoch)
		// if catching up, pull data from the new leader
		// if isr, make sure sync to the new leader
		if FindSlice(topicCoord.topicInfo.ISR, self.myNode.GetID()) != -1 {
			self.syncToNewLeader(topicCoord.topicInfo.Name, topicCoord.topicInfo.Partition, n)
		} else if FindSlice(topicCoord.topicInfo.CatchupList, self.myNode.GetID()) != -1 {
			self.catchupFromLeader(topicCoord.topicInfo)
		} else {
			// TODO: check if local has the topic data and decide whether to join
			// catchup list
		}
	}
	return nil
}

// any modify operation on the topic should check for topic leader.
func (self *NsqdCoordinator) getTopicCoord(topic string, partition int) (*TopicCoordinator, *CoordErr) {
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
	id, offset, putErr := topic.PutMessageNoLock(msg)
	if putErr != nil {
		coordLog.Warningf("put message to local failed: %v", err)
		err = ErrLocalWriteFailed
		goto exitpub
	}
	needRollback = true
	commitLog.LogID = int64(id)
	commitLog.Epoch = topicCoord.GetLeaderEpoch()
	commitLog.MsgOffset = int64(offset)

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
			coordLog.Infof("sync write to replica %v failed: %v", nodeID, putErr)
			err = putErr
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
		resetErr := topic.ResetBackendEndNoLock(offset, 1)
		if resetErr != nil {
			coordLog.Errorf("rollback local topic %v to offset %v failed: %v", topic.GetFullName(), offset, resetErr)
		}
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

func (self *NsqdCoordinator) putMessageOnSlave(topicName string, partition int, logData CommitLogData, msg *nsqd.Message) *CoordErr {
	tc, err := self.getTopicCoord(topicName, partition)
	if err != nil {
		return err
	}
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
		coordLog.Errorf("write commit log on slave failed: %v", err)
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

func (self *NsqdCoordinator) updateChannelOffsetLocal(topic string, partition int, channel string, offset ChannelConsumerOffset) *CoordErr {
	self.topicCoords[topic][partition].channelConsumeOffset[channel] = offset
	return nil
}

func (self *NsqdCoordinator) syncChannelOffsetToCluster(topic string, partition int, channel string, offset ChannelConsumerOffset) *CoordErr {
	topicCoord, err := self.getTopicCoord(topic, partition)
	if err != nil {
		return err
	}
	if err = topicCoord.checkWriteForLeader(self.myNode.GetID()); err != nil {
		return err
	}
	err = self.updateChannelOffsetLocal(topic, partition, channel, offset)
	if err != nil {
		return err
	}
	// rpc call to slaves
	successNum := 0
	isrList := topicCoord.topicInfo.ISR
	for _, nodeID := range isrList {
		c, err := self.acquireRpcClient(nodeID)
		if err != nil {
			coordLog.Infof("get rpc client failed: %v", err)
			continue
		}

		err = c.UpdateChannelOffset(&topicCoord.topicLeaderSession, &topicCoord.topicInfo, channel, offset)
		if err != nil {
			coordLog.Warningf("node %v update offset failed %v.", nodeID, err)
		} else {
			successNum++
		}
	}
	if successNum != len(isrList) {
		coordLog.Warningf("some nodes in isr is not synced with channel consumer offset.")
		return ErrWriteQuorumFailed
	}
	return nil
}

// flush cached data to disk. This should be called when topic isr list
// changed or leader changed.
func (self *NsqdCoordinator) notifyFlushData(topic string, partition int) {
	if len(self.flushNotifyChan) > 1 {
		return
	}
	self.flushNotifyChan <- TopicPartitionID{topic, partition}
}

func (self *NsqdCoordinator) readMessageData(logID int64, fileOffset int64) ([]byte, error) {
	return nil, nil
}

func (self *NsqdCoordinator) updateLocalTopic(topicInfo TopicPartionMetaInfo) *CoordErr {
	// check topic exist and prepare on local.
	t := self.localNsqd.GetTopic(topicInfo.Name, topicInfo.Partition)
	if t == nil {
		return ErrLocalGetTopicFailed
	}
	return nil
}

func (self *NsqdCoordinator) updateLocalTopicChannels(topicInfo TopicPartionMetaInfo) *CoordErr {
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
				time.Sleep(time.Second * 30)
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
