package consistence

import (
	"bytes"
	"encoding/binary"
	"github.com/absolute8511/nsq/nsqd"
	"io"
	"net"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	MAX_WRITE_RETRY   = 10
	MAX_CATCHUP_RETRY = 5
	MAX_LOG_PULL      = 16
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

type TopicPartitionID struct {
	TopicName      string
	TopicPartition int
}

func DecodeMessagesFromRaw(data []byte, msgs []*nsqd.Message, tmpbuf []byte) ([]*nsqd.Message, error) {
	msgs = msgs[:0]
	size := int32(len(data))
	current := int32(0)
	for current < size {
		if size-current < 4 {
			return msgs, io.ErrUnexpectedEOF
		}
		msgSize := int32(binary.BigEndian.Uint32(data[current : current+4]))
		current += 4
		if current+msgSize > size {
			return msgs, io.ErrUnexpectedEOF
		}
		buf := data[current : current+msgSize]
		msg, err := nsqd.DecodeMessage(buf)
		if err != nil {
			return msgs, err
		}
		current += msgSize
		msgs = append(msgs, msg)
	}
	return msgs, nil
}

type NsqdCoordinator struct {
	clusterKey             string
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
	if self.leadership != nil {
		self.leadership.InitClusterID(self.clusterKey)
		err := self.leadership.RegisterNsqd(self.myNode)
		if err != nil {
			coordLog.Warningf("failed to register nsqd coordinator: %v", err)
			return err
		}
	}

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
	coordLog.Infof("get lookup remote : %v", self.lookupLeader)
	if err == nil {
		return c, nil
	}
	coordLog.Infof("get lookup remote failed: %v", err)
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
		states = make(map[int]bool)
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
		if tc, err := self.getTopicCoordData(topicName, partition); err == nil && tc != nil {
			// already loaded
			if tc.topicLeaderSession.LeaderNode == nil || tc.topicLeaderSession.Session == "" {
				if tc.topicInfo.Leader == self.myNode.GetID() {
					err := self.acquireTopicLeader(&tc.topicInfo)
					if err != nil {
						coordLog.Warningf("failed to acquire leader : %v", err)
					}
				}
				if FindSlice(tc.topicInfo.ISR, self.myNode.GetID()) != -1 {
					topicLeaderSession, err := self.leadership.GetTopicLeaderSession(topicName, partition)
					if err != nil {
						coordLog.Infof("failed to get topic leader info:%v-%v, err:%v", topicName, partition, err)
					} else {
						coord, err := self.getTopicCoord(topicName, partition)
						if err == nil {
							if topicLeaderSession.LeaderEpoch >= tc.topicLeaderSession.LeaderEpoch {
								coord.topicLeaderSession = *topicLeaderSession
							}
						}
					}
				}
			}
			continue
		}
		coordLog.Infof("loading topic: %v-%v", topicName, partition)
		topicInfo, err := self.leadership.GetTopicInfo(topicName, partition)
		if err != nil {
			coordLog.Infof("failed to get topic info:%v-%v, err:%v", topicName, partition, err)
			if err == ErrTopicInfoNotFound {
				self.localNsqd.CloseExistingTopic(topicName, partition)
			}
			continue
		}
		shouldLoad := FindSlice(topicInfo.ISR, self.myNode.GetID()) != -1 || FindSlice(topicInfo.CatchupList, self.myNode.GetID()) != -1
		if shouldLoad {
			basepath := GetTopicPartitionBasePath(self.dataRootPath, topicInfo.Name, topicInfo.Partition)
			tc, err := NewTopicCoordinator(topicInfo.Name, topicInfo.Partition, basepath)
			if err != nil {
				coordLog.Infof("failed to get topic coordinator:%v-%v, err:%v", topicName, partition, err)
				continue
			}
			tc.topicInfo = *topicInfo
			topicLeaderSession, err := self.leadership.GetTopicLeaderSession(topicName, partition)
			if err != nil {
				coordLog.Infof("failed to get topic leader info:%v-%v, err:%v", topicName, partition, err)
			} else {
				tc.topicLeaderSession = *topicLeaderSession
			}
			self.coordMutex.Lock()
			coords, ok := self.topicCoords[topicInfo.Name]
			if !ok {
				coords = make(map[int]*TopicCoordinator)
				self.topicCoords[topicInfo.Name] = coords
			}
			coords[topicInfo.Partition] = tc
			self.coordMutex.Unlock()
		} else {
			continue
		}

		if topicInfo.Leader == self.myNode.GetID() {
			coordLog.Infof("topic starting as leader.")
			err := self.acquireTopicLeader(topicInfo)
			if err != nil {
				coordLog.Warningf("failed to acquire leader while start as leader: %v", err)
			}
		}
		if FindSlice(topicInfo.ISR, self.myNode.GetID()) != -1 {
			coordLog.Infof("topic starting as isr .")
			// trigger the lookup push the newest leadership info
			//err := self.notifyReadyForTopicISR(topicInfo, nil)
			//if err != nil {
			//	coordLog.Warningf("failed to notify ready for isr: %v", err)
			//}
		} else if FindSlice(topicInfo.CatchupList, self.myNode.GetID()) != -1 {
			coordLog.Infof("topic starting as catchup")
			go self.catchupFromLeader(*topicInfo, "")
		} else {
			coordLog.Infof("topic starting as not relevant")
			if len(topicInfo.ISR) >= topicInfo.Replica {
				coordLog.Infof("no need load the local topic since the replica is enough: %v-%v", topicName, partition)
				self.localNsqd.CloseExistingTopic(topicName, partition)
			} else if len(topicInfo.ISR)+len(topicInfo.CatchupList) < topicInfo.Replica {
				self.requestJoinCatchup(topicName, partition)
			}
		}
		self.markLocalDataState(topicInfo.Name, topicInfo.Partition, true)
	}
	return nil
}

func (self *NsqdCoordinator) checkLocalTopicForISR(tc *coordData) *CoordErr {
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
	leaderID, err := c.GetLastCommitLogID(&tc.topicInfo)
	if err != nil {
		return err
	}
	coordLog.Infof("checking if ISR synced, logid leader: %v, myself:%v", leaderID, logid)
	if leaderID > logid {
		coordLog.Infof("this node fall behand, should catchup.")
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
					go self.catchupFromLeader(*topicMeta, "")
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

func (self *NsqdCoordinator) releaseTopicLeader(topicInfo *TopicPartionMetaInfo, session string) *CoordErr {
	err := self.leadership.ReleaseTopicLeader(topicInfo.Name, topicInfo.Partition, session)
	if err != nil {
		coordLog.Infof("failed to release leader for topic(%v): %v", topicInfo.Name, err)
		return &CoordErr{err.Error(), RpcErrTopicLeaderChanged, CoordElectionErr}
	}
	return nil
}

func (self *NsqdCoordinator) acquireTopicLeader(topicInfo *TopicPartionMetaInfo) *CoordErr {
	coordLog.Infof("acquiring leader for topic(%v): %v", topicInfo.Name, self.myNode.GetID())
	err := self.leadership.AcquireTopicLeader(topicInfo.Name, topicInfo.Partition, self.myNode)
	if err != nil {
		coordLog.Infof("failed to acquire leader for topic(%v): %v", topicInfo.Name, err)
		return &CoordErr{err.Error(), RpcNoErr, CoordElectionErr}
	}
	return nil
}

func (self *NsqdCoordinator) isMineLeaderForTopic(tp *coordData) bool {
	return tp.GetLeader() == self.myNode.GetID()
}

// for isr node to check with leader
// The lookup will wait all isr sync to new leader during the leader switch
func (self *NsqdCoordinator) syncToNewLeader(topicCoord *coordData, joinSession string) *CoordErr {
	// If leadership changed, all isr nodes should sync to new leader and check
	// consistent with leader, after all isr nodes notify ready, the leader can
	// accept new write.
	coordLog.Infof("checking sync state with new leader: %v on node: %v", joinSession, self.myNode.GetID())
	err := self.checkLocalTopicForISR(topicCoord)
	if err == ErrLocalFallBehind || err == ErrLocalForwardThanLeader {
		if joinSession != "" {
			coordLog.Infof("isr begin sync with new leader")
			go self.catchupFromLeader(topicCoord.topicInfo, joinSession)
		} else {
			coordLog.Infof("isr not synced with new leader, should retry catchup")
			err := self.requestLeaveFromISR(topicCoord.topicInfo.Name, topicCoord.topicInfo.Partition)
			if err != nil {
				coordLog.Infof("request leave isr failed: %v", err)
			} else {
				self.requestJoinCatchup(topicCoord.topicInfo.Name, topicCoord.topicInfo.Partition)
			}
		}
		return err
	} else if err != nil {
		coordLog.Infof("check isr with leader err: %v", err)
		return err
	}
	if joinSession != "" && err == nil {
		rpcErr := self.notifyReadyForTopicISR(&topicCoord.topicInfo, &topicCoord.topicLeaderSession, joinSession)
		if rpcErr != nil {
			coordLog.Infof("notify I am ready for isr failed:%v ", rpcErr)
		}
	}
	return nil
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

func (self *NsqdCoordinator) notifyReadyForTopicISR(topicInfo *TopicPartionMetaInfo, leaderSession *TopicLeaderSession, joinSession string) *CoordErr {
	// notify myself is ready for isr list for current session and can accept new write.
	// The empty session will trigger the lookup send the current leader session.
	// leader session should contain the (isr list, current leader session, leader epoch), to identify the
	// the different session stage.
	c, err := self.getLookupRemoteProxy()
	if err != nil {
		return err
	}
	return c.ReadyForTopicISR(topicInfo.Name, topicInfo.Partition, self.myNode.GetID(), leaderSession, joinSession)
}

// only move from isr to catchup, if restart, we can catchup directly.
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
	topicCoord, err := self.getTopicCoordData(topic, partition)
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

func (self *NsqdCoordinator) catchupFromLeader(topicInfo TopicPartionMetaInfo, joinISRSession string) *CoordErr {
	// get local commit log from check point , and pull newer logs from leader
	tc, err := self.getTopicCoord(topicInfo.Name, topicInfo.Partition)
	if err != nil {
		coordLog.Warningf("topic(%v) catching failed since topic coordinator missing: %v", topicInfo.Name, err)
		return ErrMissingTopicCoord
	}
	if !atomic.CompareAndSwapInt32(&tc.catchupRunning, 0, 1) {
		coordLog.Debugf("topic(%v) catching already running", topicInfo.Name)
		return ErrTopicCatchupAlreadyRunning
	}
	defer atomic.StoreInt32(&tc.catchupRunning, 0)
	coordLog.Infof("local topic begin catchup : %v, join session: %v", topicInfo.GetTopicDesp(), joinISRSession)
	tc.writeHold.Lock()
	defer tc.writeHold.Unlock()
	logMgr := tc.GetData().logMgr
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
		if tc.GetData().GetLeader() != topicInfo.Leader {
			coordLog.Warningf("topic leader changed from %v to %v, abort current catchup: %v", topicInfo.Leader, tc.GetData().GetLeader(), topicInfo.GetTopicDesp())
			return ErrTopicLeaderChanged
		}
		localLogData, localErr := logMgr.GetCommitLogFromOffset(offset)
		if localErr != nil {
			offset -= int64(GetLogDataSize())
			continue
		}

		leaderOffset, leaderLogData, err := c.GetCommitLogFromOffset(&topicInfo, offset)
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
	localTopic.Lock()
	defer localTopic.Unlock()
	if offset > 0 {
		lastLog, localErr := logMgr.GetCommitLogFromOffset(offset)
		if localErr != nil {
			coordLog.Errorf("failed to get local commit log: %v", localErr)
			return &CoordErr{localErr.Error(), RpcNoErr, CoordLocalErr}
		}
		// reset the data file to (lastLog.LogID, lastLog.MsgOffset),
		// and the next message write position should be updated.
		localErr = localTopic.ResetBackendEndNoLock(nsqd.BackendOffset(lastLog.MsgOffset), lastLog.MsgCnt-1)
		if err != nil {
			coordLog.Errorf("failed to reset local topic data: %v", err)
			return &CoordErr{localErr.Error(), RpcNoErr, CoordLocalErr}
		}
		_, localErr = logMgr.TruncateToOffset(offset)
		if localErr != nil {
			coordLog.Errorf("failed to truncate local commit log: %v", localErr)
			return &CoordErr{localErr.Error(), RpcNoErr, CoordLocalErr}
		}
	} else {
		localErr = localTopic.ResetBackendEndNoLock(nsqd.BackendOffset(0), 0)
		if err != nil {
			coordLog.Errorf("failed to reset local topic data: %v", err)
			return &CoordErr{localErr.Error(), RpcNoErr, CoordLocalErr}
		}
		_, localErr = logMgr.TruncateToOffset(0)
		if localErr != nil {
			coordLog.Errorf("failed to truncate local commit log: %v", localErr)
			return &CoordErr{localErr.Error(), RpcNoErr, CoordLocalErr}
		}
	}

	synced := false
	newMsgs := make([]*nsqd.Message, 0)
	tmpBuf := make([]byte, 1000)
	leaderSession := tc.GetData().topicLeaderSession
	for {
		if tc.GetData().GetLeader() != topicInfo.Leader {
			coordLog.Warningf("topic leader changed from %v to %v, abort current catchup: %v", topicInfo.Leader, tc.GetData().GetLeader(), topicInfo.GetTopicDesp())
			return ErrTopicLeaderChanged
		}
		logs, dataList, rpcErr := c.PullCommitLogsAndData(topicInfo.Name, topicInfo.Partition, offset, MAX_LOG_PULL)
		if rpcErr != nil {
			// if not network error, something wrong with commit log file, we need return to abort.
			coordLog.Infof("error while get logs :%v, offset: %v", rpcErr, offset)
			if retryCnt > MAX_CATCHUP_RETRY {
				return &CoordErr{rpcErr.Error(), RpcCommonErr, CoordNetErr}
			}
			retryCnt++
			time.Sleep(time.Second)
			continue
		} else if len(logs) == 0 {
			synced = true
		}
		coordLog.Infof("pulled logs :%v from offset: %v", len(logs), offset)
		for i, l := range logs {
			d := dataList[i]
			// read and decode all messages
			newMsgs, localErr = DecodeMessagesFromRaw(d, newMsgs, tmpBuf)
			if localErr != nil {
				coordLog.Warningf("Failed to decode message: %v, rawData: %v, %v", localErr, len(d), d)
				return &CoordErr{localErr.Error(), RpcNoErr, CoordLocalErr}
			}
			if len(newMsgs) == 1 {
				localErr = localTopic.PutMessageOnReplica(newMsgs[0], nsqd.BackendOffset(l.MsgOffset))
				if localErr != nil {
					coordLog.Infof("Failed to put message on slave: %v, offset: %v", localErr, l.MsgOffset)
					return &CoordErr{localErr.Error(), RpcNoErr, CoordLocalErr}
				}
			} else {
				coordLog.Infof("got batch messages: %v", len(newMsgs))
				// TODO: do batch put
			}
			localErr = logMgr.AppendCommitLog(&l, true)
			if localErr != nil {
				coordLog.Infof("Failed to append local log: %v", localErr)
				return &CoordErr{localErr.Error(), RpcNoErr, CoordLocalErr}
			}
		}
		offset += int64(len(logs) * GetLogDataSize())

		if synced && joinISRSession == "" {
			// notify nsqlookupd coordinator to add myself to isr list.
			// if success, the topic leader will disable new write.
			coordLog.Infof("I am requesting join isr: %v", self.myNode.GetID())
			err := self.requestJoinTopicISR(&topicInfo)
			if err != nil {
				coordLog.Infof("request join isr failed: %v", err)
				return err
			} else {
				// request done, and the new isr and leader will be notified,
				// after we got the notify, we will re-enter with isISR = true
				return nil
			}
		} else if synced && joinISRSession != "" {
			// TODO: maybe need sync channels from leader
			logMgr.FlushCommitLogs()
			coordLog.Infof("local topic is ready for isr: %v", topicInfo.GetTopicDesp())
			rpcErr := self.notifyReadyForTopicISR(&topicInfo, &leaderSession, joinISRSession)
			if rpcErr != nil {
				coordLog.Infof("notify ready for isr failed: %v", rpcErr)
			} else {
				coordLog.Infof("my node isr synced: %v", topicInfo.GetTopicDesp())
			}
			break
		}
	}
	coordLog.Infof("local topic catchup done: %v", topicInfo.GetTopicDesp())
	return nil
}

func (self *NsqdCoordinator) updateTopicInfo(topicCoord *TopicCoordinator, shouldDisableWrite bool, newTopicInfo *TopicPartionMetaInfo) *CoordErr {
	tcData := topicCoord.GetData()
	topicCoord.dataRWMutex.Lock()
	if newTopicInfo.Epoch < topicCoord.topicInfo.Epoch {
		topicCoord.dataRWMutex.Unlock()
		coordLog.Warningf("topic (%v) info epoch is less while update: %v vs %v",
			topicCoord.topicInfo.GetTopicDesp(), newTopicInfo.Epoch, topicCoord.topicInfo.Epoch)
		return ErrEpochLessThanCurrent
	}
	coordLog.Infof("update the topic info: %v", topicCoord.topicInfo.GetTopicDesp())
	if tcData.GetLeader() == self.myNode.GetID() && newTopicInfo.Leader != self.myNode.GetID() {
		coordLog.Infof("my leader should release")
		self.releaseTopicLeader(&tcData.topicInfo, tcData.topicLeaderSession.Session)
	}
	needAcquireLeaderSession := true
	if topicCoord.IsMineLeaderSessionReady(self.myNode.GetID()) {
		needAcquireLeaderSession = false
		coordLog.Infof("leader keep unchanged: %v", newTopicInfo)
	}
	if topicCoord.topicInfo.Epoch != newTopicInfo.Epoch {
		topicCoord.topicInfo = *newTopicInfo
	}
	topicCoord.localDataLoaded = true

	err := self.updateLocalTopic(topicCoord)
	topicCoord.dataRWMutex.Unlock()
	if err != nil {
		coordLog.Warningf("init local topic failed: %v", err)
		return err
	}
	if newTopicInfo.Leader == self.myNode.GetID() {
		// not leader before and became new leader
		if tcData.GetLeader() != self.myNode.GetID() {
			coordLog.Infof("I am notified to be leader for the topic.")
			// leader switch need disable write until the lookup notify leader
			// to accept write.
			shouldDisableWrite = true
		}
		if shouldDisableWrite {
			topicCoord.DisableWrite(true)
		}
		if needAcquireLeaderSession {
			go self.acquireTopicLeader(newTopicInfo)
		}
	} else if FindSlice(newTopicInfo.ISR, self.myNode.GetID()) != -1 {
		coordLog.Infof("I am in isr list.")
	} else if FindSlice(newTopicInfo.CatchupList, self.myNode.GetID()) != -1 {
		coordLog.Infof("I am in catchup list.")
		select {
		case self.tryCheckUnsynced <- true:
		default:
		}
	}
	return nil
}

func (self *NsqdCoordinator) updateTopicLeaderSession(topicCoord *TopicCoordinator, newLS *TopicLeaderSession, joinSession string) *CoordErr {
	topicCoord.dataRWMutex.Lock()
	if newLS.LeaderEpoch < topicCoord.GetLeaderEpoch() {
		topicCoord.dataRWMutex.Unlock()
		coordLog.Infof("topic partition leadership epoch error.")
		return ErrEpochLessThanCurrent
	}
	coordLog.Infof("update the topic leader session: %v", topicCoord.topicInfo.GetTopicDesp())
	if newLS == nil {
		coordLog.Infof("leader session is lost for topic")
		topicCoord.topicLeaderSession = TopicLeaderSession{}
	} else if !topicCoord.topicLeaderSession.IsSame(newLS) {
		topicCoord.topicLeaderSession = *newLS
	}
	topicCoord.dataRWMutex.Unlock()
	tcData := topicCoord.GetData()

	topicData, err := self.localNsqd.GetExistingTopic(tcData.topicInfo.Name)
	if err != nil {
		coordLog.Infof("no topic on local: %v, %v", tcData.topicInfo.GetTopicDesp(), err)
		return ErrLocalMissingTopic
	}
	// leader changed (maybe down), we make sure out data is flushed to keep data safe
	topicData.ForceFlush()

	coordLog.Infof("topic leader session: %v", tcData.topicLeaderSession)
	if tcData.IsMineLeaderSessionReady(self.myNode.GetID()) {
		coordLog.Infof("I become the leader for the topic: %v", tcData.topicInfo.GetTopicDesp())
		topicData.EnableForMaster()
	} else {
		topicData.DisableForSlave()
		if newLS == nil || newLS.LeaderNode == nil || newLS.Session == "" {
			coordLog.Infof("topic leader is missing : %v", tcData.topicInfo.GetTopicDesp())
		} else {
			coordLog.Infof("topic %v leader changed to :%v. epoch: %v", tcData.topicInfo.GetTopicDesp(), newLS.LeaderNode.GetID(), newLS.LeaderEpoch)
			// if catching up, pull data from the new leader
			// if isr, make sure sync to the new leader
			if FindSlice(tcData.topicInfo.ISR, self.myNode.GetID()) != -1 {
				go self.syncToNewLeader(tcData, joinSession)
			} else {
				select {
				case self.tryCheckUnsynced <- true:
				default:
				}
			}
		}
	}
	return nil
}

func (self *NsqdCoordinator) getTopicCoordData(topic string, partition int) (*coordData, *CoordErr) {
	c, err := self.getTopicCoord(topic, partition)
	if err != nil {
		return nil, err
	}
	return c.GetData(), nil
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
	coord, checkErr := self.getTopicCoord(topicName, partition)
	if checkErr != nil {
		return checkErr
	}
	coord.writeHold.Lock()
	defer coord.writeHold.Unlock()

	if coord.exiting {
		return ErrTopicExiting
	}
	if coord.disableWrite {
		return ErrWriteDisabled
	}

	var clusterWriteErr *CoordErr
	tcData := coord.GetData()
	if clusterWriteErr = tcData.checkWriteForLeader(self.myNode.GetID()); clusterWriteErr != nil {
		coordLog.Warningf("topic(%v) check write failed :%v", topicName, clusterWriteErr)
		return clusterWriteErr
	}
	if !tcData.IsISRReadyForWrite() {
		coordLog.Infof("topic(%v) write failed since no enough ISR:%v", topicName, tcData.topicInfo)
		return ErrWriteQuorumFailed
	}

	logMgr := tcData.logMgr

	var commitLog CommitLogData
	needRefreshISR := false
	needLeaveISR := false
	success := 0
	retryCnt := 0

	topic.Lock()
	msg := nsqd.NewMessage(0, body)
	id, offset, writeBytes, totalCnt, putErr := topic.PutMessageNoLock(msg)
	if putErr != nil {
		coordLog.Warningf("put message to local failed: %v", putErr)
		clusterWriteErr = ErrLocalWriteFailed
		goto exitpub
	}
	needLeaveISR = true
	commitLog.LogID = int64(id)
	// epoch should not be changed.
	// leader epoch change means leadership change, leadership change
	// need disable write which should hold the write lock.
	// However, we are holding write lock while doing the cluster write replication.
	commitLog.Epoch = tcData.GetLeaderEpoch()
	commitLog.MsgOffset = int64(offset)
	commitLog.MsgSize = writeBytes
	commitLog.MsgCnt = totalCnt

retrypub:
	if retryCnt > MAX_WRITE_RETRY {
		coordLog.Warningf("write retrying times is large: %v", retryCnt)
		needRefreshISR = true
	}
	if needRefreshISR {
		tcData = coord.GetData()
		if clusterWriteErr = tcData.checkWriteForLeader(self.myNode.GetID()); clusterWriteErr != nil {
			coordLog.Warningf("topic(%v) check write failed :%v", topicName, clusterWriteErr)
			goto exitpub
		}
		logMgr = tcData.logMgr
		coordLog.Debugf("isr refreshed while write: %v", tcData)
	}
	success = 0
	retryCnt++

	// send message to slaves with current topic epoch
	// replica should check if offset matching. If not matched the replica should leave the ISR list.
	// also, the coordinator should retry on fail until all nodes in ISR success.
	// If failed, should update ISR and retry.
	// TODO: optimize send all requests first and then wait all responses
	for _, nodeID := range tcData.topicInfo.ISR {
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
		putErr := c.PutMessage(&tcData.topicLeaderSession, &tcData.topicInfo, commitLog, msg)
		if putErr == nil {
			success++
		} else {
			// check if we need to remove this replica from isr
			// isr may down or some error.
			// We also need do some work to decide if we
			// should give up my leadership.
			coordLog.Infof("sync write to replica %v failed: %v", nodeID, putErr)
			clusterWriteErr = putErr
			if !putErr.CanRetryWrite() {
				goto exitpub
			}
		}
	}

	if success == len(tcData.topicInfo.ISR) {
		localErr := logMgr.AppendCommitLog(&commitLog, false)
		if localErr != nil {
			// TODO: leave isr
			coordLog.Errorf("topic : %v failed write commit log : %v", topic.GetFullName(), localErr)
			panic(localErr)
		} else {
			needLeaveISR = false
		}
	} else {
		coordLog.Warningf("topic %v sync write message (id:%v) failed since no enough success: %v", topic.GetFullName(), msg.ID, success)
		needRefreshISR = true
		time.Sleep(time.Millisecond * time.Duration(retryCnt))
		goto retrypub
	}
exitpub:
	if needLeaveISR {
		topic.RollbackNoLock(nsqd.BackendOffset(commitLog.MsgOffset), 1)
		coordLog.Infof("topic %v begin leave from isr since write on cluster failed: %v", tcData.topicInfo.GetTopicDesp(), clusterWriteErr)
		coord.dataRWMutex.Lock()
		coord.topicInfo.Leader = ""
		coord.topicLeaderSession.LeaderNode = nil
		coord.dataRWMutex.Unlock()
		// leave isr
		tmpErr := self.requestLeaveFromISR(tcData.topicInfo.Name, tcData.topicInfo.Partition)
		if tmpErr != nil {
			coordLog.Warningf("failed to request leave from isr: %v", tmpErr)
		}
	}
	topic.Unlock()
	if clusterWriteErr != nil {
		coordLog.Infof("topic %v PutMessageToCluster error: %v", topic.GetFullName(), clusterWriteErr)
	} else {
		coordLog.Debugf("topic(%v) PutMessageToCluster done: %v", topic.GetFullName(), msg.ID)
	}
	return clusterWriteErr
}

func (self *NsqdCoordinator) PutMessagesToCluster(topic string, partition int, messages []string) error {
	_, err := self.getTopicCoord(topic, partition)
	if err != nil {
		return err
	}
	//TODO:
	return ErrWriteQuorumFailed
}

func (self *NsqdCoordinator) putMessageOnSlave(coord *TopicCoordinator, logData CommitLogData, msg *nsqd.Message) *CoordErr {
	coord.writeHold.Lock()
	defer coord.writeHold.Unlock()
	if coord.exiting {
		return ErrTopicExitingOnSlave
	}

	tc := coord.GetData()
	topicName := tc.topicInfo.Name
	partition := tc.topicInfo.Partition

	coordLog.Debugf("pub on slave : %v, msg %v", topicName, msg.ID)
	logMgr := tc.logMgr
	if logMgr.IsCommitted(logData.LogID) {
		coordLog.Infof("pub the already committed log id : %v", logData.LogID)
		return nil
	}
	var slavePubErr *CoordErr
	topic, localErr := self.localNsqd.GetExistingTopic(topicName)
	if localErr != nil {
		coordLog.Infof("pub on slave missing topic : %v", topicName)
		// leave the isr and try re-sync with leader
		slavePubErr = &CoordErr{localErr.Error(), RpcErrTopicNotExist, CoordSlaveErr}
		goto exitpubslave
	}

	if topic.GetTopicPart() != partition {
		coordLog.Errorf("topic on slave has different partition : %v vs %v", topic.GetTopicPart(), partition)
		slavePubErr = &CoordErr{ErrLocalTopicPartitionMismatch.Error(), RpcErrTopicNotExist, CoordSlaveErr}
		goto exitpubslave
	}

	topic.Lock()
	defer topic.Unlock()
	localErr = topic.PutMessageOnReplica(msg, nsqd.BackendOffset(logData.MsgOffset))
	if localErr != nil {
		coordLog.Errorf("put message on slave failed: %v", localErr)
		slavePubErr = &CoordErr{localErr.Error(), RpcCommonErr, CoordSlaveErr}
		goto exitpubslave
	}
	localErr = logMgr.AppendCommitLog(&logData, true)
	if localErr != nil {
		coordLog.Errorf("write commit log on slave failed: %v", localErr)
		slavePubErr = &CoordErr{localErr.Error(), RpcCommonErr, CoordLocalErr}
		goto exitpubslave
	}
exitpubslave:
	if slavePubErr != nil {
		coordLog.Infof("I am leaving topic %v from isr since write on slave failed: %v", topicName, slavePubErr)
		// leave isr
		tmpErr := self.requestLeaveFromISR(topicName, partition)
		if tmpErr != nil {
			coordLog.Warningf("failed to request leave from isr: %v", tmpErr)
		}
	}
	return slavePubErr
}

func (self *NsqdCoordinator) putMessagesOnSlave(topicName string, partition int, logData CommitLogData, msgs []*nsqd.Message) *CoordErr {
	if len(msgs) == 0 {
		return ErrPubArgError
	}
	if logData.LogID != int64(msgs[0].ID) {
		return ErrPubArgError
	}
	coord, err := self.getTopicCoord(topicName, partition)
	if err != nil {
		return err
	}
	coord.writeHold.Lock()
	defer coord.writeHold.Unlock()
	if coord.exiting {
		return ErrTopicExitingOnSlave
	}
	tc := coord.GetData()
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
	coord, err := self.getTopicCoord(channel.GetTopicName(), channel.GetTopicPart())
	if err != nil {
		return err
	}
	coord.writeHold.Lock()
	defer coord.writeHold.Unlock()
	if coord.exiting {
		return ErrTopicExiting
	}
	if coord.disableWrite {
		return ErrWriteDisabled
	}

	tcData := coord.GetData()

	if err = tcData.checkWriteForLeader(self.myNode.GetID()); err != nil {
		return err
	}

	if !tcData.IsISRReadyForWrite() {
		return ErrWriteQuorumFailed
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
	isrList := tcData.topicInfo.ISR
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

		err = c.UpdateChannelOffset(&tcData.topicLeaderSession, &tcData.topicInfo, channel.GetName(), syncOffset)
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

func (self *NsqdCoordinator) updateChannelOffsetOnSlave(tc *coordData, channelName string, offset ChannelConsumerOffset) *CoordErr {
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

func (self *NsqdCoordinator) readTopicRawData(topic string, partition int, offset int64, size int32) ([]byte, *CoordErr) {
	//read directly from local topic data used for pulling data by replicas
	t, err := self.localNsqd.GetExistingTopic(topic)
	if err != nil {
		return nil, ErrLocalMissingTopic
	}
	if t.GetTopicPart() != partition {
		return nil, ErrLocalTopicPartitionMismatch
	}
	snap := t.GetDiskQueueSnapshot()
	err = snap.SeekTo(nsqd.BackendOffset(offset))
	if err != nil {
		coordLog.Infof("read topic data at offset %v, size: %v, error: %v", offset, size, err)
		return nil, ErrLocalTopicDataCorrupt
	}
	buf, err := snap.ReadRaw(size)
	if err != nil {
		coordLog.Infof("read topic data at offset %v error: %v", offset, size, err)
		return nil, ErrLocalTopicDataCorrupt
	}
	return buf, nil
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
		return ErrLocalInitTopicFailed
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
	self.coordMutex.RLock()
	defer self.coordMutex.RUnlock()
	for topicName, topicData := range self.topicCoords {
		for pid, tpCoord := range topicData {
			tpCoord.Exiting()
			tcData := tpCoord.GetData()
			if FindSlice(tcData.topicInfo.ISR, self.myNode.GetID()) == -1 {
				continue
			}
			if len(tcData.topicInfo.ISR)-1 <= tcData.topicInfo.Replica/2 {
				coordLog.Infof("The isr nodes in topic %v is not enough while leaving: %v",
					tpCoord.topicInfo.GetTopicDesp(), tpCoord.topicInfo.ISR)
			}

			if tcData.topicLeaderSession.LeaderNode.GetID() == self.myNode.GetID() {
				// leader
				self.leadership.ReleaseTopicLeader(topicName, pid, tcData.topicLeaderSession.Session)
				coordLog.Infof("The leader for topic %v is transfered.", tcData.topicInfo.GetTopicDesp())
			}
			// wait lookup choose new node for isr/leader
			self.requestLeaveFromISR(topicName, pid)
		}
	}
	coordLog.Infof("prepare leaving finished.")
}
