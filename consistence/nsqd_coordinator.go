package consistence

import (
	"bytes"
	"encoding/binary"
	"github.com/absolute8511/nsq/internal/levellogger"
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
	MAX_WRITE_RETRY    = 10
	MAX_CATCHUP_RETRY  = 5
	MAX_LOG_PULL       = 1000
	MAX_LOG_PULL_BYTES = 1024 * 1024 * 32
)

var (
	MaxRetryWait = time.Second * 10
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
	lookupLeader           NsqLookupdNodeInfo
	lookupRemoteCreateFunc nsqlookupRemoteProxyCreateFunc
	topicCoords            map[string]map[int]*TopicCoordinator
	coordMutex             sync.RWMutex
	myNode                 NsqdNodeInfo
	rpcClientMutex         sync.Mutex
	nsqdRpcClients         map[string]*NsqdRpcClient
	flushNotifyChan        chan TopicPartitionID
	stopChan               chan struct{}
	dataRootPath           string
	localNsqd              *nsqd.NSQD
	rpcServer              *NsqdCoordRpcServer
	tryCheckUnsynced       chan bool
	wg                     sync.WaitGroup
	stopping               bool
}

func NewNsqdCoordinator(cluster, ip, tcpport, rpcport, extraID string, rootPath string, nsqd *nsqd.NSQD) *NsqdCoordinator {
	nodeInfo := NsqdNodeInfo{
		NodeIP:  ip,
		TcpPort: tcpport,
		RpcPort: rpcport,
	}
	nodeInfo.ID = GenNsqdNodeID(&nodeInfo, extraID)
	nsqdCoord := &NsqdCoordinator{
		clusterKey:             cluster,
		leadership:             nil,
		topicCoords:            make(map[string]map[int]*TopicCoordinator),
		myNode:                 nodeInfo,
		nsqdRpcClients:         make(map[string]*NsqdRpcClient),
		flushNotifyChan:        make(chan TopicPartitionID, 2),
		stopChan:               make(chan struct{}),
		dataRootPath:           rootPath,
		localNsqd:              nsqd,
		tryCheckUnsynced:       make(chan bool, 1),
		lookupRemoteCreateFunc: NewNsqLookupRpcClient,
	}

	if nsqdCoord.leadership != nil {
		nsqdCoord.leadership.InitClusterID(nsqdCoord.clusterKey)
	}
	nsqdCoord.rpcServer = NewNsqdCoordRpcServer(nsqdCoord, rootPath)
	return nsqdCoord
}

func (self *NsqdCoordinator) SetLeadershipMgr(l NSQDLeadership) {
	self.leadership = l
	if self.leadership != nil {
		self.leadership.InitClusterID(self.clusterKey)
	}
}

func (self *NsqdCoordinator) acquireRpcClient(nid string) (*NsqdRpcClient, *CoordErr) {
	self.rpcClientMutex.Lock()
	defer self.rpcClientMutex.Unlock()
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
		err := self.leadership.RegisterNsqd(&self.myNode)
		if err != nil {
			coordLog.Warningf("failed to register nsqd coordinator: %v", err)
			return err
		}
	}
	go self.rpcServer.start(self.myNode.NodeIP, self.myNode.RpcPort)
	self.wg.Add(1)
	go self.watchNsqLookupd()

	start := time.Now()
	for {
		if self.lookupLeader.GetID() != "" {
			break
		}
		time.Sleep(time.Millisecond * 100)
		coordLog.Infof("waiting for lookupd ...")
		if time.Now().Sub(start) > time.Second*10 {
			panic("no lookupd found while starting nsqd coordinator")
		}
	}

	err := self.loadLocalTopicData()
	if err != nil {
		close(self.stopChan)
		self.rpcServer.stop()
		return err
	}

	self.wg.Add(1)
	go self.checkForUnsyncedTopics()
	self.wg.Add(1)
	go self.periodFlushCommitLogs()
	return nil
}

func (self *NsqdCoordinator) Stop() {
	// give up the leadership on the topic to
	// allow other isr take over to avoid electing.
	self.prepareLeavingCluster()
	close(self.stopChan)
	self.rpcServer.stop()
	self.rpcServer = nil
	for _, c := range self.nsqdRpcClients {
		c.Close()
	}
	self.wg.Wait()
}

func (self *NsqdCoordinator) periodFlushCommitLogs() {
	tmpCoords := make(map[string]map[int]*TopicCoordinator)
	syncChannelCounter := 0
	defer self.wg.Done()
	flushTicker := time.NewTicker(time.Second)
	doFlush := func() {
		syncChannelCounter++
		self.coordMutex.RLock()
		for name, tc := range self.topicCoords {
			coords, ok := tmpCoords[name]
			if !ok {
				coords = make(map[int]*TopicCoordinator)
				tmpCoords[name] = coords
			}
			for pid, tpc := range tc {
				coords[pid] = tpc
			}
		}
		self.coordMutex.RUnlock()
		for _, tc := range tmpCoords {
			for pid, tpc := range tc {
				tcData := tpc.GetData()
				if tcData.GetLeader() == self.myNode.GetID() {
					tcData.logMgr.FlushCommitLogs()
				} else if syncChannelCounter%10 == 0 {
					tcData.logMgr.FlushCommitLogs()
				}
				if syncChannelCounter%2 == 0 && tcData.GetLeader() == self.myNode.GetID() {
					self.trySyncTopicChannels(tcData)
				}
				delete(tc, pid)
			}
		}
	}
	for {
		select {
		case <-flushTicker.C:
			doFlush()
		case <-self.stopChan:
			time.Sleep(time.Second)
			doFlush()
			return
		}
	}
}

func (self *NsqdCoordinator) getLookupRemoteProxy() (INsqlookupRemoteProxy, *CoordErr) {
	c, err := self.lookupRemoteCreateFunc(net.JoinHostPort(self.lookupLeader.NodeIP, self.lookupLeader.RpcPort), RPC_TIMEOUT)
	if err == nil {
		return c, nil
	}
	coordLog.Infof("get lookup remote %v failed: %v", self.lookupLeader, err)
	return c, NewCoordErr(err.Error(), CoordNetErr)
}

func (self *NsqdCoordinator) GetCurrentLookupd() NsqLookupdNodeInfo {
	return self.lookupLeader
}

func (self *NsqdCoordinator) GetAllLookupdNodes() ([]NsqLookupdNodeInfo, error) {
	return self.leadership.GetAllLookupdNodes()
}

func (self *NsqdCoordinator) watchNsqLookupd() {
	// watch the leader of nsqlookupd, always check the leader before response
	// to the nsqlookup admin operation.
	nsqlookupLeaderChan := make(chan *NsqLookupdNodeInfo, 1)
	if self.leadership != nil {
		go self.leadership.WatchLookupdLeader(nsqlookupLeaderChan, self.stopChan)
	}
	defer self.wg.Done()
	for {
		select {
		case n, ok := <-nsqlookupLeaderChan:
			if !ok {
				return
			}
			if n.GetID() != self.lookupLeader.GetID() ||
				n.Epoch != self.lookupLeader.Epoch {
				coordLog.Infof("nsqlookup leader changed: %v", n)
				self.lookupLeader = *n
			}
		}
	}
}

func (self *NsqdCoordinator) loadLocalTopicData() error {
	if self.localNsqd == nil {
		return nil
	}
	topicMap := self.localNsqd.GetTopicMapCopy()

	for topicName, topicParts := range topicMap {
		for _, topic := range topicParts {
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
			if topicName == "" {
				continue
			}
			topicInfo, commonErr := self.leadership.GetTopicInfo(topicName, partition)
			if commonErr != nil {
				coordLog.Infof("failed to get topic info:%v-%v, err:%v", topicName, partition, commonErr)
				if commonErr == ErrKeyNotFound {
					self.localNsqd.CloseExistingTopic(topicName, partition)
				}
				continue
			}
			topic.SetAutoCommit(false)
			shouldLoad := FindSlice(topicInfo.ISR, self.myNode.GetID()) != -1 || FindSlice(topicInfo.CatchupList, self.myNode.GetID()) != -1
			if shouldLoad {
				basepath := GetTopicPartitionBasePath(self.dataRootPath, topicInfo.Name, topicInfo.Partition)
				tc, err := NewTopicCoordinator(topicInfo.Name, topicInfo.Partition, basepath, topicInfo.SyncEvery)
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
				coordLog.Infof("topic %v starting as leader.", topicInfo.GetTopicDesp())
				err := self.acquireTopicLeader(topicInfo)
				if err != nil {
					coordLog.Warningf("failed to acquire leader while start as leader: %v", err)
				}
			}
			if FindSlice(topicInfo.ISR, self.myNode.GetID()) != -1 {
				coordLog.Infof("topic starting as isr .")
				if len(topicInfo.ISR) > 1 {
					go self.requestLeaveFromISR(topicInfo.Name, topicInfo.Partition)
				}
			} else if FindSlice(topicInfo.CatchupList, self.myNode.GetID()) != -1 {
				coordLog.Infof("topic %v starting as catchup", topicInfo.GetTopicDesp())
				go self.catchupFromLeader(*topicInfo, "")
			} else {
				coordLog.Infof("topic %v starting as not relevant", topicInfo.GetTopicDesp())
				if len(topicInfo.ISR) >= topicInfo.Replica {
					coordLog.Infof("no need load the local topic since the replica is enough: %v-%v", topicName, partition)
					self.localNsqd.CloseExistingTopic(topicName, partition)
				} else if len(topicInfo.ISR)+len(topicInfo.CatchupList) < topicInfo.Replica {
					go self.requestJoinCatchup(topicName, partition)
				}
			}
		}
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
	defer self.wg.Done()
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

func (self *NsqdCoordinator) releaseTopicLeader(topicInfo *TopicPartitionMetaInfo, session *TopicLeaderSession) *CoordErr {
	err := self.leadership.ReleaseTopicLeader(topicInfo.Name, topicInfo.Partition, session)
	if err != nil {
		coordLog.Infof("failed to release leader for topic(%v): %v", topicInfo.Name, err)
		return &CoordErr{err.Error(), RpcErrTopicLeaderChanged, CoordElectionErr}
	}
	return nil
}

func (self *NsqdCoordinator) acquireTopicLeader(topicInfo *TopicPartitionMetaInfo) *CoordErr {
	coordLog.Infof("acquiring leader for topic(%v): %v", topicInfo.Name, self.myNode.GetID())
	// TODO: leader channel should be closed if not success,
	// how to handle acquire twice by the same node?
	err := self.leadership.AcquireTopicLeader(topicInfo.Name, topicInfo.Partition, &self.myNode, topicInfo.Epoch)
	if err != nil {
		coordLog.Infof("failed to acquire leader for topic(%v): %v", topicInfo.Name, err)
		return &CoordErr{err.Error(), RpcNoErr, CoordElectionErr}
	}

	coordLog.Infof("acquiring leader for topic(%v) success", topicInfo.Name)
	return nil
}

func (self *NsqdCoordinator) IsMineLeaderForTopic(topic string, part int) bool {
	tcData, err := self.getTopicCoordData(topic, part)
	if err != nil {
		return false
	}
	return tcData.GetLeader() == self.myNode.GetID() && tcData.GetLeaderSessionID() == self.myNode.GetID()
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
		// TODO: only sync with leader when write is disabled,
		// otherwise, we may miss to get the un-commit logs during write.
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
	defer c.Close()
	err = c.RequestJoinCatchup(topic, partition, self.myNode.GetID())
	if err != nil {
		coordLog.Infof("request join catchup failed: %v", err)
	}
	return err
}

func (self *NsqdCoordinator) requestJoinTopicISR(topicInfo *TopicPartitionMetaInfo) *CoordErr {
	// request change catchup to isr list and wait for nsqlookupd response to temp disable all new write.
	c, err := self.getLookupRemoteProxy()
	if err != nil {
		return err
	}
	defer c.Close()
	err = c.RequestJoinTopicISR(topicInfo.Name, topicInfo.Partition, self.myNode.GetID())
	return err
}

func (self *NsqdCoordinator) notifyReadyForTopicISR(topicInfo *TopicPartitionMetaInfo, leaderSession *TopicLeaderSession, joinSession string) *CoordErr {
	// notify myself is ready for isr list for current session and can accept new write.
	// leader session should contain the (isr list, current leader session, leader epoch), to identify the
	// the different session stage.
	c, err := self.getLookupRemoteProxy()
	if err != nil {
		return err
	}
	defer c.Close()
	return c.ReadyForTopicISR(topicInfo.Name, topicInfo.Partition, self.myNode.GetID(), leaderSession, joinSession)
}

// only move from isr to catchup, if restart, we can catchup directly.
func (self *NsqdCoordinator) requestLeaveFromISR(topic string, partition int) *CoordErr {
	c, err := self.getLookupRemoteProxy()
	if err != nil {
		return err
	}
	defer c.Close()
	return c.RequestLeaveFromISR(topic, partition, self.myNode.GetID())
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
	defer c.Close()
	return c.RequestLeaveFromISRByLeader(topic, partition, self.myNode.GetID(), &topicCoord.topicLeaderSession)
}

func (self *NsqdCoordinator) catchupFromLeader(topicInfo TopicPartitionMetaInfo, joinISRSession string) *CoordErr {
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
	coordLog.Infof("topic %v local commit log match leader %v at: %v", topicInfo.GetTopicDesp(), topicInfo.Leader, offset)
	localTopic, localErr := self.localNsqd.GetExistingTopic(topicInfo.Name, topicInfo.Partition)
	if localErr != nil {
		coordLog.Errorf("get local topic failed:%v", localErr)
		return ErrLocalMissingTopic
	}
	if localTopic.GetTopicPart() != topicInfo.Partition {
		coordLog.Errorf("local topic partition mismatch:%v vs %v", topicInfo.Partition, localTopic.GetTopicPart())
		return ErrLocalTopicPartitionMismatch
	}
	if offset > 0 {
		lastLog, localErr := logMgr.GetCommitLogFromOffset(offset)
		if localErr != nil {
			coordLog.Errorf("failed to get local commit log: %v", localErr)
			return &CoordErr{localErr.Error(), RpcNoErr, CoordLocalErr}
		}
		// reset the data file to (lastLog.LogID, lastLog.MsgOffset),
		// and the next message write position should be updated.
		localTopic.Lock()
		localErr = localTopic.ResetBackendEndNoLock(nsqd.BackendOffset(lastLog.MsgOffset), lastLog.MsgCnt-1)
		localTopic.Unlock()
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
		localTopic.Lock()
		localErr = localTopic.ResetBackendEndNoLock(nsqd.BackendOffset(0), 0)
		localTopic.Unlock()
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
			coordLog.Infof("topic %v error while get logs :%v, offset: %v", topicInfo.GetTopicDesp(), rpcErr, offset)
			if retryCnt > MAX_CATCHUP_RETRY {
				return &CoordErr{rpcErr.Error(), RpcCommonErr, CoordNetErr}
			}
			retryCnt++
			time.Sleep(time.Second)
			continue
		} else if len(logs) == 0 {
			synced = true
		}
		coordLog.Infof("topic %v pulled logs :%v from offset: %v", topicInfo.GetTopicDesp(), len(logs), offset)
		localTopic.Lock()
		hasErr := false
		var lastCommitOffset nsqd.BackendQueueEnd
		for i, l := range logs {
			d := dataList[i]
			// read and decode all messages
			newMsgs, localErr = DecodeMessagesFromRaw(d, newMsgs, tmpBuf)
			if localErr != nil || len(newMsgs) == 0 {
				coordLog.Warningf("Failed to decode message: %v, rawData: %v, %v, decoded len: %v", localErr, len(d), d, len(newMsgs))
				hasErr = true
				break
			}
			lastMsgLogID := int64(newMsgs[len(newMsgs)-1].ID)
			var queueEnd nsqd.BackendQueueEnd
			if len(newMsgs) == 1 {
				queueEnd, localErr = localTopic.PutMessageOnReplica(newMsgs[0], nsqd.BackendOffset(l.MsgOffset))
				if localErr != nil {
					coordLog.Infof("Failed to put message on slave: %v, offset: %v", localErr, l.MsgOffset)
					hasErr = true
					break
				}
				lastCommitOffset = queueEnd
			} else {
				coordLog.Debugf("got batch messages: %v", len(newMsgs))
				queueEnd, localErr = localTopic.PutMessagesOnReplica(newMsgs, nsqd.BackendOffset(l.MsgOffset))
				if localErr != nil {
					coordLog.Infof("Failed to batch put messages on slave: %v, offset: %v", localErr, l.MsgOffset)
					hasErr = true
					break
				}
				lastCommitOffset = queueEnd
			}
			if l.LastMsgLogID != lastMsgLogID {
				coordLog.Infof("Failed to put message on slave since last log id mismatch %v, %v", l, lastMsgLogID)
				localErr = ErrCommitLogWrongLastID
				hasErr = true
				break
			}
			localErr = logMgr.AppendCommitLog(&l, true)
			if localErr != nil {
				coordLog.Infof("Failed to append local log: %v", localErr)
				hasErr = true
				break
			}
		}
		logMgr.FlushCommitLogs()
		localTopic.UpdateCommittedOffset(lastCommitOffset)
		localTopic.Unlock()
		if hasErr {
			return &CoordErr{localErr.Error(), RpcNoErr, CoordLocalErr}
		}
		offset += int64(len(logs) * GetLogDataSize())

		if synced && joinISRSession == "" {
			// notify nsqlookupd coordinator to add myself to isr list.
			// if success, the topic leader will disable new write.
			coordLog.Infof("I am requesting join isr: %v", self.myNode.GetID())
			go func() {
				err := self.requestJoinTopicISR(&topicInfo)
				if err != nil {
					coordLog.Infof("request join isr failed: %v", err)
				}
			}()
			break
		} else if synced && joinISRSession != "" {
			// TODO: maybe need sync channels from leader
			logMgr.FlushCommitLogs()
			logMgr.switchForMaster(false)
			localTopic.ForceFlush()
			coordLog.Infof("local topic is ready for isr: %v", topicInfo.GetTopicDesp())
			go func() {
				rpcErr := self.notifyReadyForTopicISR(&topicInfo, &leaderSession, joinISRSession)
				if rpcErr != nil {
					coordLog.Infof("notify ready for isr failed: %v", rpcErr)
				} else {
					coordLog.Infof("my node isr synced: %v", topicInfo.GetTopicDesp())
				}
			}()
			break
		}
	}
	coordLog.Infof("local topic catchup done: %v", topicInfo.GetTopicDesp())
	return nil
}

func (self *NsqdCoordinator) updateTopicInfo(topicCoord *TopicCoordinator, shouldDisableWrite bool, newTopicInfo *TopicPartitionMetaInfo) *CoordErr {
	if self.stopping {
		return ErrClusterChanged
	}
	oldData := topicCoord.GetData()
	if oldData.topicInfo.Name == "" {
		coordLog.Infof("empty topic name not allowed")
		return ErrTopicArgError
	}
	if FindSlice(oldData.topicInfo.ISR, self.myNode.GetID()) == -1 &&
		FindSlice(newTopicInfo.ISR, self.myNode.GetID()) != -1 {
		coordLog.Infof("I am notified to be a new node in ISR: %v", self.myNode.GetID())
		topicCoord.DisableWrite(true)
	}
	disableWrite := topicCoord.GetData().disableWrite
	topicCoord.dataRWMutex.Lock()
	if newTopicInfo.Epoch < topicCoord.topicInfo.Epoch {
		coordLog.Warningf("topic (%v) info epoch is less while update: %v vs %v",
			topicCoord.topicInfo.GetTopicDesp(), newTopicInfo.Epoch, topicCoord.topicInfo.Epoch)

		topicCoord.dataRWMutex.Unlock()
		return ErrEpochLessThanCurrent
	}
	// if any of new node in isr or leader is changed, the write disabled should be set first on isr nodes.
	if newTopicInfo.Epoch != topicCoord.topicInfo.Epoch {
		if !disableWrite && newTopicInfo.Leader != "" && FindSlice(newTopicInfo.ISR, self.myNode.GetID()) != -1 {
			if newTopicInfo.Leader != topicCoord.topicInfo.Leader || len(newTopicInfo.ISR) > len(topicCoord.topicInfo.ISR) {
				coordLog.Errorf("should disable the write before changing the leader or isr of topic")
				topicCoord.dataRWMutex.Unlock()
				return ErrTopicCoordStateInvalid
			}
			// note: removing failed node no need to disable write.
			for _, newNode := range newTopicInfo.ISR {
				if FindSlice(topicCoord.topicInfo.ISR, newNode) == -1 {
					coordLog.Errorf("should disable the write before adding new ISR node ")
					topicCoord.dataRWMutex.Unlock()
					return ErrTopicCoordStateInvalid
				}
			}
		}
	}
	if topicCoord.IsExiting() {
		coordLog.Infof("update the topic info: %v while exiting.", oldData.topicInfo.GetTopicDesp())
		topicCoord.dataRWMutex.Unlock()
		return nil
	}

	coordLog.Infof("update the topic info: %v", topicCoord.topicInfo.GetTopicDesp())
	if oldData.GetLeader() == self.myNode.GetID() && newTopicInfo.Leader != self.myNode.GetID() {
		coordLog.Infof("my leader should release: %v", oldData)
		self.releaseTopicLeader(&oldData.topicInfo, &oldData.topicLeaderSession)
	}
	needAcquireLeaderSession := true
	if topicCoord.IsMineLeaderSessionReady(self.myNode.GetID()) {
		needAcquireLeaderSession = false
		coordLog.Infof("leader keep unchanged: %v", newTopicInfo)
	} else if topicCoord.GetLeader() == self.myNode.GetID() {
		coordLog.Infof("leader session not ready: %v", topicCoord.topicLeaderSession)
	}
	if topicCoord.topicInfo.Epoch != newTopicInfo.Epoch {
		topicCoord.topicInfo = *newTopicInfo
	}
	topicCoord.localDataLoaded = true
	topicCoord.dataRWMutex.Unlock()

	localTopic, err := self.updateLocalTopic(topicCoord.GetData())
	if err != nil {
		coordLog.Warningf("init local topic failed: %v", err)
		return err
	}
	// flush topic data and channel comsume data if any cluster topic info changed
	localTopic.ForceFlush()
	newTcData := topicCoord.GetData()
	newTcData.logMgr.FlushCommitLogs()
	topicCoord.dataRWMutex.Lock()
	offsetMap := make(map[string]ChannelConsumerOffset)
	for chName, offset := range newTcData.channelConsumeOffset {
		offsetMap[chName] = offset
		delete(newTcData.channelConsumeOffset, chName)
	}
	topicCoord.dataRWMutex.Unlock()
	for chName, offset := range offsetMap {
		ch := localTopic.GetChannel(chName)
		currentConfirmed := ch.GetConfirmedOffset()
		if nsqd.BackendOffset(offset.VOffset) <= currentConfirmed {
			continue
		}
		currentEnd := ch.GetChannelEnd()
		if nsqd.BackendOffset(offset.VOffset) > currentEnd {
			continue
		}
		err := ch.ConfirmBackendQueueOnSlave(nsqd.BackendOffset(offset.VOffset))
		if err != nil {
			coordLog.Warningf("update local channel(%v) offset %v failed: %v, current channel end: %v, topic end: %v",
				chName, offset, err, currentEnd, localTopic.TotalDataSize())
		}
	}

	if newTopicInfo.Leader == self.myNode.GetID() {
		newTcData.logMgr.switchForMaster(true)
		// not leader before and became new leader
		if oldData.GetLeader() != self.myNode.GetID() {
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
	} else {
		newTcData.logMgr.switchForMaster(false)
		localTopic.DisableForSlave()
		if FindSlice(newTopicInfo.ISR, self.myNode.GetID()) != -1 {
			coordLog.Infof("I am in isr list.")
		} else if FindSlice(newTopicInfo.CatchupList, self.myNode.GetID()) != -1 {
			coordLog.Infof("I am in catchup list.")
			select {
			case self.tryCheckUnsynced <- true:
			default:
			}
		}
	}
	return nil
}

func (self *NsqdCoordinator) notifyAcquireTopicLeader(coord *coordData) *CoordErr {
	if self.stopping {
		return ErrClusterChanged
	}
	coordLog.Infof("I am notified to acquire topic leader %v.", coord.topicInfo)
	go self.acquireTopicLeader(&coord.topicInfo)
	return nil
}

func (self *NsqdCoordinator) updateTopicLeaderSession(topicCoord *TopicCoordinator, newLS *TopicLeaderSession, joinSession string) *CoordErr {
	if self.stopping {
		return ErrClusterChanged
	}
	topicCoord.dataRWMutex.Lock()
	if newLS.LeaderEpoch < topicCoord.GetLeaderSessionEpoch() {
		topicCoord.dataRWMutex.Unlock()
		coordLog.Infof("topic partition leadership epoch error.")
		return ErrEpochLessThanCurrent
	}
	coordLog.Infof("update the topic %v leader session: %v", topicCoord.topicInfo.GetTopicDesp(), newLS)
	if newLS != nil && newLS.LeaderNode != nil && topicCoord.GetLeader() != newLS.LeaderNode.GetID() {
		coordLog.Infof("topic leader info not match leader session: %v", topicCoord.GetLeader())
		topicCoord.dataRWMutex.Unlock()
		return ErrTopicLeaderSessionInvalid
	}
	if newLS == nil {
		coordLog.Infof("leader session is lost for topic")
		topicCoord.topicLeaderSession = TopicLeaderSession{}
	} else if !topicCoord.topicLeaderSession.IsSame(newLS) {
		topicCoord.topicLeaderSession = *newLS
	}
	topicCoord.dataRWMutex.Unlock()
	tcData := topicCoord.GetData()
	if topicCoord.IsExiting() {
		coordLog.Infof("update the topic info: %v while exiting.", tcData.topicInfo.GetTopicDesp())
		return nil
	}

	localTopic, err := self.localNsqd.GetExistingTopic(tcData.topicInfo.Name, tcData.topicInfo.Partition)
	if err != nil {
		coordLog.Infof("no topic on local: %v, %v", tcData.topicInfo.GetTopicDesp(), err)
		return ErrLocalMissingTopic
	}
	// leader changed (maybe down), we make sure out data is flushed to keep data safe
	localTopic.ForceFlush()
	tcData.logMgr.FlushCommitLogs()
	topicCoord.dataRWMutex.Lock()
	offsetMap := make(map[string]ChannelConsumerOffset)
	for chName, offset := range tcData.channelConsumeOffset {
		offsetMap[chName] = offset
		delete(tcData.channelConsumeOffset, chName)
	}
	topicCoord.dataRWMutex.Unlock()
	for chName, offset := range offsetMap {
		ch := localTopic.GetChannel(chName)
		currentConfirmed := ch.GetConfirmedOffset()
		if nsqd.BackendOffset(offset.VOffset) <= currentConfirmed {
			continue
		}
		currentEnd := ch.GetChannelEnd()
		if nsqd.BackendOffset(offset.VOffset) > currentEnd {
			continue
		}
		err := ch.ConfirmBackendQueueOnSlave(nsqd.BackendOffset(offset.VOffset))
		if err != nil {
			coordLog.Warningf("update local channel(%v) offset %v failed: %v, current channel end: %v, topic end: %v",
				chName, offset, err, currentEnd, localTopic.TotalDataSize())
		}
	}

	coordLog.Infof("topic leader session: %v", tcData.topicLeaderSession)
	if tcData.IsMineLeaderSessionReady(self.myNode.GetID()) {
		coordLog.Infof("I become the leader for the topic: %v", tcData.topicInfo.GetTopicDesp())
		tcData.logMgr.switchForMaster(true)
		if !tcData.disableWrite {
			localTopic.EnableForMaster()
		}
	} else {
		tcData.logMgr.switchForMaster(false)
		localTopic.DisableForSlave()
		if newLS == nil || newLS.LeaderNode == nil || newLS.Session == "" {
			coordLog.Infof("topic leader is missing : %v", tcData.topicInfo.GetTopicDesp())
			if tcData.GetLeader() == self.myNode.GetID() {
				go self.acquireTopicLeader(&tcData.topicInfo)
			}
		} else {
			coordLog.Infof("topic %v leader changed to :%v. epoch: %v", tcData.topicInfo.GetTopicDesp(), newLS.LeaderNode.GetID(), newLS.LeaderEpoch)
			// if catching up, pull data from the new leader
			// if isr, make sure sync to the new leader
			if FindSlice(tcData.topicInfo.ISR, self.myNode.GetID()) != -1 {
				coordLog.Infof("I am in isr while update leader session.")
				go self.syncToNewLeader(tcData, joinSession)
			} else if FindSlice(tcData.topicInfo.CatchupList, self.myNode.GetID()) != -1 {
				coordLog.Infof("I am in catchup while update leader session.")
				select {
				case self.tryCheckUnsynced <- true:
				default:
				}
			} else {
				coordLog.Infof("I am not relevant while update leader session.")
			}
		}
	}
	return nil
}

// since only one master is allowed on the same topic, we can get it.
func (self *NsqdCoordinator) GetMasterTopicCoordData(topic string) (int, *coordData, error) {
	self.coordMutex.RLock()
	defer self.coordMutex.RUnlock()
	if v, ok := self.topicCoords[topic]; ok {
		for pid, tc := range v {
			tcData := tc.GetData()
			if tcData.GetLeader() == self.myNode.GetID() {
				return pid, tcData, nil
			}
		}
	}
	return -1, nil, ErrMissingTopicCoord
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

func (self *NsqdCoordinator) removeTopicCoord(topic string, partition int) (*TopicCoordinator, *CoordErr) {
	var topicCoord *TopicCoordinator
	self.coordMutex.Lock()
	if v, ok := self.topicCoords[topic]; ok {
		if tc, ok := v[partition]; ok {
			topicCoord = tc
			delete(v, partition)
		}
	}
	self.coordMutex.Unlock()
	if topicCoord != nil {
		topicCoord.Delete()
		return topicCoord, nil
	}
	return nil, ErrMissingTopicCoord
}

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
	var logMgr *TopicCommitLogMgr
	var msg *nsqd.Message
	var queueEnd nsqd.BackendQueueEnd
	msg = nsqd.NewMessage(0, body)
	msg.TraceID = traceID

	doLocalWrite := func(d *coordData) *CoordErr {
		logMgr = d.logMgr
		topic.Lock()
		id, offset, writeBytes, qe, putErr := topic.PutMessageNoLock(msg)
		queueEnd = qe
		topic.Unlock()
		if putErr != nil {
			coordLog.Warningf("put message to local failed: %v", putErr)
			return ErrLocalWriteFailed
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
		commitLog.MsgCnt = queueEnd.GetTotalMsgCnt()

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
			coordLog.Errorf("topic : %v failed write commit log : %v", topic.GetFullName(), localErr)
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
		return nil
	}
	doSlaveSync := func(c *NsqdRpcClient, nodeID string, tcData *coordData) *CoordErr {
		// should retry if failed, and the slave should keep the last success write to avoid the duplicated
		putErr := c.PutMessage(&tcData.topicLeaderSession, &tcData.topicInfo, commitLog, msg)
		if putErr != nil {
			coordLog.Infof("sync write to replica %v failed: %v. put offset:%v", nodeID, putErr, commitLog)
		}
		return putErr
	}
	handleSyncResult := func(successNum int, tcData *coordData) bool {
		if successNum == len(tcData.topicInfo.ISR) {
			return true
		}
		return false
	}

	clusterErr := self.doWriteOpToCluster(topic, doLocalWrite, doLocalExit, doLocalCommit, doLocalRollback,
		doRefresh, doSlaveSync, handleSyncResult)

	return msg.ID, nsqd.BackendOffset(commitLog.MsgOffset), commitLog.MsgSize, queueEnd, clusterErr
}

func (self *NsqdCoordinator) PutMessagesToCluster(topic *nsqd.Topic,
	msgs []*nsqd.Message) error {
	var commitLog CommitLogData
	var queueEnd nsqd.BackendQueueEnd
	var logMgr *TopicCommitLogMgr

	doLocalWrite := func(d *coordData) *CoordErr {
		topic.Lock()
		logMgr = d.logMgr
		id, offset, writeBytes, totalCnt, qe, putErr := topic.PutMessagesNoLock(msgs)
		queueEnd = qe
		topic.Unlock()
		if putErr != nil {
			coordLog.Warningf("put batch messages to local failed: %v", putErr)
			return ErrLocalWriteFailed
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
		commitLog.MsgCnt = totalCnt
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
			coordLog.Errorf("topic : %v failed write commit log : %v", topic.GetFullName(), localErr)
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
		return nil
	}
	doSlaveSync := func(c *NsqdRpcClient, nodeID string, tcData *coordData) *CoordErr {
		// should retry if failed, and the slave should keep the last success write to avoid the duplicated
		putErr := c.PutMessages(&tcData.topicLeaderSession, &tcData.topicInfo, commitLog, msgs)
		if putErr != nil {
			coordLog.Infof("sync write to replica %v failed: %v, put offset: %v", nodeID, putErr, commitLog)
		}
		return putErr
	}
	handleSyncResult := func(successNum int, tcData *coordData) bool {
		if successNum == len(tcData.topicInfo.ISR) {
			return true
		}
		return false
	}
	return self.doWriteOpToCluster(topic, doLocalWrite, doLocalExit, doLocalCommit, doLocalRollback,
		doRefresh, doSlaveSync, handleSyncResult)
}

func (self *NsqdCoordinator) doWriteOpToCluster(topic *nsqd.Topic, doLocalWrite localWriteFunc,
	doLocalExit localExitFunc, doLocalCommit localCommitFunc, doLocalRollback localRollbackFunc,
	doRefresh refreshCoordFunc, doSlaveSync slaveSyncFunc, handleSyncResult handleSyncResultFunc) error {
	topicName := topic.GetTopicName()
	partition := topic.GetTopicPart()
	coord, checkErr := self.getTopicCoord(topicName, partition)
	if checkErr != nil {
		return checkErr
	}
	coord.writeHold.Lock()
	defer coord.writeHold.Unlock()

	if coord.IsExiting() {
		return ErrTopicExiting
	}
	tcData := coord.GetData()
	if tcData.disableWrite {
		return ErrWriteDisabled
	}

	var clusterWriteErr *CoordErr
	if clusterWriteErr = tcData.checkWriteForLeader(self.myNode.GetID()); clusterWriteErr != nil {
		coordLog.Warningf("topic(%v) check write failed :%v", topicName, clusterWriteErr)
		return clusterWriteErr
	}
	if !tcData.IsISRReadyForWrite() {
		coordLog.Infof("topic(%v) write failed since no enough ISR:%v", topicName, tcData.topicInfo)
		return ErrWriteQuorumFailed
	}

	needRefreshISR := false
	needLeaveISR := false
	success := 0
	failedNodes := make(map[string]struct{})
	retryCnt := uint32(0)
	exitErr := 0

	localErr := doLocalWrite(tcData)
	if localErr != nil {
		clusterWriteErr = ErrLocalWriteFailed
		goto exitpub
	}
	needLeaveISR = true

retrypub:
	if retryCnt > MAX_WRITE_RETRY {
		coordLog.Warningf("write retrying times is large: %v", retryCnt)
		needRefreshISR = true
		if coord.IsExiting() {
			clusterWriteErr = ErrTopicExiting
			goto exitpub
		}
	}
	if needRefreshISR {
		tcData = coord.GetData()
		if clusterWriteErr = tcData.checkWriteForLeader(self.myNode.GetID()); clusterWriteErr != nil {
			coordLog.Warningf("topic(%v) check write failed :%v", topicName, clusterWriteErr)
			goto exitpub
		}
		if clusterWriteErr = doRefresh(tcData); clusterWriteErr != nil {
			coordLog.Warningf("topic(%v) write failed after refresh data:%v", topicName, clusterWriteErr)
			goto exitpub
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
		// should retry if failed, and the slave should keep the last success write to avoid the duplicated
		rpcErr = doSlaveSync(c, nodeID, tcData)
		if rpcErr == nil {
			success++
		} else {
			coordLog.Infof("sync write to replica %v failed: %v", nodeID, rpcErr)
			clusterWriteErr = rpcErr
			failedNodes[nodeID] = struct{}{}
			if !rpcErr.CanRetryWrite() {
				exitErr++
				coordLog.Infof("write failed and no retry type: %v, %v", rpcErr.ErrType, exitErr)
				if exitErr > len(tcData.topicInfo.ISR)/2 {
					goto exitpub
				}
			}
		}
	}

	if handleSyncResult(success, tcData) {
		localErr := doLocalCommit()
		if localErr != nil {
			coordLog.Errorf("topic : %v failed write commit log : %v", topic.GetFullName(), localErr)
			needLeaveISR = true
			clusterWriteErr = &CoordErr{localErr.Error(), RpcCommonErr, CoordLocalErr}
		} else {
			needLeaveISR = false
			clusterWriteErr = nil
		}
	} else {
		coordLog.Warningf("topic %v sync write failed since no enough success: %v", topic.GetFullName(), success)
		if retryCnt > MAX_WRITE_RETRY && success > tcData.topicInfo.Replica/2 {
			// request lookup to remove the failed nodes from isr and keep the quorum alive.
			// isr may down or some error.
			// We also need do some work to decide if we
			// should give up my leadership.
			for nid, _ := range failedNodes {
				tmpErr := self.requestLeaveFromISRByLeader(tcData.topicInfo.Name, tcData.topicInfo.Partition, nid)
				if tmpErr != nil {
					coordLog.Warningf("failed to request remove the failed isr node: %v, %v", nid, tmpErr)
					break
				}
			}
			time.Sleep(time.Second)
		}

		needRefreshISR = true
		sleepTime := time.Millisecond * time.Duration(2<<retryCnt)
		if sleepTime < MaxRetryWait {
			time.Sleep(sleepTime)
		} else {
			time.Sleep(MaxRetryWait)
		}
		goto retrypub
	}
exitpub:
	if needLeaveISR {
		doLocalRollback()
		coord.dataRWMutex.Lock()
		coord.topicLeaderSession.LeaderNode = nil
		coord.dataRWMutex.Unlock()
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
	}
	return clusterWriteErr
}

func (self *NsqdCoordinator) putMessageOnSlave(coord *TopicCoordinator, logData CommitLogData, msg *nsqd.Message) *CoordErr {
	tcData := coord.GetData()
	topicName := tcData.topicInfo.Name
	partition := tcData.topicInfo.Partition
	var logMgr *TopicCommitLogMgr
	var topic *nsqd.Topic
	var queueEnd nsqd.BackendQueueEnd

	checkDupOnSlave := func(tc *coordData) bool {
		if coordLog.Level() >= levellogger.LOG_DETAIL {
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
		topic, localErr = self.localNsqd.GetExistingTopic(topicName, partition)
		if localErr != nil {
			coordLog.Infof("pub on slave missing topic : %v", topicName)
			// leave the isr and try re-sync with leader
			return &CoordErr{localErr.Error(), RpcErrTopicNotExist, CoordSlaveErr}
		}

		if topic.GetTopicPart() != partition {
			coordLog.Errorf("topic on slave has different partition : %v vs %v", topic.GetTopicPart(), partition)
			return &CoordErr{ErrLocalTopicPartitionMismatch.Error(), RpcErrTopicNotExist, CoordSlaveErr}
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
	tcData := coord.GetData()
	topicName := tcData.topicInfo.Name
	partition := tcData.topicInfo.Partition
	checkDupOnSlave := func(tc *coordData) bool {
		if coordLog.Level() >= levellogger.LOG_DETAIL {
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
		topic, localErr = self.localNsqd.GetExistingTopic(topicName, partition)
		if localErr != nil {
			coordLog.Infof("pub on slave missing topic : %v", topicName)
			// leave the isr and try re-sync with leader
			return &CoordErr{localErr.Error(), RpcErrTopicNotExist, CoordSlaveErr}
		}

		start := time.Now()
		topic.Lock()
		cost := time.Now().Sub(start)
		if cost > time.Second {
			coordLog.Infof("prepare write on slave local cost :%v", cost)
		}
		if coordLog.Level() >= levellogger.LOG_DETAIL {
			coordLog.Infof("prepare write on slave local cost :%v", cost)
		}

		queueEnd, localErr = topic.PutMessagesOnReplica(msgs, nsqd.BackendOffset(logData.MsgOffset))
		cost2 := time.Now().Sub(start)
		if cost2 > time.Second {
			coordLog.Infof("write local on slave cost :%v, %v", cost, cost2)
		}
		if coordLog.Level() >= levellogger.LOG_DETAIL {
			coordLog.Infof("write local on slave cost :%v", cost2)
		}

		topic.Unlock()
		if localErr != nil {
			var lastLog *CommitLogData
			lastLogOffset, err := logMgr.GetLastLogOffset()
			if err == nil {
				lastLog, _ = logMgr.GetCommitLogFromOffset(lastLogOffset)
			}
			coordLog.Errorf("put messages on slave failed: %v, slave last logid: %v, data: %v",
				localErr, logMgr.GetLastCommitLogID(), lastLog)
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
	tc := coord.GetData()

	start := time.Now()
	coord.writeHold.Lock()
	defer coord.writeHold.Unlock()
	// check should be protected by write lock to avoid the next write check during the commit log flushing.
	if checkDupOnSlave(tc) {
		return nil
	}

	if coord.IsExiting() {
		return ErrTopicExitingOnSlave
	}
	if coord.GetData().disableWrite {
		return ErrWriteDisabled
	}
	if !tc.IsMineISR(self.myNode.GetID()) {
		return ErrTopicWriteOnNonISR
	}

	cost := time.Now().Sub(start)
	if cost > time.Second {
		coordLog.Infof("prepare write on slave cost :%v", cost)
	}
	if coordLog.Level() >= levellogger.LOG_DETAIL {
		coordLog.Infof("prepare write on slave cost :%v", cost)
	}

	topicName := tc.topicInfo.Name
	partition := tc.topicInfo.Partition

	var slaveErr *CoordErr
	var localErr error
	slaveErr = doLocalWriteOnSlave(tc)
	cost2 := time.Now().Sub(start)
	if cost2 > time.Second {
		coordLog.Infof("write local on slave cost :%v, %v", cost, cost2)
	}
	if coordLog.Level() >= levellogger.LOG_DETAIL {
		coordLog.Infof("write local on slave cost :%v", cost2)
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
	cost3 := time.Now().Sub(start)
	if cost3 > time.Second {
		coordLog.Infof("write local on slave cost :%v, %v, %v", cost, cost2, cost3)
	}
	if coordLog.Level() >= levellogger.LOG_DETAIL {
		coordLog.Infof("write local on slave cost :%v", cost3)
	}

	return slaveErr
}

func (self *NsqdCoordinator) FinishMessageToCluster(channel *nsqd.Channel, clientID int64, msgID nsqd.MessageID) error {
	topicName := channel.GetTopicName()
	partition := channel.GetTopicPart()
	coord, checkErr := self.getTopicCoord(topicName, partition)
	if checkErr != nil {
		return checkErr
	}

	tcData := coord.GetData()
	var clusterWriteErr *CoordErr
	if clusterWriteErr = tcData.checkWriteForLeader(self.myNode.GetID()); clusterWriteErr != nil {
		coordLog.Warningf("topic(%v) check write failed :%v", topicName, clusterWriteErr)
		return clusterWriteErr
	}

	success := 0
	var syncOffset ChannelConsumerOffset
	offset, changed, localErr := channel.FinishMessage(clientID, msgID)
	if localErr != nil {
		coordLog.Infof("channel %v finish local msg %v error: %v", channel.GetName(), msgID, localErr)
		clusterWriteErr = ErrLocalWriteFailed
		return clusterWriteErr
	}
	// this confirmed offset not changed, so we no need to sync with replica.
	if !changed {
		return nil
	}

	// TODO: maybe use channel to aggregate all the sync of message to reduce the rpc call.
	syncOffset.VOffset = int64(offset)

	for _, nodeID := range tcData.topicInfo.ISR {
		if nodeID == self.myNode.GetID() {
			success++
			continue
		}
		c, rpcErr := self.acquireRpcClient(nodeID)
		if rpcErr != nil {
			coordLog.Infof("get rpc client %v failed: %v", nodeID, rpcErr)
			clusterWriteErr = rpcErr
			continue
		}
		// should retry if failed, and the slave should keep the last success write to avoid the duplicated
		rpcErr = c.NotifyUpdateChannelOffset(&tcData.topicLeaderSession, &tcData.topicInfo, channel.GetName(), syncOffset)
		if rpcErr == nil {
			success++
		} else {
			coordLog.Debugf("node %v update offset %v failed %v.", nodeID, syncOffset, rpcErr)
			clusterWriteErr = rpcErr
		}
	}

	if success != len(tcData.topicInfo.ISR) {
		coordLog.Debugf("some nodes in isr is not synced with channel consumer offset.")
	}

	if clusterWriteErr != nil {
		coordLog.Infof("channel %v FinishMessage %v error: %v", channel.GetName(), msgID, clusterWriteErr)
	}

	// TODO: we ignore the sync failed on slave, since the consume offset will not cause the data loss.
	// However, if ordered is supported for this channel we need to make sure the offset is synced.
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
		coordLog.Infof("slave missing topic : %v", topicName)
		// TODO: leave the isr and try re-sync with leader
		return &CoordErr{localErr.Error(), RpcCommonErr, CoordLocalErr}
	}

	if topic.GetTopicPart() != partition {
		coordLog.Errorf("topic on slave has different partition : %v vs %v", topic.GetTopicPart(), partition)
		return ErrLocalMissingTopic
	}
	ch := topic.GetChannel(channelName)
	currentEnd := ch.GetChannelEnd()
	if nsqd.BackendOffset(offset.VOffset) > currentEnd {
		coordLog.Debugf("update channel(%v) consume offset exceed end %v on slave : %v", channelName, offset, currentEnd)
		if offset.Flush {
			topic.ForceFlush()
			currentEnd = ch.GetChannelEnd()
			if nsqd.BackendOffset(offset.VOffset) > currentEnd {
				offset.VOffset = int64(currentEnd)
			}
		} else {
			// cache the offset (using map?) to reduce the slave channel flush.
			coord.dataRWMutex.Lock()
			cur, ok := coord.channelConsumeOffset[channelName]
			if !ok || cur.VOffset < offset.VOffset {
				coord.channelConsumeOffset[channelName] = offset
			}
			coord.dataRWMutex.Unlock()
			return nil
		}
	}
	err := ch.ConfirmBackendQueueOnSlave(nsqd.BackendOffset(offset.VOffset))
	if err != nil {
		coordLog.Warningf("update local channel(%v) offset %v failed: %v, current channel end: %v, topic end: %v",
			channelName, offset, err, currentEnd, topic.TotalDataSize())
		return &CoordErr{err.Error(), RpcCommonErr, CoordLocalErr}
	}
	return nil
}

func (self *NsqdCoordinator) trySyncTopicChannels(tcData *coordData) {
	localTopic, _ := self.localNsqd.GetExistingTopic(tcData.topicInfo.Name, tcData.topicInfo.Partition)
	if localTopic != nil {
		channels := localTopic.GetChannelMapCopy()
		var syncOffset ChannelConsumerOffset
		syncOffset.Flush = true
		for _, ch := range channels {
			syncOffset.VOffset = int64(ch.GetConfirmedOffset())

			for _, nodeID := range tcData.topicInfo.ISR {
				if nodeID == self.myNode.GetID() {
					continue
				}
				c, rpcErr := self.acquireRpcClient(nodeID)
				if rpcErr != nil {
					continue
				}
				rpcErr = c.UpdateChannelOffset(&tcData.topicLeaderSession, &tcData.topicInfo, ch.GetName(), syncOffset)
				if rpcErr != nil {
					coordLog.Infof("node %v update offset %v failed %v.", nodeID, syncOffset, rpcErr)
				}
			}
			// only the first channel of topic should flush.
			syncOffset.Flush = false
		}
	}
}

func (self *NsqdCoordinator) readTopicRawData(topic string, partition int, offsetList []int64, sizeList []int32) ([][]byte, *CoordErr) {
	//read directly from local topic data used for pulling data by replicas
	t, err := self.localNsqd.GetExistingTopic(topic, partition)
	if err != nil {
		return nil, ErrLocalMissingTopic
	}
	if t.GetTopicPart() != partition {
		return nil, ErrLocalTopicPartitionMismatch
	}
	dataList := make([][]byte, 0, len(offsetList))
	snap := t.GetDiskQueueSnapshot()
	for i, offset := range offsetList {
		size := sizeList[i]
		err = snap.SeekTo(nsqd.BackendOffset(offset))
		if err != nil {
			coordLog.Infof("read topic data at offset %v, size: %v, error: %v", offset, size, err)
			break
		}
		buf, err := snap.ReadRaw(size)
		if err != nil {
			coordLog.Infof("read topic data at offset %v, size:%v, error: %v", offset, size, err)
			break
		}
		dataList = append(dataList, buf)
	}
	return dataList, nil
}

// flush cached data to disk. This should be called when topic isr list
// changed or leader changed.
func (self *NsqdCoordinator) notifyFlushData(topic string, partition int) {
	select {
	case self.flushNotifyChan <- TopicPartitionID{topic, partition}:
	default:
	}
}

func (self *NsqdCoordinator) updateLocalTopic(topicCoord *coordData) (*nsqd.Topic, *CoordErr) {
	// check topic exist and prepare on local.
	t := self.localNsqd.GetTopicWithDisabled(topicCoord.topicInfo.Name, topicCoord.topicInfo.Partition)
	if t == nil {
		return nil, ErrLocalInitTopicFailed
	}
	t.SetAutoCommit(false)
	if t.MsgIDCursor == nil {
		t.MsgIDCursor = topicCoord.logMgr
	}
	return t, nil
}

// before shutdown, we transfer the leader to others to reduce
// the unavailable time.
func (self *NsqdCoordinator) prepareLeavingCluster() {
	coordLog.Infof("I am prepare leaving the cluster.")
	tmpTopicCoords := make(map[string]map[int]*TopicCoordinator, len(self.topicCoords))
	self.coordMutex.RLock()
	for t, v := range self.topicCoords {
		tmp, ok := tmpTopicCoords[t]
		if !ok {
			tmp = make(map[int]*TopicCoordinator)
			tmpTopicCoords[t] = tmp
		}
		for pid, coord := range v {
			tmp[pid] = coord
		}
	}
	self.coordMutex.RUnlock()
	for topicName, topicData := range tmpTopicCoords {
		for pid, tpCoord := range topicData {
			tcData := tpCoord.GetData()
			tcData.logMgr.FlushCommitLogs()
			if FindSlice(tcData.topicInfo.ISR, self.myNode.GetID()) == -1 {
				tpCoord.Exiting()
				continue
			}
			if len(tcData.topicInfo.ISR)-1 <= tcData.topicInfo.Replica/2 {
				coordLog.Infof("The isr nodes in topic %v is not enough while leaving: %v",
					tpCoord.topicInfo.GetTopicDesp(), tpCoord.topicInfo.ISR)
			}

			tpCoord.Exiting()
			if tcData.GetLeader() == self.myNode.GetID() {
				self.trySyncTopicChannels(tcData)
			}
			// TODO: if we release leader first, we can not transfer the leader properly,
			// if we leave isr first, we would get the state that the leader not in isr
			// wait lookup choose new node for isr/leader
			retry := 3
			for retry > 0 {
				retry--
				err := self.requestLeaveFromISR(topicName, pid)
				if err == nil {
					break
				}
				if err != nil && err.IsEqual(ErrLeavingISRWait) {
					coordLog.Infof("======= should wait leaving from isr")
					time.Sleep(time.Second)
				} else {
					coordLog.Infof("======= request leave isr failed: %v", err)
					time.Sleep(time.Millisecond * 100)
				}
			}

			if tcData.IsMineLeaderSessionReady(self.myNode.GetID()) {
				// leader
				self.leadership.ReleaseTopicLeader(topicName, pid, &tcData.topicLeaderSession)
				coordLog.Infof("The leader for topic %v is transfered.", tcData.topicInfo.GetTopicDesp())
			}
			localTopic, err := self.localNsqd.GetExistingTopic(topicName, pid)
			if err != nil {
				coordLog.Infof("no local topic")
			} else {
				localTopic.PrintCurrentStats()
				localTopic.Close()
			}
		}
	}
	coordLog.Infof("prepare leaving finished.")
	if self.leadership != nil {
		self.stopping = true
		self.leadership.UnregisterNsqd(&self.myNode)
	}
}
