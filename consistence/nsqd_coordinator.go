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
	nsqdRpcClients         map[string]*NsqdRpcClient
	flushNotifyChan        chan TopicPartitionID
	stopChan               chan struct{}
	dataRootPath           string
	localNsqd              *nsqd.NSQD
	rpcServer              *NsqdCoordRpcServer
	tryCheckUnsynced       chan bool
}

func NewNsqdCoordinator(cluster, ip, tcpport, rpcport, extraID string, rootPath string, nsqd *nsqd.NSQD) *NsqdCoordinator {
	nodeInfo := NsqdNodeInfo{
		NodeIp:  ip,
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
	self.prepareLeavingCluster()
	close(self.stopChan)
	// TODO: rpc exit should be avoid in test.
	//self.rpcServer.stop()
	//self.rpcServer = nil
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

func (self *NsqdCoordinator) releaseTopicLeader(topicInfo *TopicPartionMetaInfo, session *TopicLeaderSession) *CoordErr {
	err := self.leadership.ReleaseTopicLeader(topicInfo.Name, topicInfo.Partition, session)
	if err != nil {
		coordLog.Infof("failed to release leader for topic(%v): %v", topicInfo.Name, err)
		return &CoordErr{err.Error(), RpcErrTopicLeaderChanged, CoordElectionErr}
	}
	return nil
}

func (self *NsqdCoordinator) acquireTopicLeader(topicInfo *TopicPartionMetaInfo) *CoordErr {
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
		localTopic.Lock()
		hasErr := false
		for i, l := range logs {
			d := dataList[i]
			// read and decode all messages
			newMsgs, localErr = DecodeMessagesFromRaw(d, newMsgs, tmpBuf)
			if localErr != nil {
				coordLog.Warningf("Failed to decode message: %v, rawData: %v, %v", localErr, len(d), d)
				hasErr = true
				break
			}
			if len(newMsgs) == 1 {
				localErr = localTopic.PutMessageOnReplica(newMsgs[0], nsqd.BackendOffset(l.MsgOffset))
				if localErr != nil {
					coordLog.Infof("Failed to put message on slave: %v, offset: %v", localErr, l.MsgOffset)
					hasErr = true
					break
				}
			} else {
				coordLog.Debugf("got batch messages: %v", len(newMsgs))
				localErr = localTopic.PutMessagesOnReplica(newMsgs, nsqd.BackendOffset(l.MsgOffset))
				if localErr != nil {
					coordLog.Infof("Failed to batch put messages on slave: %v, offset: %v", localErr, l.MsgOffset)
					hasErr = true
					break
				}
			}
			localErr = logMgr.AppendCommitLog(&l, true)
			if localErr != nil {
				coordLog.Infof("Failed to append local log: %v", localErr)
				hasErr = true
				break
			}
		}
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

func (self *NsqdCoordinator) updateTopicInfo(topicCoord *TopicCoordinator, shouldDisableWrite bool, newTopicInfo *TopicPartionMetaInfo) *CoordErr {
	oldData := topicCoord.GetData()
	if FindSlice(oldData.topicInfo.ISR, self.myNode.GetID()) == -1 &&
		FindSlice(newTopicInfo.ISR, self.myNode.GetID()) != -1 {
		coordLog.Infof("I am notified to be a new node in ISR: %v", self.myNode.GetID())
		topicCoord.DisableWrite(true)
	}
	disableWrite := topicCoord.GetData().disableWrite
	topicCoord.dataRWMutex.Lock()
	if newTopicInfo.Epoch < topicCoord.topicInfo.Epoch {
		topicCoord.dataRWMutex.Unlock()
		coordLog.Warningf("topic (%v) info epoch is less while update: %v vs %v",
			topicCoord.topicInfo.GetTopicDesp(), newTopicInfo.Epoch, topicCoord.topicInfo.Epoch)
		return ErrEpochLessThanCurrent
	}
	// if any of new node in isr or leader is changed, the write disabled should be set first on isr nodes.
	if newTopicInfo.Epoch != topicCoord.topicInfo.Epoch {
		if !disableWrite && newTopicInfo.Leader != "" && FindSlice(newTopicInfo.ISR, self.myNode.GetID()) != -1 {
			if newTopicInfo.Leader != topicCoord.topicInfo.Leader || len(newTopicInfo.ISR) > len(topicCoord.topicInfo.ISR) {
				coordLog.Errorf("should disable the write before changing the leader or isr of topic")
				return ErrTopicCoordStateInvalid
			}
			// note: removing failed node no need to disable write.
			for _, newNode := range newTopicInfo.ISR {
				if FindSlice(topicCoord.topicInfo.ISR, newNode) == -1 {
					coordLog.Errorf("should disable the write before adding new ISR node ")
					return ErrTopicCoordStateInvalid
				}
			}
		}
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
	} else {
		coordLog.Infof("leader session not ready: %v", topicCoord.topicLeaderSession)
	}
	if topicCoord.topicInfo.Epoch != newTopicInfo.Epoch {
		topicCoord.topicInfo = *newTopicInfo
	}
	topicCoord.localDataLoaded = true
	topicCoord.dataRWMutex.Unlock()

	err := self.updateLocalTopic(topicCoord.GetData())
	if err != nil {
		coordLog.Warningf("init local topic failed: %v", err)
		return err
	}
	if newTopicInfo.Leader == self.myNode.GetID() {
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
	if newLS.LeaderEpoch < topicCoord.GetLeaderSessionEpoch() {
		topicCoord.dataRWMutex.Unlock()
		coordLog.Infof("topic partition leadership epoch error.")
		return ErrEpochLessThanCurrent
	}
	coordLog.Infof("update the topic %v leader session: %v", topicCoord.topicInfo.GetTopicDesp(), newLS)
	if newLS != nil && newLS.LeaderNode != nil && topicCoord.GetLeader() != newLS.LeaderNode.GetID() {
		coordLog.Infof("topic leader info not match leader session: %v", topicCoord.GetLeader())
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

	topicData, err := self.localNsqd.GetExistingTopic(tcData.topicInfo.Name, tcData.topicInfo.Partition)
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

type localWriteFunc func(*coordData) *CoordErr
type localExitFunc func(*CoordErr)
type localCommitFunc func() error
type localRollbackFunc func()
type refreshCoordFunc func(*coordData)
type slaveSyncFunc func(*NsqdRpcClient, string, *coordData) *CoordErr
type handleSyncResultFunc func(int, *coordData) bool

type checkDupFunc func(*coordData) bool

func (self *NsqdCoordinator) PutMessageToCluster(topic *nsqd.Topic, body []byte) error {
	var commitLog CommitLogData
	var logMgr *TopicCommitLogMgr
	var msg *nsqd.Message

	doLocalWrite := func(d *coordData) *CoordErr {
		topic.Lock()
		logMgr = d.logMgr
		msg = nsqd.NewMessage(0, body)
		id, offset, writeBytes, totalCnt, putErr := topic.PutMessageNoLock(msg)
		if putErr != nil {
			coordLog.Warningf("put message to local failed: %v", putErr)
			return ErrLocalWriteFailed
		}
		commitLog.LogID = int64(id)
		// epoch should not be changed.
		// leader epoch change means leadership change, leadership change
		// need disable write which should hold the write lock.
		// However, we are holding write lock while doing the cluster write replication.
		commitLog.Epoch = d.GetTopicEpoch()
		commitLog.MsgOffset = int64(offset)
		commitLog.MsgSize = writeBytes
		commitLog.MsgCnt = totalCnt

		return nil
	}
	doLocalExit := func(err *CoordErr) {
		topic.Unlock()
		if err != nil {
			coordLog.Infof("topic %v PutMessageToCluster msg %v error: %v", topic.GetFullName(), msg, err)
		}
	}
	doLocalCommit := func() error {
		localErr := logMgr.AppendCommitLog(&commitLog, false)
		if localErr != nil {
			coordLog.Errorf("topic : %v failed write commit log : %v", topic.GetFullName(), localErr)
		}
		return localErr
	}
	doLocalRollback := func() {
		topic.RollbackNoLock(nsqd.BackendOffset(commitLog.MsgOffset), 1)
	}
	doRefresh := func(d *coordData) {
		logMgr = d.logMgr
	}
	doSlaveSync := func(c *NsqdRpcClient, nodeID string, tcData *coordData) *CoordErr {
		// should retry if failed, and the slave should keep the last success write to avoid the duplicated
		putErr := c.PutMessage(&tcData.topicLeaderSession, &tcData.topicInfo, commitLog, msg)
		if putErr != nil {
			coordLog.Infof("sync write to replica %v failed: %v", nodeID, putErr)
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

func (self *NsqdCoordinator) PutMessagesToCluster(topic *nsqd.Topic, msgs []*nsqd.Message) error {
	var commitLog CommitLogData
	var logMgr *TopicCommitLogMgr

	doLocalWrite := func(d *coordData) *CoordErr {
		topic.Lock()
		logMgr = d.logMgr
		id, offset, writeBytes, totalCnt, putErr := topic.PutMessagesNoLock(msgs)
		if putErr != nil {
			coordLog.Warningf("put batch messages to local failed: %v", putErr)
			return ErrLocalWriteFailed
		}
		commitLog.LogID = int64(id)
		// epoch should not be changed.
		// leader epoch change means leadership change, leadership change
		// need disable write which should hold the write lock.
		// However, we are holding write lock while doing the cluster write replication.
		commitLog.Epoch = d.GetTopicEpoch()
		commitLog.MsgOffset = int64(offset)
		commitLog.MsgSize = writeBytes
		commitLog.MsgCnt = totalCnt
		return nil
	}
	doLocalExit := func(err *CoordErr) {
		topic.Unlock()
		if err != nil {
			coordLog.Infof("topic %v PutMessagesToCluster error: %v", topic.GetFullName(), err)
		}
	}
	doLocalCommit := func() error {
		localErr := logMgr.AppendCommitLog(&commitLog, false)
		if localErr != nil {
			coordLog.Errorf("topic : %v failed write commit log : %v", topic.GetFullName(), localErr)
		}
		return localErr
	}
	doLocalRollback := func() {
		topic.ResetBackendEndNoLock(nsqd.BackendOffset(commitLog.MsgOffset), commitLog.MsgCnt-1)
	}
	doRefresh := func(d *coordData) {
		logMgr = d.logMgr
	}
	doSlaveSync := func(c *NsqdRpcClient, nodeID string, tcData *coordData) *CoordErr {
		// should retry if failed, and the slave should keep the last success write to avoid the duplicated
		putErr := c.PutMessages(&tcData.topicLeaderSession, &tcData.topicInfo, commitLog, msgs)
		if putErr != nil {
			coordLog.Infof("sync write to replica %v failed: %v", nodeID, putErr)
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

	if coord.exiting {
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
	}
	if needRefreshISR {
		tcData = coord.GetData()
		if clusterWriteErr = tcData.checkWriteForLeader(self.myNode.GetID()); clusterWriteErr != nil {
			coordLog.Warningf("topic(%v) check write failed :%v", topicName, clusterWriteErr)
			goto exitpub
		}
		doRefresh(tcData)
		coordLog.Debugf("coord data refreshed while write: %v", tcData)
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
	return clusterWriteErr
}

func (self *NsqdCoordinator) putMessageOnSlave(coord *TopicCoordinator, logData CommitLogData, msg *nsqd.Message) *CoordErr {
	tcData := coord.GetData()
	topicName := tcData.topicInfo.Name
	partition := tcData.topicInfo.Partition
	var logMgr *TopicCommitLogMgr

	checkDupOnSlave := func(tc *coordData) bool {
		coordLog.Debugf("pub on slave : %v, msg %v", topicName, msg.ID)
		logMgr = tc.logMgr
		if logMgr.IsCommitted(logData.LogID) {
			coordLog.Infof("pub the already committed log id : %v", logData.LogID)
			return true
		}
		return false
	}

	doLocalWriteOnSlave := func(tc *coordData) *CoordErr {
		topic, localErr := self.localNsqd.GetExistingTopic(topicName, partition)
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
		defer topic.Unlock()
		localErr = topic.PutMessageOnReplica(msg, nsqd.BackendOffset(logData.MsgOffset))
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

	checkDupOnSlave := func(tc *coordData) bool {
		logMgr = tc.logMgr
		if logMgr.IsCommitted(logData.LogID) {
			coordLog.Infof("put the already committed log id : %v", logData.LogID)
			return true
		}
		return false
	}

	tcData := coord.GetData()
	topicName := tcData.topicInfo.Name
	partition := tcData.topicInfo.Partition
	doLocalWriteOnSlave := func(tc *coordData) *CoordErr {
		topic, localErr := self.localNsqd.GetExistingTopic(topicName, partition)
		if localErr != nil {
			coordLog.Infof("pub on slave missing topic : %v", topicName)
			// leave the isr and try re-sync with leader
			return &CoordErr{localErr.Error(), RpcErrTopicNotExist, CoordSlaveErr}
		}

		topic.Lock()
		defer topic.Unlock()
		localErr = topic.PutMessagesOnReplica(msgs, nsqd.BackendOffset(logData.MsgOffset))
		if localErr != nil {
			coordLog.Errorf("put messages on slave failed: %v", localErr)
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
	if checkDupOnSlave(tc) {
		return nil
	}

	coord.writeHold.Lock()
	defer coord.writeHold.Unlock()
	if coord.exiting {
		return ErrTopicExitingOnSlave
	}
	if coord.GetData().disableWrite {
		return ErrWriteDisabled
	}
	if !tc.IsMineISR(self.myNode.GetID()) {
		return ErrTopicWriteOnNonISR
	}

	topicName := tc.topicInfo.Name
	partition := tc.topicInfo.Partition

	var slaveErr *CoordErr
	var localErr error
	slaveErr = doLocalWriteOnSlave(tc)
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
	offset, localErr := channel.FinishMessage(clientID, msgID)
	if localErr != nil {
		coordLog.Infof("channel %v finish local msg %v error: %v", channel.GetName(), msgID, localErr)
		clusterWriteErr = ErrLocalWriteFailed
		goto exit
	}
	syncOffset.OffsetID = int64(msgID)
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
		rpcErr = c.UpdateChannelOffset(&tcData.topicLeaderSession, &tcData.topicInfo, channel.GetName(), syncOffset)
		if rpcErr == nil {
			success++
		} else {
			coordLog.Warningf("node %v update offset %v failed %v.", nodeID, syncOffset, rpcErr)
			clusterWriteErr = rpcErr
		}
	}

	if success != len(tcData.topicInfo.ISR) {
		coordLog.Warningf("some nodes in isr is not synced with channel consumer offset.")
	}

exit:
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

	coordLog.Debugf("got update channel(%v) offset on slave : %v", channelName, offset)
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
	ch.DisableConsume(true)
	currentEnd := ch.GetChannelEnd()
	if nsqd.BackendOffset(offset.VOffset) > currentEnd {
		topic.ForceFlush()
	}
	err := ch.ConfirmBackendQueueOnSlave(nsqd.BackendOffset(offset.VOffset))
	if err != nil {
		coordLog.Warningf("update local channel(%v) offset %v failed: %v, current channel end: %v", channelName, offset, err, currentEnd)
		return &CoordErr{err.Error(), RpcCommonErr, CoordLocalErr}
	}
	return nil
}

func (self *NsqdCoordinator) readTopicRawData(topic string, partition int, offset int64, size int32) ([]byte, *CoordErr) {
	//read directly from local topic data used for pulling data by replicas
	t, err := self.localNsqd.GetExistingTopic(topic, partition)
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
	select {
	case self.flushNotifyChan <- TopicPartitionID{topic, partition}:
	default:
	}
}

func (self *NsqdCoordinator) updateLocalTopic(topicCoord *coordData) *CoordErr {
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
			tpCoord.Exiting()
			tcData := tpCoord.GetData()
			if FindSlice(tcData.topicInfo.ISR, self.myNode.GetID()) == -1 {
				continue
			}
			if len(tcData.topicInfo.ISR)-1 <= tcData.topicInfo.Replica/2 {
				coordLog.Infof("The isr nodes in topic %v is not enough while leaving: %v",
					tpCoord.topicInfo.GetTopicDesp(), tpCoord.topicInfo.ISR)
			}

			if tcData.IsMineLeaderSessionReady(self.myNode.GetID()) {
				// leader
				self.leadership.ReleaseTopicLeader(topicName, pid, &tcData.topicLeaderSession)
				coordLog.Infof("The leader for topic %v is transfered.", tcData.topicInfo.GetTopicDesp())
			}
			// wait lookup choose new node for isr/leader
			self.requestLeaveFromISR(topicName, pid)
		}
	}
	coordLog.Infof("prepare leaving finished.")
	if self.leadership != nil {
		self.leadership.UnregisterNsqd(&self.myNode)
	}
}
