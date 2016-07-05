package consistence

import (
	"bytes"
	"encoding/binary"
	"errors"
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
	MAX_LOG_PULL       = 10000
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
	lookupMutex            sync.Mutex
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

func (self *NsqdCoordinator) GetMyID() string {
	return self.myNode.GetID()
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
	self.wg.Add(1)
	go self.watchNsqLookupd()

	start := time.Now()
	for {
		self.lookupMutex.Lock()
		l := self.lookupLeader
		self.lookupMutex.Unlock()
		if l.GetID() != "" {
			break
		}
		time.Sleep(time.Second)
		coordLog.Infof("waiting for lookupd ...")
		if time.Now().Sub(start) > time.Second*30 {
			panic("no lookupd found while starting nsqd coordinator")
		}
	}

	err := self.loadLocalTopicData()
	if err != nil {
		close(self.stopChan)
		self.rpcServer.stop()
		return err
	}
	go self.rpcServer.start(self.myNode.NodeIP, self.myNode.RpcPort)
	if self.leadership != nil {
		err := self.leadership.RegisterNsqd(&self.myNode)
		if err != nil {
			coordLog.Warningf("failed to register nsqd coordinator: %v", err)
			return err
		}
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
	self.lookupMutex.Lock()
	l := self.lookupLeader
	self.lookupMutex.Unlock()
	c, err := self.lookupRemoteCreateFunc(net.JoinHostPort(l.NodeIP, l.RpcPort), RPC_TIMEOUT_FOR_LOOKUP)
	if err == nil {
		return c, nil
	}
	coordLog.Infof("get lookup remote %v failed: %v", l, err)
	return c, NewCoordErr(err.Error(), CoordNetErr)
}

func (self *NsqdCoordinator) GetCurrentLookupd() NsqLookupdNodeInfo {
	self.lookupMutex.Lock()
	defer self.lookupMutex.Unlock()
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
			self.lookupMutex.Lock()
			if n.GetID() != self.lookupLeader.GetID() ||
				n.Epoch != self.lookupLeader.Epoch {
				coordLog.Infof("nsqlookup leader changed: %v", n)
				self.lookupLeader = *n
			}
			self.lookupMutex.Unlock()
		}
	}
}

func (self *NsqdCoordinator) checkLocalTopicMagicCode(topicInfo *TopicPartitionMetaInfo, tryFix bool) {
	self.localNsqd.CheckMagicCode(topicInfo.Name, topicInfo.Partition, topicInfo.MagicCode, tryFix)
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
			self.checkLocalTopicMagicCode(topicInfo, topicInfo.Leader != self.myNode.GetID())

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

				tc.writeHold.Lock()
				var coordErr *CoordErr
				topic, coordErr = self.updateLocalTopic(topicInfo, tc.GetData().logMgr)
				if coordErr != nil {
					coordLog.Errorf("failed to update local topic %v: %v", topicInfo.GetTopicDesp(), coordErr)
					panic(coordErr)
				}
				tc.writeHold.Unlock()
			} else {
				continue
			}

			// TODO: check the last commit log data logid is equal with the disk queue message
			// this can avoid data corrupt, if not equal we need rollback and find backward for the right data.
			if topicInfo.Leader == self.myNode.GetID() {
				coordLog.Infof("topic %v starting as leader.", topicInfo.GetTopicDesp())
				err := self.acquireTopicLeader(topicInfo)
				if err != nil {
					coordLog.Warningf("failed to acquire leader while start as leader: %v", err)
				}
			}
			if FindSlice(topicInfo.ISR, self.myNode.GetID()) != -1 {
				coordLog.Infof("topic starting as isr .")
				if len(topicInfo.ISR) > 1 && topicInfo.Leader != self.myNode.GetID() {
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

func (self *NsqdCoordinator) SearchLogByMsgCnt(topic string, part int, count int64) (*CommitLogData, int64, error) {
	tcData, err := self.getTopicCoordData(topic, part)
	if err != nil || tcData.logMgr == nil {
		return nil, 0, errors.New(err.Error())
	}
	_, _, l, localErr := tcData.logMgr.SearchLogDataByMsgCnt(count)
	if localErr != nil {
		coordLog.Infof("search data failed: %v", localErr)
		return nil, 0, localErr
	}
	realOffset := l.MsgOffset
	if l.MsgCnt < count {
		t, localErr := self.localNsqd.GetExistingTopic(topic, part)
		if localErr != nil {
			return l, realOffset, localErr
		}
		snap := t.GetDiskQueueSnapshot()
		localErr = snap.SeekTo(nsqd.BackendOffset(realOffset))
		if localErr != nil {
			coordLog.Infof("seek to disk queue error: %v, %v", localErr, realOffset)
			return l, realOffset, localErr
		}
		curCount := l.MsgCnt

		for {
			ret := snap.ReadOne()
			if ret.Err != nil {
				coordLog.Infof("read disk queue error: %v", ret.Err)
				if ret.Err == io.EOF {
					return l, realOffset, nil
				}
				return l, realOffset, ret.Err
			} else {
				realOffset = int64(ret.Offset)
				if curCount >= count || curCount > l.MsgCnt+int64(l.MsgNum-1) {
					break
				}
				curCount++
			}
		}
	}
	return l, realOffset, nil
}

type MsgTimestampComparator struct {
	localTopicReader *nsqd.DiskQueueSnapshot
	searchEnd        int64
	searchTs         int64
}

func (self *MsgTimestampComparator) SearchEndBoundary() int64 {
	return self.searchEnd
}

func (self *MsgTimestampComparator) LessThanLeftBoundary(l *CommitLogData) bool {
	err := self.localTopicReader.SeekTo(nsqd.BackendOffset(l.MsgOffset))
	if err != nil {
		coordLog.Errorf("seek disk queue failed: %v, %v", l, err)
		return true
	}
	r := self.localTopicReader.ReadOne()
	if r.Err != nil {
		coordLog.Errorf("seek disk queue failed: %v, %v", l, r.Err)
		return true
	}
	msg, err := nsqd.DecodeMessage(r.Data)
	if err != nil {
		coordLog.Errorf("failed to decode message - %s - %v", err, r)
		return true
	}
	if self.searchTs < msg.Timestamp {
		return true
	}
	return false
}

func (self *MsgTimestampComparator) GreatThanRightBoundary(l *CommitLogData) bool {
	// we may read the eof , in this situation we reach the end, so the search should not be great than right boundary
	err := self.localTopicReader.SeekTo(nsqd.BackendOffset(l.MsgOffset + int64(l.MsgSize)))
	if err != nil {
		coordLog.Errorf("seek disk queue failed: %v, %v", l, err)
		return false
	}
	r := self.localTopicReader.ReadOne()
	if r.Err != nil {
		coordLog.Errorf("seek disk queue failed: %v, %v", l, r.Err)
		return false
	}
	msg, err := nsqd.DecodeMessage(r.Data)
	if err != nil {
		coordLog.Errorf("failed to decode message - %s - %v", err, r)
		return false
	}
	if self.searchTs > msg.Timestamp {
		return true
	}
	return false
}

func (self *NsqdCoordinator) SearchLogByMsgTimestamp(topic string, part int, ts_sec int64) (*CommitLogData, int64, error) {
	tcData, err := self.getTopicCoordData(topic, part)
	if err != nil || tcData.logMgr == nil {
		return nil, 0, errors.New(err.Error())
	}
	t, localErr := self.localNsqd.GetExistingTopic(topic, part)
	if localErr != nil {
		return nil, 0, localErr
	}

	snap := t.GetDiskQueueSnapshot()
	comp := &MsgTimestampComparator{
		localTopicReader: snap,
		searchEnd:        tcData.logMgr.GetCurrentStart(),
		searchTs:         ts_sec * 1000 * 1000 * 1000,
	}
	startSearch := time.Now()
	_, _, l, localErr := tcData.logMgr.SearchLogDataByComparator(comp)
	coordLog.Infof("search log cost: %v", time.Since(startSearch))
	if localErr != nil {
		return nil, 0, localErr
	}
	realOffset := l.MsgOffset
	// check if the message timestamp is fit the require
	localErr = snap.SeekTo(nsqd.BackendOffset(realOffset))
	if localErr != nil {
		coordLog.Infof("seek to disk queue error: %v, %v", localErr, realOffset)
		return l, realOffset, localErr
	}
	curCount := l.MsgCnt

	for {
		ret := snap.ReadOne()
		if ret.Err != nil {
			coordLog.Infof("read disk queue error: %v", ret.Err)
			if ret.Err == io.EOF {
				return l, realOffset, nil
			}
			return l, realOffset, ret.Err
		} else {
			realOffset = int64(ret.Offset)
			msg, err := nsqd.DecodeMessage(ret.Data)
			if err != nil {
				coordLog.Errorf("failed to decode message - %v - %v", err, ret)
				return l, realOffset, err
			}
			// we allow to read the next message count exceed the current log
			// in case the current log contains only one message (the returned timestamp may be
			// next to the current)
			if msg.Timestamp >= comp.searchTs || curCount > l.MsgCnt+int64(l.MsgNum-1) {
				break
			}
			curCount++
		}
	}
	return l, realOffset, nil
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
	if topicCoord.GetLeaderSessionID() != self.myNode.GetID() || topicCoord.GetLeader() != self.myNode.GetID() {
		return ErrNotTopicLeader
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
		coordLog.Infof("topic(%v) catching already running", topicInfo.Name)
		return ErrTopicCatchupAlreadyRunning
	}
	defer atomic.StoreInt32(&tc.catchupRunning, 0)
	coordLog.Infof("local topic begin catchup : %v, join session: %v", topicInfo.GetTopicDesp(), joinISRSession)
	tc.writeHold.Lock()
	defer tc.writeHold.Unlock()
	logMgr := tc.GetData().logMgr
	logIndex, offset, _, logErr := logMgr.GetLastCommitLogOffsetV2()
	if logErr != nil && logErr != ErrCommitLogEOF {
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
	for offset > 0 || logIndex > 0 {
		// if leader changed we abort and wait next time
		if tc.GetData().GetLeader() != topicInfo.Leader {
			coordLog.Warningf("topic leader changed from %v to %v, abort current catchup: %v", topicInfo.Leader, tc.GetData().GetLeader(), topicInfo.GetTopicDesp())
			return ErrTopicLeaderChanged
		}
		localLogData, localErr := logMgr.GetCommitLogFromOffsetV2(logIndex, offset)
		if localErr != nil {
			offset -= int64(GetLogDataSize())
			if offset < 0 && logIndex > 0 {
				logIndex--
				offset, _, localErr = logMgr.GetLastCommitLogDataOnSegment(logIndex)
				if localErr != nil {
					coordLog.Warningf("topic %v read commit log failed: %v, %v", topicInfo.GetTopicDesp(), logIndex, localErr)
					return &CoordErr{localErr.Error(), RpcNoErr, CoordLocalErr}
				}
			}
			continue
		}

		leaderLogIndex, leaderOffset, leaderLogData, err := c.GetCommitLogFromOffset(&topicInfo, logIndex, offset)
		if err.IsEqual(ErrTopicCommitLogOutofBound) || err.IsEqual(ErrTopicCommitLogEOF) {
			coordLog.Infof("local commit log is more than leader while catchup: %v:%v vs %v:%v",
				logIndex, offset, leaderLogIndex, leaderOffset)
			// local log is ahead of the leader, must truncate local data.
			// truncate commit log and truncate the data file to last log
			// commit offset.
			offset = leaderOffset
			logIndex = leaderLogIndex
		} else if err != nil {
			coordLog.Warningf("something wrong while get leader logdata while catchup: %v", err)
			if retryCnt > MAX_CATCHUP_RETRY {
				return err
			}
			retryCnt++
			time.Sleep(time.Second)
		} else {
			if *localLogData == leaderLogData {
				coordLog.Infof("topic %v local commit log match leader %v at: %v", topicInfo.GetTopicDesp(), topicInfo.Leader, leaderLogData)
				break
			}
			offset -= int64(GetLogDataSize())
			if offset < 0 && logIndex > 0 {
				logIndex--
				offset, _, localErr = logMgr.GetLastCommitLogDataOnSegment(logIndex)
				if localErr != nil {
					coordLog.Warningf("topic %v read commit log failed: %v, %v", topicInfo.GetTopicDesp(), logIndex, localErr)
					return &CoordErr{localErr.Error(), RpcNoErr, CoordLocalErr}
				}
			}
		}
	}
	coordLog.Infof("topic %v local commit log match leader %v at: %v:%v", topicInfo.GetTopicDesp(), topicInfo.Leader, logIndex, offset)
	localTopic, localErr := self.localNsqd.GetExistingTopic(topicInfo.Name, topicInfo.Partition)
	if localErr != nil {
		coordLog.Errorf("get local topic failed:%v", localErr)
		return ErrLocalMissingTopic
	}
	if localTopic.GetTopicPart() != topicInfo.Partition {
		coordLog.Errorf("local topic partition mismatch:%v vs %v", topicInfo.Partition, localTopic.GetTopicPart())
		return ErrLocalTopicPartitionMismatch
	}
	localTopic.SetAutoCommit(false)
	localTopic.SetMsgGenerator(logMgr)
	if offset > 0 || logIndex > 0 {
		lastLog, localErr := logMgr.GetCommitLogFromOffsetV2(logIndex, offset)
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
		_, localErr = logMgr.TruncateToOffsetV2(logIndex, offset)
		if localErr != nil {
			coordLog.Errorf("failed to truncate local commit log to %v:%v: %v", logIndex, offset, localErr)
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
		_, localErr = logMgr.TruncateToOffsetV2(0, 0)
		if localErr != nil {
			coordLog.Errorf("failed to truncate local commit log to %v:%v: %v", 0, 0, localErr)
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
		logs, dataList, rpcErr := c.PullCommitLogsAndData(topicInfo.Name, topicInfo.Partition, logIndex, offset, MAX_LOG_PULL)
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
		coordLog.Infof("topic %v pulled logs :%v from offset: %v:%v", topicInfo.GetTopicDesp(), len(logs), logIndex, offset)
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
		logIndex, offset = logMgr.GetCurrentEnd()

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
	disableWrite := topicCoord.IsWriteDisabled()
	topicCoord.dataMutex.Lock()
	if newTopicInfo.Epoch < topicCoord.topicInfo.Epoch {
		coordLog.Warningf("topic (%v) info epoch is less while update: %v vs %v",
			topicCoord.topicInfo.GetTopicDesp(), newTopicInfo.Epoch, topicCoord.topicInfo.Epoch)

		topicCoord.dataMutex.Unlock()
		return ErrEpochLessThanCurrent
	}
	// if any of new node in isr or leader is changed, the write disabled should be set first on isr nodes.
	if newTopicInfo.Epoch != topicCoord.topicInfo.Epoch {
		if !disableWrite && newTopicInfo.Leader != "" && FindSlice(newTopicInfo.ISR, self.myNode.GetID()) != -1 {
			if newTopicInfo.Leader != topicCoord.topicInfo.Leader || len(newTopicInfo.ISR) > len(topicCoord.topicInfo.ISR) {
				coordLog.Errorf("should disable the write before changing the leader or isr of topic")
				topicCoord.dataMutex.Unlock()
				return ErrTopicCoordStateInvalid
			}
			// note: removing failed node no need to disable write.
			for _, newNode := range newTopicInfo.ISR {
				if FindSlice(topicCoord.topicInfo.ISR, newNode) == -1 {
					coordLog.Errorf("should disable the write before adding new ISR node ")
					topicCoord.dataMutex.Unlock()
					return ErrTopicCoordStateInvalid
				}
			}
		}
	}
	if topicCoord.IsExiting() {
		coordLog.Infof("update the topic info: %v while exiting.", oldData.topicInfo.GetTopicDesp())
		topicCoord.dataMutex.Unlock()
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
	newCoordData := topicCoord.coordData.GetCopy()
	if topicCoord.topicInfo.Epoch != newTopicInfo.Epoch {
		newCoordData.topicInfo = *newTopicInfo
	}
	topicCoord.coordData = newCoordData
	topicCoord.dataMutex.Unlock()

	localTopic, err := self.updateLocalTopic(newTopicInfo, topicCoord.GetData().logMgr)
	if err != nil {
		coordLog.Warningf("init local topic failed: %v", err)
		self.removeTopicCoord(newTopicInfo.Name, newTopicInfo.Partition, false)
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
	} else {
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
	self.switchStateForMaster(topicCoord, localTopic, newTopicInfo.Leader == self.myNode.GetID(), false)
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

func (self *NsqdCoordinator) switchStateForMaster(topicCoord *TopicCoordinator, localTopic *nsqd.Topic, master bool, syncCommitAndDisk bool) *CoordErr {
	// flush topic data and channel comsume data if any cluster topic info changed
	tcData := topicCoord.GetData()
	// leader changed (maybe down), we make sure out data is flushed to keep data safe
	localTopic.ForceFlush()
	tcData.logMgr.FlushCommitLogs()
	tcData.logMgr.switchForMaster(master)
	if master {
		localTopic.Lock()
		logIndex, logOffset, logData, err := tcData.logMgr.GetLastCommitLogOffsetV2()
		if err != nil {
			if err != ErrCommitLogEOF {
				coordLog.Errorf("commit log is corrupted: %v", err)
			} else {
				coordLog.Infof("no commit last log data : %v", err)
			}
		} else {
			coordLog.Infof("current topic %v log: %v:%v, %v, pid: %v, %v",
				tcData.topicInfo.GetTopicDesp(), logIndex, logOffset, logData, tcData.logMgr.pLogID, tcData.logMgr.nLogID)
			if syncCommitAndDisk {
				localErr := localTopic.ResetBackendEndNoLock(nsqd.BackendOffset(logData.MsgOffset+int64(logData.MsgSize)),
					logData.MsgCnt+int64(logData.MsgNum)-1)
				if localErr != nil {
					coordLog.Errorf("reset local backend failed: %v", localErr)
				}
			}
		}
		localTopic.Unlock()
		coordLog.Infof("current topic %v write state: %v",
			tcData.topicInfo.GetTopicDesp(), topicCoord.IsWriteDisabled())
		if !topicCoord.IsWriteDisabled() {
			localTopic.EnableForMaster()
		}
	} else {
		localTopic.DisableForSlave()
	}
	offsetMap := make(map[string]ChannelConsumerOffset)
	tcData.consumeMgr.Lock()
	for chName, offset := range tcData.consumeMgr.channelConsumeOffset {
		offsetMap[chName] = offset
		coordLog.Infof("current channel %v offset: %v", chName, offset)
		delete(tcData.consumeMgr.channelConsumeOffset, chName)
	}
	tcData.consumeMgr.Unlock()
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

	return nil
}

func (self *NsqdCoordinator) updateTopicLeaderSession(topicCoord *TopicCoordinator, newLS *TopicLeaderSession, joinSession string) *CoordErr {
	if self.stopping {
		return ErrClusterChanged
	}
	topicCoord.dataMutex.Lock()
	if newLS.LeaderEpoch < topicCoord.GetLeaderSessionEpoch() {
		topicCoord.dataMutex.Unlock()
		coordLog.Infof("topic partition leadership epoch error.")
		return ErrEpochLessThanCurrent
	}
	coordLog.Infof("update the topic %v leader session: %v", topicCoord.topicInfo.GetTopicDesp(), newLS)
	if newLS != nil && newLS.LeaderNode != nil && topicCoord.GetLeader() != newLS.LeaderNode.GetID() {
		coordLog.Infof("topic leader info not match leader session: %v", topicCoord.GetLeader())
		topicCoord.dataMutex.Unlock()
		return ErrTopicLeaderSessionInvalid
	}
	newCoordData := topicCoord.coordData.GetCopy()
	if newLS == nil {
		coordLog.Infof("leader session is lost for topic")
		newCoordData.topicLeaderSession = TopicLeaderSession{}
	} else if !topicCoord.topicLeaderSession.IsSame(newLS) {
		newCoordData.topicLeaderSession = *newLS
	}
	topicCoord.coordData = newCoordData
	topicCoord.dataMutex.Unlock()
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
	localTopic.SetAutoCommit(false)
	localTopic.SetMsgGenerator(tcData.logMgr)
	// leader changed (maybe down), we make sure out data is flushed to keep data safe
	self.switchStateForMaster(topicCoord, localTopic, tcData.IsMineLeaderSessionReady(self.myNode.GetID()), false)

	coordLog.Infof("topic leader session: %v", tcData.topicLeaderSession)
	if tcData.IsMineLeaderSessionReady(self.myNode.GetID()) {
		coordLog.Infof("I become the leader for the topic: %v", tcData.topicInfo.GetTopicDesp())
	} else {
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

func (self *NsqdCoordinator) removeTopicCoord(topic string, partition int, removeData bool) (*TopicCoordinator, *CoordErr) {
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
		topicCoord.Delete(removeData)
		return topicCoord, nil
	}
	return nil, ErrMissingTopicCoord
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
				rpcErr = c.NotifyUpdateChannelOffset(&tcData.topicLeaderSession, &tcData.topicInfo, ch.GetName(), syncOffset)
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

func (self *NsqdCoordinator) updateLocalTopic(topicInfo *TopicPartitionMetaInfo, logMgr *TopicCommitLogMgr) (*nsqd.Topic, *CoordErr) {
	// check topic exist and prepare on local.
	t := self.localNsqd.GetTopicWithDisabled(topicInfo.Name, topicInfo.Partition)
	if t == nil {
		return nil, ErrLocalInitTopicFailed
	}
	localErr := self.localNsqd.SetTopicMagicCode(t, topicInfo.MagicCode)
	if localErr != nil {
		return t, ErrLocalInitTopicFailed
	}
	t.SetAutoCommit(false)
	t.SetMsgGenerator(logMgr)
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

func (self *NsqdCoordinator) Stats() *CoordStats {
	s := &CoordStats{}
	if self.rpcServer != nil && self.rpcServer.rpcServer != nil {
		s.RpcStats = self.rpcServer.rpcServer.Stats
	}
	return s
}
