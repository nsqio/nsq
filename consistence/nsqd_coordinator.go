package consistence

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/absolute8511/nsq/nsqd"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	MAX_WRITE_RETRY                  = 10
	MAX_CATCHUP_RETRY                = 5
	MAX_LOG_PULL                     = 10000
	MAX_LOG_PULL_BYTES               = 1024 * 1024 * 32
	MAX_TOPIC_RETENTION_SIZE_PER_DAY = 1024 * 1024 * 1024
	MAX_CATCHUP_RUNNING              = 3
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

func (self *TopicPartitionID) String() string {
	return self.TopicName + "-" + strconv.Itoa(self.TopicPartition)
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
	lookupRemoteClients    map[string]INsqlookupRemoteProxy
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
	grpcServer             *nsqdCoordGRpcServer
	tryCheckUnsynced       chan bool
	wg                     sync.WaitGroup
	enableBenchCost        bool
	stopping               int32
	catchupRunning         int32
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
		lookupRemoteClients:    make(map[string]INsqlookupRemoteProxy),
	}

	if nsqdCoord.leadership != nil {
		nsqdCoord.leadership.InitClusterID(nsqdCoord.clusterKey)
	}
	nsqdCoord.rpcServer = NewNsqdCoordRpcServer(nsqdCoord, rootPath)
	nsqdCoord.grpcServer = NewNsqdCoordGRpcServer(nsqdCoord, rootPath)
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
	realAddr, err := self.rpcServer.start(self.myNode.NodeIP, self.myNode.RpcPort)
	if err != nil {
		return err
	}
	_, realRpcPort, _ := net.SplitHostPort(realAddr)
	self.myNode.RpcPort = realRpcPort
	port, _ := strconv.Atoi(realRpcPort)
	grpcPort := strconv.Itoa(port + 1)
	go self.grpcServer.start(self.myNode.NodeIP, grpcPort)
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
	self.wg.Add(1)
	go self.checkAndCleanOldData()
	return nil
}

func (self *NsqdCoordinator) Stop() {
	if atomic.LoadInt32(&self.stopping) == 1 {
		return
	}
	// give up the leadership on the topic to
	// allow other isr take over to avoid electing.
	self.prepareLeavingCluster()
	close(self.stopChan)
	self.rpcServer.stop()
	self.rpcServer = nil
	self.rpcClientMutex.Lock()
	for _, c := range self.nsqdRpcClients {
		c.Close()
	}
	self.rpcClientMutex.Unlock()
	self.wg.Wait()

	self.lookupMutex.Lock()
	for _, c := range self.lookupRemoteClients {
		if c != nil {
			c.Close()
		}
	}
	self.lookupMutex.Unlock()
}

func (self *NsqdCoordinator) checkAndCleanOldData() {
	defer self.wg.Done()
	ticker := time.NewTicker(time.Hour)
	doCheckAndCleanOld := func(checkRetentionDay bool) {
		tmpCoords := make(map[string]map[int]*TopicCoordinator)
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
			for _, tpc := range tc {
				tcData := tpc.GetData()
				localTopic, err := self.localNsqd.GetExistingTopic(tcData.topicInfo.Name, tcData.topicInfo.Partition)
				if err != nil {
					coordLog.Infof("failed to get local topic: %v", tcData.topicInfo.GetTopicDesp())
					continue
				}
				retentionDay := tcData.topicInfo.RetentionDay
				if retentionDay == 0 {
					retentionDay = int32(nsqd.DEFAULT_RETENTION_DAYS)
				}
				retentionSize := MAX_TOPIC_RETENTION_SIZE_PER_DAY * int64(retentionDay)
				if checkRetentionDay {
					retentionSize = 0
				}
				// first clean we just check the clean offset
				// then we clean the commit log to make sure no access from log index
				// after that, we clean the real queue data
				cleanEndInfo, err := localTopic.TryCleanOldData(retentionSize, true, 0)
				if err != nil {
					coordLog.Infof("failed to get clean end: %v", err)
				}
				coordLog.Infof("topic %v try clean to : %v", tcData.topicInfo.GetTopicDesp(), cleanEndInfo)
				if cleanEndInfo != nil {
					matchIndex, matchOffset, l, err := tcData.logMgr.SearchLogDataByMsgOffset(int64(cleanEndInfo.Offset()))
					if err != nil {
						coordLog.Infof("search log failed: %v", err)
						continue
					}
					coordLog.Infof("clean commit log at : %v, %v, %v", matchIndex, matchOffset, l)
					if l.MsgOffset > int64(cleanEndInfo.Offset()) {
						coordLog.Warningf("search log clean position exceed the clean end, something wrong")
						continue
					}
					maxCleanOffset := cleanEndInfo.Offset()
					if l.MsgOffset < int64(cleanEndInfo.Offset()) {
						// the commit log is in the middle of the batch put,
						// it may happen that the batch across the end of the segment of data file,
						// so we should not clean the segment at the middle of the batch.
						maxCleanOffset = nsqd.BackendOffset(l.MsgOffset)
					}
					err = tcData.logMgr.CleanOldData(matchIndex, matchOffset)
					if err != nil {
						coordLog.Infof("clean commit log err : %v", err)
					} else {
						_, err := localTopic.TryCleanOldData(retentionSize, false, maxCleanOffset)
						if err != nil {
							coordLog.Infof("failed to clean disk queue: %v", err)
						}
					}
				}
			}
		}
	}
	for {
		select {
		case <-ticker.C:
			now := time.Now()
			if now.Hour() > 2 && now.Hour() < 6 {
				coordLog.Infof("check and clean at time: %v", now)
				doCheckAndCleanOld(true)
			} else {
				doCheckAndCleanOld(false)
			}
		case <-self.stopChan:
			return
		}
	}
}

func (self *NsqdCoordinator) periodFlushCommitLogs() {
	tmpCoords := make(map[string]map[int]*TopicCoordinator)
	syncCounter := 0
	defer self.wg.Done()
	flushTicker := time.NewTicker(time.Second * 2)
	doFlush := func() {
		syncCounter++
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
				if tcData.GetLeader() == self.myNode.GetID() || (syncCounter%10 == 0) {
					localTopic, err := self.localNsqd.GetExistingTopic(tcData.topicInfo.Name, tcData.topicInfo.Partition)
					if err != nil {
						coordLog.Infof("no local topic: %v", tcData.topicInfo.GetTopicDesp())
					} else {
						localTopic.ForceFlush()
						tcData.logMgr.FlushCommitLogs()
					}
				}
				if !tpc.IsExiting() {
					if syncCounter%2 == 0 && tcData.GetLeader() == self.myNode.GetID() {
						self.trySyncTopicChannels(tcData)
					}
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

//func (self *NsqdCoordinator) putLookupRemoteProxy(c INsqlookupRemoteProxy) {
//	self.lookupMutex.Lock()
//	ch, ok := self.lookupRemoteClients[c.RemoteAddr()]
//	self.lookupMutex.Unlock()
//	if ok {
//		select {
//		case ch <- c:
//		default:
//		}
//	}
//}

func (self *NsqdCoordinator) getLookupRemoteProxy() (INsqlookupRemoteProxy, *CoordErr) {
	self.lookupMutex.Lock()
	l := self.lookupLeader
	addr := net.JoinHostPort(l.NodeIP, l.RpcPort)
	c, ok := self.lookupRemoteClients[addr]
	self.lookupMutex.Unlock()
	if ok && c != nil {
		return c, nil
	}
	c, err := self.lookupRemoteCreateFunc(addr, RPC_TIMEOUT_FOR_LOOKUP)
	if err == nil {
		self.lookupMutex.Lock()
		self.lookupRemoteClients[addr] = c
		if len(self.lookupRemoteClients) > 3 {
			for k, _ := range self.lookupRemoteClients {
				if k == addr {
					continue
				}
				delete(self.lookupRemoteClients, k)
			}
		}
		self.lookupMutex.Unlock()
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

func (self *NsqdCoordinator) checkLocalTopicMagicCode(topicInfo *TopicPartitionMetaInfo, tryFix bool) error {
	removedPath, err := self.localNsqd.CheckMagicCode(topicInfo.Name, topicInfo.Partition, topicInfo.MagicCode, tryFix)
	if err != nil {
		coordLog.Infof("check magic code error: %v", err)
		return err
	}
	if removedPath != "" {
		basepath := GetTopicPartitionBasePath(self.dataRootPath, topicInfo.Name, topicInfo.Partition)
		tmpLogMgr, err := InitTopicCommitLogMgr(topicInfo.Name, topicInfo.Partition, basepath, 1)
		if err != nil {
			coordLog.Warningf("topic %v failed to init tmp log manager: %v", topicInfo.GetTopicDesp(), err)
		} else {
			tmpLogMgr.MoveTo(removedPath)
			tmpLogMgr.Delete()
		}
	}
	return nil
}

func (self *NsqdCoordinator) forceCleanTopicData(topicName string, partition int) *CoordErr {
	// check if any data on local and try remove
	basepath := GetTopicPartitionBasePath(self.dataRootPath, topicName, partition)
	tmpLogMgr, err := InitTopicCommitLogMgr(topicName, partition, basepath, 1)
	if err != nil {
		coordLog.Warningf("topic %v failed to init tmp log manager: %v", topicName, err)
	} else {
		tmpLogMgr.Delete()
	}
	localErr := self.localNsqd.ForceDeleteTopicData(topicName, partition)
	if localErr != nil {
		if !os.IsNotExist(localErr) {
			coordLog.Infof("delete topic %v-%v local data failed : %v", topicName, partition, localErr)
			return &CoordErr{localErr.Error(), RpcCommonErr, CoordLocalErr}
		}
	}
	return nil
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
									coord.dataMutex.Lock()
									newCoordData := coord.coordData.GetCopy()
									newCoordData.topicLeaderSession = *topicLeaderSession
									coord.coordData = newCoordData
									coord.dataMutex.Unlock()
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
					self.forceCleanTopicData(topicName, partition)
				}
				continue
			}

			checkErr := self.checkLocalTopicMagicCode(topicInfo, topicInfo.Leader != self.myNode.GetID())
			if checkErr != nil {
				coordLog.Errorf("failed to check topic :%v-%v, err:%v", topicName, partition, checkErr)
				go self.requestLeaveFromISR(topicInfo.Name, topicInfo.Partition)
				continue
			}

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
				coordLog.Infof("topic %v starting as not relevant", topicInfo.GetTopicDesp())
				if len(topicInfo.ISR) >= topicInfo.Replica {
					coordLog.Infof("no need load the local topic since the replica is enough: %v", topicInfo.GetTopicDesp())
					self.forceCleanTopicData(topicInfo.Name, topicInfo.Partition)
				} else if len(topicInfo.ISR)+len(topicInfo.CatchupList) < topicInfo.Replica {
					go self.requestJoinCatchup(topicName, partition)
				} else {
					self.localNsqd.CloseExistingTopic(topicName, partition)
				}
				continue
			}

			tc, err := self.getTopicCoord(topicInfo.Name, topicInfo.Partition)
			if err != nil {
				coordLog.Errorf("no coordinator for topic: %v", topicInfo.GetTopicDesp())
				panic(err)
			}
			dyConf := &nsqd.TopicDynamicConf{SyncEvery: int64(topicInfo.SyncEvery),
				AutoCommit:   0,
				RetentionDay: topicInfo.RetentionDay,
			}
			tc.GetData().logMgr.updateBufferSize(int(dyConf.SyncEvery - 1))
			topic.SetDynamicInfo(*dyConf, tc.GetData().logMgr)
			// TODO: check the last commit log data logid is equal with the disk queue message
			// this can avoid data corrupt, if not equal we need rollback and find backward for the right data.
			// check the first log commit log is valid on the disk queue, so that we can fix the wrong start of the commit log
			localErr := self.checkAndFixLocalTopicData(tc.GetData(), topic)
			if localErr != nil {
				coordLog.Errorf("check local topic %v data need to be fixed:%v", topicInfo.GetTopicDesp(), localErr)
				topic.SetDataFixState(true)
				go self.requestLeaveFromISR(topicInfo.Name, topicInfo.Partition)
			}
			if topicInfo.Leader == self.myNode.GetID() {
				coordLog.Infof("topic %v starting as leader.", topicInfo.GetTopicDesp())
				tc.DisableWrite(true)
				err := self.acquireTopicLeader(topicInfo)
				if err != nil {
					coordLog.Warningf("failed to acquire leader while start as leader: %v", err)
				}
			}
			if FindSlice(topicInfo.ISR, self.myNode.GetID()) != -1 {
				// restart node should rejoin the isr
				coordLog.Infof("topic starting as isr .")
				if len(topicInfo.ISR) > 1 && topicInfo.Leader != self.myNode.GetID() {
					go self.requestLeaveFromISR(topicInfo.Name, topicInfo.Partition)
				}
			} else if FindSlice(topicInfo.CatchupList, self.myNode.GetID()) != -1 {
				coordLog.Infof("topic %v starting as catchup", topicInfo.GetTopicDesp())
				go self.catchupFromLeader(*topicInfo, "")
			}
		}
	}
	return nil
}

func (self *NsqdCoordinator) checkAndFixLocalTopicData(tc *coordData, localTopic *nsqd.Topic) error {
	logStart, log, err := tc.logMgr.GetLogStartInfo()
	if err != nil {
		if err == ErrCommitLogEOF {
			return nil
		}
		coordLog.Warningf("get log start failed: %v", err)
		return err
	}
	snap := localTopic.GetDiskQueueSnapshot()
	for {
		err = snap.SeekTo(nsqd.BackendOffset(log.MsgOffset))
		if err != nil {
			coordLog.Warningf("topic %v log start %v should be fixed: %v, %v", tc.topicInfo.GetTopicDesp(), logStart, log, err)
			// try fix start
			if err == nsqd.ErrReadQueueAlreadyCleaned {
				start := snap.GetQueueReadStart()
				logStart.SegmentStartOffset = GetNextLogOffset(logStart.SegmentStartOffset)
				if log.MsgOffset+int64(log.MsgSize) < int64(start.Offset()) {
					matchIndex, matchOffset, _, err := tc.logMgr.SearchLogDataByMsgOffset(int64(start.Offset()))
					if err != nil {
						coordLog.Infof("search log failed: %v", err)
					} else if matchIndex > logStart.SegmentStartIndex ||
						(matchIndex == logStart.SegmentStartIndex && matchOffset > logStart.SegmentStartOffset) {
						logStart.SegmentStartIndex = matchIndex
						logStart.SegmentStartOffset = matchOffset
					}
				}
				err = tc.logMgr.CleanOldData(logStart.SegmentStartIndex, logStart.SegmentStartOffset)
				if err != nil {
					// maybe the diskqueue data corrupt, we need sync from leader
					coordLog.Errorf("clean log failed : %v, %v", logStart, err)
					return err
				}
				logStart, log, err = tc.logMgr.GetLogStartInfo()
				if err != nil {
					return err
				}
				coordLog.Warningf("topic %v log start fixed to: %v, %v", tc.topicInfo.GetTopicDesp(), logStart, log)
			} else {
				coordLog.Errorf("read disk failed at log start: %v, %v, %v", logStart, log, err)
				return err
			}
		} else {
			break
		}
	}
	r := snap.ReadOne()
	if r.Err != nil {
		if r.Err == io.EOF {
			return nil
		}
		coordLog.Errorf("read the start of disk failed %v ", r.Err)
		return r.Err
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
			for pid, tc := range info {
				if tc.IsExiting() {
					continue
				}
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
						self.removeTopicCoord(topicMeta.Name, topicMeta.Partition, true)
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
	if session != nil && session.LeaderNode != nil {
		if self.GetMyID() != session.LeaderNode.GetID() {
			coordLog.Warningf("the leader session should not be released by other node: %v, %v", session.LeaderNode, self.GetMyID())
			return ErrLeaderSessionMismatch
		}
	}
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

func (self *NsqdCoordinator) SearchLogByMsgOffset(topic string, part int, offset int64) (*CommitLogData, int64, int64, error) {
	tcData, err := self.getTopicCoordData(topic, part)
	if err != nil || tcData.logMgr == nil {
		return nil, 0, 0, errors.New(err.String())
	}
	_, _, l, localErr := tcData.logMgr.SearchLogDataByMsgOffset(offset)
	if localErr != nil {
		coordLog.Infof("search data failed: %v", localErr)
		return nil, 0, 0, localErr
	}
	realOffset := l.MsgOffset
	curCount := l.MsgCnt - 1
	if l.MsgOffset < offset {
		t, localErr := self.localNsqd.GetExistingTopic(topic, part)
		if localErr != nil {
			return l, 0, 0, localErr
		}
		snap := t.GetDiskQueueSnapshot()
		localErr = snap.SeekTo(nsqd.BackendOffset(realOffset))
		if localErr != nil {
			coordLog.Infof("seek to disk queue error: %v, %v", localErr, realOffset)
			return l, 0, 0, localErr
		}

		for {
			ret := snap.ReadOne()
			if ret.Err != nil {
				coordLog.Infof("read disk queue error: %v", ret.Err)
				if ret.Err == io.EOF {
					return l, realOffset, curCount, nil
				}
				return l, realOffset, curCount, ret.Err
			} else {
				if int64(ret.Offset) >= offset || curCount > l.MsgCnt+int64(l.MsgNum-1) {
					break
				}
				realOffset = int64(ret.Offset) + int64(ret.MovedSize)
				curCount++
			}
		}
	}
	return l, realOffset, curCount, nil
}

func (self *NsqdCoordinator) SearchLogByMsgCnt(topic string, part int, count int64) (*CommitLogData, int64, int64, error) {
	tcData, err := self.getTopicCoordData(topic, part)
	if err != nil || tcData.logMgr == nil {
		return nil, 0, 0, errors.New(err.String())
	}
	_, _, l, localErr := tcData.logMgr.SearchLogDataByMsgCnt(count)
	if localErr != nil {
		coordLog.Infof("search data failed: %v", localErr)
		return nil, 0, 0, localErr
	}
	realOffset := l.MsgOffset
	curCount := l.MsgCnt - 1
	if l.MsgCnt < count {
		t, localErr := self.localNsqd.GetExistingTopic(topic, part)
		if localErr != nil {
			return l, 0, 0, localErr
		}
		snap := t.GetDiskQueueSnapshot()
		localErr = snap.SeekTo(nsqd.BackendOffset(realOffset))
		if localErr != nil {
			coordLog.Infof("seek to disk queue error: %v, %v", localErr, realOffset)
			return l, 0, 0, localErr
		}

		for {
			ret := snap.ReadOne()
			if ret.Err != nil {
				coordLog.Infof("read disk queue error: %v", ret.Err)
				if ret.Err == io.EOF {
					return l, realOffset, curCount, nil
				}
				return l, realOffset, curCount, ret.Err
			} else {
				if curCount >= count-1 || curCount > l.MsgCnt+int64(l.MsgNum-1) {
					break
				}
				realOffset = int64(ret.Offset) + int64(ret.MovedSize)
				curCount++
			}
		}
	}
	return l, realOffset, curCount, nil
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
	err := self.localTopicReader.ResetSeekTo(nsqd.BackendOffset(l.MsgOffset))
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
	err := self.localTopicReader.ResetSeekTo(nsqd.BackendOffset(l.MsgOffset + int64(l.MsgSize)))
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

// return the searched log data and the exact offset the reader should be reset and the total count before the offset.
func (self *NsqdCoordinator) SearchLogByMsgTimestamp(topic string, part int, ts_sec int64) (*CommitLogData, int64, int64, error) {
	tcData, err := self.getTopicCoordData(topic, part)
	if err != nil || tcData.logMgr == nil {
		return nil, 0, 0, errors.New(err.String())
	}
	t, localErr := self.localNsqd.GetExistingTopic(topic, part)
	if localErr != nil {
		return nil, 0, 0, localErr
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
		return nil, 0, 0, localErr
	}
	realOffset := l.MsgOffset
	// check if the message timestamp is fit the require
	localErr = snap.ResetSeekTo(nsqd.BackendOffset(realOffset))
	if localErr != nil {
		coordLog.Infof("seek to disk queue error: %v, %v", localErr, realOffset)
		return l, 0, 0, localErr
	}
	curCount := l.MsgCnt - 1

	for {
		ret := snap.ReadOne()
		if ret.Err != nil {
			coordLog.Infof("read disk queue error: %v", ret.Err)
			if ret.Err == io.EOF {
				return l, realOffset, curCount, nil
			}
			return l, realOffset, curCount, ret.Err
		} else {
			msg, err := nsqd.DecodeMessage(ret.Data)
			if err != nil {
				coordLog.Errorf("failed to decode message - %v - %v", err, ret)
				return l, realOffset, curCount, err
			}
			// we allow to read the next message count exceed the current log
			// in case the current log contains only one message (the returned timestamp may be
			// next to the current)
			if msg.Timestamp >= comp.searchTs || curCount > l.MsgCnt+int64(l.MsgNum-1) {
				break
			}
			realOffset = int64(ret.Offset) + int64(ret.MovedSize)
			curCount++
		}
	}
	return l, realOffset, curCount, nil
}

// for isr node to check with leader
// The lookup will wait all isr sync to new leader during the leader switch
func (self *NsqdCoordinator) syncToNewLeader(topicCoord *coordData, joinSession string, mustSynced bool) *CoordErr {
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
			coordLog.Infof("topic %v isr not synced with new leader ", topicCoord.topicInfo.GetTopicDesp())
			if mustSynced {
				err := self.requestLeaveFromISR(topicCoord.topicInfo.Name, topicCoord.topicInfo.Partition)
				if err != nil {
					coordLog.Infof("request leave isr failed: %v", err)
				} else {
					self.requestJoinCatchup(topicCoord.topicInfo.Name, topicCoord.topicInfo.Partition)
				}
			} else {
				coordLog.Infof("topic %v ignore current isr node out of synced since write is not disabled", topicCoord.topicInfo.GetTopicDesp())
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

func (self *NsqdCoordinator) catchupFromLeader(topicInfo TopicPartitionMetaInfo, joinISRSession string) *CoordErr {
	// get local commit log from check point , and pull newer logs from leader
	tc, coordErr := self.getTopicCoord(topicInfo.Name, topicInfo.Partition)
	if coordErr != nil {
		coordLog.Warningf("topic(%v) catching failed since topic coordinator missing: %v", topicInfo.Name, coordErr)
		return ErrMissingTopicCoord
	}
	if !atomic.CompareAndSwapInt32(&tc.catchupRunning, 0, 1) {
		coordLog.Infof("topic(%v) catching already running", topicInfo.Name)
		return ErrTopicCatchupAlreadyRunning
	}
	defer atomic.StoreInt32(&tc.catchupRunning, 0)
	myRunning := atomic.AddInt32(&self.catchupRunning, 1)
	defer atomic.AddInt32(&self.catchupRunning, -1)
	if myRunning > MAX_CATCHUP_RUNNING {
		coordLog.Infof("catching too much running: %v", myRunning)
		return ErrCatchupRunningBusy
	}
	coordLog.Infof("local topic begin catchup : %v, join session: %v", topicInfo.GetTopicDesp(), joinISRSession)

	tc.writeHold.Lock()
	defer tc.writeHold.Unlock()
	if tc.IsExiting() {
		coordLog.Warningf("catchup exit since the topic is exited")
		return ErrTopicExitingOnSlave
	}
	logMgr := tc.GetData().logMgr
	logIndex, offset, _, logErr := logMgr.GetLastCommitLogOffsetV2()
	if logErr != nil && logErr != ErrCommitLogEOF {
		coordLog.Warningf("catching failed since log offset read error: %v", logErr)
		logMgr.Close()
		self.forceCleanTopicData(topicInfo.Name, topicInfo.Partition)
		logMgr.Reopen()
		tc.Exiting()
		go self.removeTopicCoord(topicInfo.Name, topicInfo.Partition, true)
		return ErrLocalTopicDataCorrupt
	}
	// pull logdata from leader at the offset.
	c, coordErr := self.acquireRpcClient(topicInfo.Leader)
	if coordErr != nil {
		coordLog.Warningf("failed to get rpc client while catchup: %v", coordErr)
		return coordErr
	}

	localTopic, localErr := self.localNsqd.GetExistingTopic(topicInfo.Name, topicInfo.Partition)
	if localErr != nil {
		coordLog.Errorf("get local topic failed:%v", localErr)
		return ErrLocalMissingTopic
	}

	localErr = self.checkAndFixLocalTopicData(tc.GetData(), localTopic)
	if localErr != nil {
		coordLog.Errorf("check local topic %v data need to be fixed:%v", topicInfo.GetTopicDesp(), localErr)
		localTopic.SetDataFixState(true)
	}

	retryCnt := 0
	needFullSync := false
	localLogSegStart, _, _ := logMgr.GetLogStartInfo()
	countNumIndex, _ := logMgr.ConvertToCountIndex(logIndex, offset)
	for offset > localLogSegStart.SegmentStartOffset || logIndex > localLogSegStart.SegmentStartIndex {
		// if leader changed we abort and wait next time
		if tc.GetData().GetLeader() != topicInfo.Leader {
			coordLog.Warningf("topic leader changed from %v to %v, abort current catchup: %v", topicInfo.Leader, tc.GetData().GetLeader(), topicInfo.GetTopicDesp())
			return ErrTopicLeaderChanged
		}
		localLogData, localErr := logMgr.GetCommitLogFromOffsetV2(logIndex, offset)
		if localErr != nil {
			offset -= int64(GetLogDataSize())
			if offset < 0 && logIndex > localLogSegStart.SegmentStartIndex {
				logIndex--
				offset, _, localErr = logMgr.GetLastCommitLogDataOnSegment(logIndex)
				if localErr != nil {
					coordLog.Warningf("topic %v read commit log failed: %v:%v, %v", topicInfo.GetTopicDesp(),
						logIndex, offset, localErr)
					return &CoordErr{localErr.Error(), RpcNoErr, CoordLocalErr}
				}
			}
			continue
		}

		countNumIndex, _ = logMgr.ConvertToCountIndex(logIndex, offset)
		useCountIndex, leaderCountNumIndex, leaderLogIndex, leaderOffset, leaderLogData, rpcErr := c.GetCommitLogFromOffset(&topicInfo, countNumIndex, logIndex, offset)
		if rpcErr.IsEqual(ErrTopicCommitLogOutofBound) || rpcErr.IsEqual(ErrTopicCommitLogEOF) {
			coordLog.Infof("local commit log is more than leader while catchup: %v:%v:%v vs %v:%v:%v",
				logIndex, offset, countNumIndex, leaderLogIndex, leaderOffset, leaderCountNumIndex)
			// local log is ahead of the leader, must truncate local data.
			// truncate commit log and truncate the data file to last log
			// commit offset.
			if useCountIndex {
				if leaderCountNumIndex > countNumIndex {
					coordLog.Infof("commit log changed while check leader commit log")
					// the leader commit log end changed
					time.Sleep(time.Second)
					continue
				}
				logIndex, offset, localErr = logMgr.ConvertToOffsetIndex(leaderCountNumIndex)
				if localErr != nil {
					coordLog.Infof("convert The leader commit log count index %v failed: %v", leaderCountNumIndex, localErr)
					needFullSync = true
					break
				}
			} else {
				if leaderLogIndex > logIndex ||
					(leaderLogIndex == logIndex && leaderOffset > offset) {
					coordLog.Infof("commit log changed while check leader commit log")
					time.Sleep(time.Second)
					continue
				}
				offset = leaderOffset
				logIndex = leaderLogIndex
			}
		} else if rpcErr.IsEqual(ErrTopicCommitLogLessThanSegmentStart) {
			coordLog.Infof("The leader commit log has been cleaned here, maybe the follower fall behind too much")
			// need a full sync
			needFullSync = true
			break
		} else if rpcErr != nil {
			coordLog.Warningf("something wrong while get leader logdata while catchup: %v", rpcErr)
			if retryCnt > MAX_CATCHUP_RETRY {
				return rpcErr
			}
			retryCnt++
			time.Sleep(time.Second)
		} else {
			if *localLogData == leaderLogData {
				coordLog.Infof("topic %v local commit log match leader %v at: %v", topicInfo.GetTopicDesp(), topicInfo.Leader, leaderLogData)
				break
			}
			offset -= int64(GetLogDataSize())
			if offset < 0 && logIndex > localLogSegStart.SegmentStartIndex {
				logIndex--
				offset, _, localErr = logMgr.GetLastCommitLogDataOnSegment(logIndex)
				if localErr != nil {
					coordLog.Warningf("topic %v read commit log failed: %v, %v", topicInfo.GetTopicDesp(), logIndex, localErr)
					return &CoordErr{localErr.Error(), RpcNoErr, CoordLocalErr}
				}
			}
		}
	}
	countNumIndex, _ = logMgr.ConvertToCountIndex(logIndex, offset)
	coordLog.Infof("topic %v local commit log match leader %v at: %v:%v:%v", topicInfo.GetTopicDesp(),
		topicInfo.Leader, logIndex, offset, countNumIndex)

	dyConf := &nsqd.TopicDynamicConf{SyncEvery: int64(topicInfo.SyncEvery),
		AutoCommit:   0,
		RetentionDay: topicInfo.RetentionDay,
	}
	logMgr.updateBufferSize(int(dyConf.SyncEvery - 1))
	localTopic.SetDynamicInfo(*dyConf, logMgr)
	compatibleMethod := false
	if offset == localLogSegStart.SegmentStartOffset && logIndex == localLogSegStart.SegmentStartIndex {
		if logIndex == 0 && offset == 0 {
			// for compatible, if all replica start with 0 (no auto clean happen)
			_, _, _, _, _, rpcErr := c.GetCommitLogFromOffset(&topicInfo, 0, 0, 0)
			coordLog.Infof("catchup start with 0, check return: %v", rpcErr)
			if rpcErr == nil || rpcErr.IsEqual(ErrTopicCommitLogEOF) {
				coordLog.Infof("catchup start with 0, We can do full sync in old way since leader is also start with 0")
				compatibleMethod = true
			} else {
				coordLog.Infof("catchup start with 0, need do full sync in new way: %v", rpcErr)
			}
		}
		needFullSync = true
	}
	if localTopic.IsDataNeedFix() {
		needFullSync = true
		compatibleMethod = false
	}
	if !needFullSync {
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
		if localErr != nil {
			coordLog.Errorf("failed to reset local topic %v data: %v", localTopic.GetFullName(), localErr)
			localTopic.SetDataFixState(true)
			tc.logMgr.Close()
			self.forceCleanTopicData(localTopic.GetTopicName(), localTopic.GetTopicPart())
			tc.logMgr.Reopen()
			tc.Exiting()
			go self.removeTopicCoord(localTopic.GetTopicName(), localTopic.GetTopicPart(), true)
			return &CoordErr{localErr.Error(), RpcNoErr, CoordLocalErr}
		}
		_, localErr = logMgr.TruncateToOffsetV2(logIndex, offset)
		if localErr != nil {
			coordLog.Errorf("failed to truncate local commit log to %v:%v: %v", logIndex, offset, localErr)
			return &CoordErr{localErr.Error(), RpcNoErr, CoordLocalErr}
		}
	} else {
		// need sync the commit log segment start info and the topic diskqueue start info
		// we should clean local data, since the local start info maybe different with leader start info
		coordLog.Infof("local topic %v should do full sync", localTopic.GetFullName())
		localTopic.PrintCurrentStats()
		if compatibleMethod {
			localTopic.Lock()
			localErr = localTopic.ResetBackendEndNoLock(nsqd.BackendOffset(0), 0)
			localTopic.Unlock()
			if localErr != nil {
				coordLog.Errorf("failed to reset local topic data: %v", localErr)
				return &CoordErr{localErr.Error(), RpcNoErr, CoordLocalErr}
			}
			_, localErr = logMgr.TruncateToOffsetV2(0, 0)
			if localErr != nil {
				if localErr != ErrCommitLogEOF {
					coordLog.Errorf("failed to truncate local commit log to %v:%v: %v", 0, 0, localErr)
					return &CoordErr{localErr.Error(), RpcNoErr, CoordLocalErr}
				}
			}
		} else {
			leaderCommitStartInfo, firstLogData, rpcErr := c.GetFullSyncInfo(topicInfo.Name, topicInfo.Partition)
			if rpcErr != nil {
				coordLog.Warningf("failed to get full sync info: %v", rpcErr)
				return &CoordErr{rpcErr.Error(), RpcCommonErr, CoordNetErr}
			}
			coordLog.Infof("topic %v full sync with start: %v, %v", localTopic.GetFullName(), leaderCommitStartInfo, firstLogData)
			localErr = logMgr.ResetLogWithStart(*leaderCommitStartInfo)
			if localErr != nil {
				coordLog.Warningf("reset commit log with start %v failed: %v", leaderCommitStartInfo, localErr)
				return &CoordErr{localErr.Error(), RpcNoErr, CoordLocalErr}
			}
			localTopic.Lock()
			// Is it possible that the first log start is 0 ?? If it is 0 , the leader queue must start with 0,
			// in that case, we can sync in the compatible way
			localErr = localTopic.ResetBackendWithQueueStartNoLock(firstLogData.MsgOffset, firstLogData.MsgCnt-1)
			localTopic.Unlock()
			if localErr != nil {
				coordLog.Warningf("reset topic %v queue with start %v failed: %v", topicInfo.GetTopicDesp(), firstLogData, localErr)
				tc.logMgr.Close()
				self.forceCleanTopicData(localTopic.GetTopicName(), localTopic.GetTopicPart())
				tc.logMgr.Reopen()
				tc.Exiting()
				go self.removeTopicCoord(localTopic.GetTopicName(), localTopic.GetTopicPart(), true)
				return &CoordErr{localErr.Error(), RpcNoErr, CoordLocalErr}
			}
			logIndex, offset, localErr = logMgr.ConvertToOffsetIndex(leaderCommitStartInfo.SegmentStartCount)
			if localErr != nil {
				return &CoordErr{localErr.Error(), RpcNoErr, CoordLocalErr}
			}
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
		countNumIndex, localErr := logMgr.ConvertToCountIndex(logIndex, offset)
		if localErr != nil {
			return &CoordErr{localErr.Error(), RpcNoErr, CoordLocalErr}
		}
		logs, dataList, rpcErr := c.PullCommitLogsAndData(topicInfo.Name, topicInfo.Partition,
			countNumIndex, logIndex, offset, MAX_LOG_PULL)
		if rpcErr != nil {
			// if not network error, something wrong with commit log file, we need return to abort.
			coordLog.Infof("topic %v error while get logs :%v, offset: %v:%v:%v", topicInfo.GetTopicDesp(),
				rpcErr, logIndex, offset, countNumIndex)
			if retryCnt > MAX_CATCHUP_RETRY {
				return &CoordErr{rpcErr.Error(), RpcCommonErr, CoordNetErr}
			}
			retryCnt++
			time.Sleep(time.Second)
			continue
		} else if len(logs) == 0 {
			synced = true
		}
		coordLog.Infof("topic %v pulled logs :%v from offset: %v:%v:%v", topicInfo.GetTopicDesp(),
			len(logs), logIndex, offset, countNumIndex)
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
					coordLog.Warningf("topic %v Failed to put message on slave: %v, offset: %v, need to be fixed", localTopic.GetFullName(), localErr, l.MsgOffset)
					localTopic.SetDataFixState(true)
					hasErr = true
					break
				}
				lastCommitOffset = queueEnd
			} else {
				coordLog.Debugf("got batch messages: %v", len(newMsgs))
				queueEnd, localErr = localTopic.PutMessagesOnReplica(newMsgs, nsqd.BackendOffset(l.MsgOffset))
				if localErr != nil {
					coordLog.Warningf("Failed to batch put messages on slave: %v, offset: %v, need to be fixed", localErr, l.MsgOffset)
					localTopic.SetDataFixState(true)
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
				coordLog.Errorf("Failed to append local log: %v, need to be fixed ", localErr)
				localTopic.SetDataFixState(true)
				hasErr = true
				break
			}
		}
		if !hasErr {
			logMgr.FlushCommitLogs()
			localTopic.UpdateCommittedOffset(lastCommitOffset)
		}
		localTopic.Unlock()
		if hasErr {
			return &CoordErr{localErr.Error(), RpcNoErr, CoordLocalErr}
		}
		localTopic.ForceFlush()
		logIndex, offset = logMgr.GetCurrentEnd()
		localTopic.SetDataFixState(false)

		if synced && joinISRSession == "" {
			// notify nsqlookupd coordinator to add myself to isr list.
			// if success, the topic leader will disable new write.
			coordLog.Infof("I am requesting join isr: %v", self.myNode.GetID())
			stat, err := c.GetTopicStats(topicInfo.Name)
			if err != nil {
				coordLog.Infof("try get stats from leader failed: %v", err)
			} else {
				localTopic.GetDetailStats().ResetHistoryInitPub(localTopic.TotalDataSize())
				localTopic.GetDetailStats().UpdateHistory(stat.TopicHourlyPubDataList[topicInfo.GetTopicDesp()])
				chList, ok := stat.ChannelList[topicInfo.GetTopicDesp()]
				coordLog.Infof("topic %v sync channel list from leader: %v", topicInfo.GetTopicDesp(), chList)
				if ok {
					oldChList := localTopic.GetChannelMapCopy()
					for _, chName := range chList {
						localTopic.GetChannel(chName)
						delete(oldChList, chName)
					}
					coordLog.Infof("topic %v local channel not on leader: %v", topicInfo.GetTopicDesp(), oldChList)
					for chName, _ := range oldChList {
						localTopic.DeleteExistingChannel(chName)
					}
					localTopic.SaveChannelMeta()
				}
			}
			if !tc.IsExiting() {
				go func() {
					err := self.requestJoinTopicISR(&topicInfo)
					if err != nil {
						coordLog.Infof("request join isr failed: %v", err)
					}
				}()
			}
			break
		} else if synced && joinISRSession != "" {
			// TODO: maybe need sync channels from leader
			// should sync the history stats to make sure balance stats data is ok
			localTopic.ForceFlush()
			logMgr.FlushCommitLogs()
			logMgr.switchForMaster(false)
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

// Note:
// update epoch should be increased
// if write epoch keep unchanged, the leader should be the same and no new node in isr
// write epoch should be changed only when write is disabled
// Any new ISR or leader change should be allowed only when write is disabled
// any removal of isr or leader session change can be done without disable write
// if I am not in isr anymore, all update should be allowed
func (self *NsqdCoordinator) checkUpdateState(topicCoord *coordData, writeDisabled bool, newData *TopicPartitionMetaInfo) *CoordErr {
	topicDesp := topicCoord.topicInfo.GetTopicDesp()
	if newData.EpochForWrite < topicCoord.topicInfo.EpochForWrite ||
		newData.Epoch < topicCoord.topicInfo.Epoch {
		coordLog.Warningf("topic (%v) epoch should be increased: %v, %v", topicDesp,
			newData, topicCoord.topicInfo)
		return ErrEpochLessThanCurrent
	}
	if newData.EpochForWrite == topicCoord.topicInfo.EpochForWrite {
		if newData.Leader != topicCoord.topicInfo.Leader {
			coordLog.Warningf("topic (%v) write epoch should be changed since the leader is changed: %v, %v", topicDesp, newData, topicCoord.topicInfo)
			return ErrTopicCoordStateInvalid
		}
		if len(newData.ISR) > len(topicCoord.topicInfo.ISR) {
			coordLog.Warningf("topic (%v) write epoch should be changed since new node in isr: %v, %v",
				topicDesp, newData, topicCoord.topicInfo)
			return ErrTopicCoordStateInvalid
		}
		for _, nid := range newData.ISR {
			if FindSlice(topicCoord.topicInfo.ISR, nid) == -1 {
				coordLog.Warningf("topic %v write epoch should be changed since new node in isr: %v, %v",
					topicDesp, newData, topicCoord.topicInfo)
				return ErrTopicCoordStateInvalid
			}
		}
	}
	if writeDisabled {
		return nil
	}
	if FindSlice(newData.ISR, self.myNode.GetID()) == -1 {
		return nil
	}
	if newData.EpochForWrite != topicCoord.topicInfo.EpochForWrite {
		return ErrTopicCoordStateInvalid
	}
	return nil
}

func (self *NsqdCoordinator) updateTopicInfo(topicCoord *TopicCoordinator, shouldDisableWrite bool, newTopicInfo *TopicPartitionMetaInfo) *CoordErr {
	if atomic.LoadInt32(&self.stopping) == 1 {
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
	// if any of new node in isr or leader is changed, the write disabled should be set first on isr nodes.
	if checkErr := self.checkUpdateState(topicCoord.coordData, disableWrite, newTopicInfo); checkErr != nil {
		coordLog.Warningf("topic (%v) check update failed : %v ",
			topicCoord.topicInfo.GetTopicDesp(), checkErr)
		topicCoord.dataMutex.Unlock()
		return checkErr
	}

	if topicCoord.IsExiting() {
		coordLog.Infof("update the topic info: %v while exiting.", oldData.topicInfo.GetTopicDesp())
		topicCoord.dataMutex.Unlock()
		return nil
	}

	coordLog.Infof("update the topic info: %v", topicCoord.topicInfo.GetTopicDesp())
	if topicCoord.coordData.GetLeader() == self.GetMyID() && newTopicInfo.Leader != self.GetMyID() {
		coordLog.Infof("my leader should release: %v", topicCoord.coordData)
		self.releaseTopicLeader(&topicCoord.coordData.topicInfo, &topicCoord.coordData.topicLeaderSession)
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
	self.switchStateForMaster(topicCoord, localTopic, false)
	return nil
}

func (self *NsqdCoordinator) notifyAcquireTopicLeader(coord *coordData) *CoordErr {
	if atomic.LoadInt32(&self.stopping) == 1 {
		return ErrClusterChanged
	}
	coordLog.Infof("I am notified to acquire topic leader %v.", coord.topicInfo)
	go self.acquireTopicLeader(&coord.topicInfo)
	return nil
}

func (self *NsqdCoordinator) switchStateForMaster(topicCoord *TopicCoordinator,
	localTopic *nsqd.Topic, syncCommitDisk bool) *CoordErr {
	// flush topic data and channel comsume data if any cluster topic info changed
	tcData := topicCoord.GetData()
	master := tcData.GetLeader() == self.myNode.GetID()
	// leader changed (maybe down), we make sure out data is flushed to keep data safe
	localTopic.ForceFlush()
	tcData.logMgr.FlushCommitLogs()
	tcData.logMgr.switchForMaster(master)
	if master {
		isWriteDisabled := topicCoord.IsWriteDisabled()
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
			if !isWriteDisabled && syncCommitDisk {
				localErr := localTopic.ResetBackendEndNoLock(nsqd.BackendOffset(logData.MsgOffset+int64(logData.MsgSize)),
					logData.MsgCnt+int64(logData.MsgNum)-1)
				if localErr != nil {
					coordLog.Errorf("reset local backend failed: %v", localErr)
				}
			}
		}
		localTopic.Unlock()
		coordLog.Infof("current topic %v write state: %v",
			tcData.topicInfo.GetTopicDesp(), isWriteDisabled)
		if !isWriteDisabled && tcData.IsISRReadyForWrite() {
			// try fix channel consume count here, since the old version has not count info,
			// we need restore from commit log.
			localTopic.EnableForMaster()
			chs := localTopic.GetChannelMapCopy()
			for _, ch := range chs {
				confirmed := ch.GetConfirmed()
				if confirmed.Offset() > 0 && confirmed.TotalMsgCnt() <= 0 {
					l, offset, cnt, err := self.SearchLogByMsgOffset(
						tcData.topicInfo.Name,
						tcData.topicInfo.Partition,
						int64(confirmed.Offset()))
					if err != nil {
						coordLog.Infof("search msg offset failed: %v", err)
					} else {
						coordLog.Warningf("try fix the channel %v confirmed queue info from %v to %v:%v, commitlog: %v",
							ch.GetName(), confirmed, offset, cnt, l)
						ch.SetConsumeOffset(nsqd.BackendOffset(offset), cnt, true)
					}
				}
			}
		} else {
			localTopic.DisableForSlave()
		}
	} else {
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
		}
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
		// it may not synced for  the new create channel and the leader changed.
		// to avoid init channel with end we need check here.
		ch, localErr := localTopic.GetExistingChannel(chName)
		if localErr != nil {
			offset.AllowBackward = true
			ch = localTopic.GetChannel(chName)
			coordLog.Warningf("slave init the channel : %v, %v, offset: %v",
				tcData.topicInfo.GetTopicDesp(), chName, ch.GetConfirmed())
		}

		currentConfirmed := ch.GetConfirmed()
		if !offset.AllowBackward && (nsqd.BackendOffset(offset.VOffset) <= currentConfirmed.Offset()) {
			continue
		}
		currentEnd := ch.GetChannelEnd()
		if nsqd.BackendOffset(offset.VOffset) > currentEnd.Offset() {
			continue
		}
		err := ch.ConfirmBackendQueueOnSlave(nsqd.BackendOffset(offset.VOffset), offset.VCnt, offset.AllowBackward)
		if err != nil {
			coordLog.Warningf("update local channel(%v) offset %v failed: %v, current channel end: %v, topic end: %v",
				chName, offset, err, currentEnd, localTopic.TotalDataSize())
		}
	}

	return nil
}

func (self *NsqdCoordinator) updateTopicLeaderSession(topicCoord *TopicCoordinator, newLS *TopicLeaderSession, joinSession string) *CoordErr {
	if atomic.LoadInt32(&self.stopping) == 1 {
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
	// if the write is disabled currently, we should make sure all isr is in synced
	mustSynced := topicCoord.IsWriteDisabled()

	localTopic, err := self.localNsqd.GetExistingTopic(tcData.topicInfo.Name, tcData.topicInfo.Partition)
	if err != nil {
		coordLog.Infof("no topic on local: %v, %v", tcData.topicInfo.GetTopicDesp(), err)
		return ErrLocalMissingTopic
	}
	dyConf := &nsqd.TopicDynamicConf{SyncEvery: int64(tcData.topicInfo.SyncEvery),
		AutoCommit:   0,
		RetentionDay: tcData.topicInfo.RetentionDay,
	}
	tcData.logMgr.updateBufferSize(int(dyConf.SyncEvery - 1))
	localTopic.SetDynamicInfo(*dyConf, tcData.logMgr)
	// leader changed (maybe down), we make sure out data is flushed to keep data safe
	self.switchStateForMaster(topicCoord, localTopic, false)

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
				go self.syncToNewLeader(tcData, joinSession, mustSynced)
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
	return -1, nil, ErrMissingTopicCoord.ToErrorType()
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
	coordErrStats.incTopicCoordMissingErr()
	return nil, ErrMissingTopicCoord
}

func (self *NsqdCoordinator) removeTopicCoord(topic string, partition int, removeData bool) (*TopicCoordinator, *CoordErr) {
	var topicCoord *TopicCoordinator
	self.coordMutex.Lock()
	defer self.coordMutex.Unlock()
	if v, ok := self.topicCoords[topic]; ok {
		if tc, ok := v[partition]; ok {
			topicCoord = tc
			delete(v, partition)
		}
	}
	var err *CoordErr
	if topicCoord == nil {
		err = ErrMissingTopicCoord
	} else {
		coordLog.Infof("removing topic coodinator: %v-%v", topic, partition)
		topicCoord.writeHold.Lock()
		defer topicCoord.writeHold.Unlock()
		topicCoord.DeleteNoWriteLock(removeData)
		err = nil
	}
	if removeData {
		coordLog.Infof("removing topic data: %v-%v", topic, partition)
		// check if any data on local and try remove
		self.forceCleanTopicData(topic, partition)
	}
	return topicCoord, err
}

func (self *NsqdCoordinator) trySyncTopicChannels(tcData *coordData) {
	localTopic, _ := self.localNsqd.GetExistingTopic(tcData.topicInfo.Name, tcData.topicInfo.Partition)
	if localTopic != nil {
		channels := localTopic.GetChannelMapCopy()
		var syncOffset ChannelConsumerOffset
		syncOffset.Flush = true
		for _, ch := range channels {
			confirmed := ch.GetConfirmed()
			// try fix message count here, since old version has no count info.
			if confirmed.Offset() > 0 && confirmed.TotalMsgCnt() <= 0 {
				l, offset, cnt, err := self.SearchLogByMsgOffset(tcData.topicInfo.Name, tcData.topicInfo.Partition, int64(confirmed.Offset()))
				if err != nil {
					coordLog.Infof("search msg offset failed: %v", err)
				} else {
					coordLog.Infof("try fix the channel %v confirmed queue info from %v to %v:%v, commitlog: %v",
						ch.GetName(), confirmed, offset, cnt, l)
					ch.SetConsumeOffset(nsqd.BackendOffset(offset), cnt, true)
					time.Sleep(time.Millisecond * 10)
					confirmed = ch.GetConfirmed()
				}
			}

			syncOffset.VOffset = int64(confirmed.Offset())
			syncOffset.VCnt = confirmed.TotalMsgCnt()

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
					coordLog.Infof("node %v update channel %v offset %v failed %v.", nodeID, ch.GetName(), syncOffset, rpcErr)
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
			coordLog.Infof("read topic %v data at offset %v, size: %v, error: %v", t.GetFullName(), offset, size, err)
			break
		}
		buf, err := snap.ReadRaw(size)
		if err != nil {
			coordLog.Infof("read topic data at offset %v, size:%v, error: %v", offset, size, err)
			break
		}
		dataList = append(dataList, buf)
	}
	if err != nil {
		return dataList, &CoordErr{err.Error(), RpcCommonErr, CoordLocalErr}
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
	dyConf := &nsqd.TopicDynamicConf{SyncEvery: int64(topicInfo.SyncEvery),
		AutoCommit:   0,
		RetentionDay: topicInfo.RetentionDay,
	}
	logMgr.updateBufferSize(int(dyConf.SyncEvery - 1))
	t.SetDynamicInfo(*dyConf, logMgr)

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
			localTopic, err := self.localNsqd.GetExistingTopic(topicName, pid)
			if err != nil {
				coordLog.Infof("no local topic")
				continue
			}

			if FindSlice(tcData.topicInfo.ISR, self.myNode.GetID()) == -1 {
				tpCoord.Exiting()
				localTopic.PrintCurrentStats()
				localTopic.ForceFlush()
				tcData.logMgr.FlushCommitLogs()
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
					coordLog.Infof("======= should wait leaving from isr: %v", topicName)
				} else {
					coordLog.Infof("======= request leave isr failed: %v", err)
				}
				time.Sleep(time.Millisecond * 100)
			}

			if tcData.IsMineLeaderSessionReady(self.GetMyID()) {
				// leader
				self.leadership.ReleaseTopicLeader(topicName, pid, &tcData.topicLeaderSession)
				coordLog.Infof("The leader for topic %v is transfered.", tcData.topicInfo.GetTopicDesp())
			}

			localTopic.PrintCurrentStats()
			localTopic.ForceFlush()
			tcData.logMgr.FlushCommitLogs()
			localTopic.Close()
		}
	}
	coordLog.Infof("prepare leaving finished.")
	if self.leadership != nil {
		atomic.StoreInt32(&self.stopping, 1)
		self.leadership.UnregisterNsqd(&self.myNode)
	}
}

func (self *NsqdCoordinator) Stats(topic string, part int) *CoordStats {
	s := &CoordStats{}
	if self.rpcServer != nil && self.rpcServer.rpcServer != nil {
		s.RpcStats = self.rpcServer.rpcServer.Stats.Snapshot()
	}
	s.ErrStats = *coordErrStats.GetCopy()
	s.TopicCoordStats = make([]TopicCoordStat, 0)
	if len(topic) > 0 {
		if part >= 0 {
			tcData, err := self.getTopicCoordData(topic, part)
			if err != nil {
			} else {
				var stat TopicCoordStat
				stat.Name = topic
				stat.Partition = part
				for _, nid := range tcData.topicInfo.ISR {
					stat.ISRStats = append(stat.ISRStats, ISRStat{HostName: "", NodeID: nid})
				}
				for _, nid := range tcData.topicInfo.CatchupList {
					stat.CatchupStats = append(stat.CatchupStats, CatchupStat{HostName: "", NodeID: nid, Progress: 0})
				}
				s.TopicCoordStats = append(s.TopicCoordStats, stat)
			}
		} else {
			self.coordMutex.RLock()
			defer self.coordMutex.RUnlock()
			v, ok := self.topicCoords[topic]
			if ok {
				for _, tc := range v {
					var stat TopicCoordStat
					stat.Name = topic
					stat.Partition = tc.topicInfo.Partition
					for _, nid := range tc.topicInfo.ISR {
						stat.ISRStats = append(stat.ISRStats, ISRStat{HostName: "", NodeID: nid})
					}
					for _, nid := range tc.topicInfo.CatchupList {
						stat.CatchupStats = append(stat.CatchupStats, CatchupStat{HostName: "", NodeID: nid, Progress: 0})
					}

					s.TopicCoordStats = append(s.TopicCoordStats, stat)
				}
			}
		}
	}
	return s
}
