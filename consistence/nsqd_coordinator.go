package consistence

import (
	"bytes"
	"errors"
	"github.com/golang/glog"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

var (
	ErrNotTopicLeader            = errors.New("not topic leader")
	ErrEpochMismatch             = errors.New("commit epoch not match")
	ErrWriteQuorumFailed         = errors.New("write to quorum failed.")
	ErrCommitLogIDDup            = errors.New("commit id duplicated")
	ErrMissingTopicLeaderSession = errors.New("missing topic leader session")
	ErrWriteDisabled             = errors.New("write is disabled on the topic")
	ErrPubArgError               = errors.New("pub argument error")
	ErrLocalFallBehind           = errors.New("local data fall behind")
	ErrLocalForwardThanLeader    = errors.New("local data is more than leader")
)

func GetTopicPartitionPath(topic string, partition int) string {
	return topic + "_" + strconv.Itoa(partition)
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

type ConsumerChanOffset struct {
	OffsetID   int64
	FileOffset int
}

type TopicSummaryData struct {
	topicInfo            TopicPartionMetaInfo
	topicLeaderSession   TopicLeaderSession
	disableWrite         bool
	channelConsumeOffset map[string]ConsumerChanOffset
}

type NsqdCoordinator struct {
	leadership      NSQDLeadership
	lookupLeader    *NsqLookupdNodeInfo
	topicsData      map[string]map[int]*TopicSummaryData
	myNode          NsqdNodeInfo
	nsqdRpcClients  map[string]*NsqdRpcClient
	topicLogMgr     map[string]map[int]*TopicCommitLogMgr
	flushNotifyChan chan TopicPartitionID
	stopChan        chan struct{}
	rpcListener     net.Listener
	dataRootPath    string
	localDataStates map[string]map[int]bool
}

func NewNsqdCoordinator(ip, tcpport, rpcport, extraID string, rootPath string) *NsqdCoordinator {
	nodeInfo := NsqdNodeInfo{
		NodeIp:  ip,
		TcpPort: tcpport,
		RpcPort: rpcport,
	}
	nodeInfo.ID = GenNsqdNodeID(&nodeInfo, extraID)
	return &NsqdCoordinator{
		leadership:      nil,
		topicsData:      make(map[string]map[int]*TopicSummaryData),
		myNode:          nodeInfo,
		nsqdRpcClients:  make(map[string]*NsqdRpcClient),
		topicLogMgr:     make(map[string]map[int]*TopicCommitLogMgr),
		flushNotifyChan: make(chan TopicPartitionID, 2),
		stopChan:        make(chan struct{}),
		dataRootPath:    rootPath,
		localDataStates: make(map[string]map[int]bool),
	}
}

func (self *NsqdCoordinator) getLogMgrWithoutCreate(topic string, partition int) (*TopicCommitLogMgr, error) {
	var mgr *TopicCommitLogMgr
	if v, ok := self.topicLogMgr[topic]; ok {
		if mgr, ok = v[partition]; ok {
			return mgr, nil
		}
	}
	return nil, ErrMissingTopic
}

func (self *NsqdCoordinator) getLogMgr(topic string, partition int) *TopicCommitLogMgr {
	mgr, err := self.getLogMgrWithoutCreate(topic, partition)
	if err == nil {
		return mgr
	}
	tmp, ok := self.topicLogMgr[topic]
	if !ok {
		tmp = make(map[int]*TopicCommitLogMgr)
	}

	mgr = InitTopicCommitLogMgr(topic, partition, GetTopicPartitionPath(topic, partition), DEFAULT_COMMIT_BUF_SIZE)
	tmp[partition] = mgr
	self.topicLogMgr[topic] = tmp
	return mgr
}

func (self *NsqdCoordinator) acquireRpcClient(nid string) (*NsqdRpcClient, error) {
	c, ok := self.nsqdRpcClients[nid]
	var err error
	if !ok {
		addr := ExtractRpcAddrFromID(nid)
		c, err = NewNsqdRpcClient(addr, RPC_TIMEOUT_SHORT)
		if err != nil {
			return nil, err
		}
		self.nsqdRpcClients[nid] = c
	}
	return c, nil
}

func (self *NsqdCoordinator) Start() error {
	rpc.Register(self)
	var e error
	self.rpcListener, e = net.Listen("tcp", ":"+self.myNode.RpcPort)
	if e != nil {
		glog.Warningf("listen rpc error : %v", e.Error())
		return e
	}
	go self.watchNsqLookupd()
	go self.loadLocalTopicData()
	go self.checkForUnusedTopics()
	// for each topic, wait other replicas and sync data with leader,
	// begin accept client request.
	rpc.Accept(self.rpcListener)
	return nil
}

func (self *NsqdCoordinator) Stop() {
	// give up the leadership on the topic to
	// allow other isr take over to avoid electing.
	close(self.stopChan)
	self.rpcListener.Close()
}

func (self *NsqdCoordinator) getLookupConn() (*NsqLookupRpcClient, error) {
	return NewNsqLookupRpcClient(net.JoinHostPort(self.lookupLeader.NodeIp, self.lookupLeader.RpcPort), time.Second)
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
				glog.Infof("nsqlookup leader changed: %v", n)
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
			glog.Infof("read topic partition file failed: %v", err)
		}
		for _, partitionStr := range partitionList {
			partition, err := strconv.Atoi(partitionStr)
			if err != nil {
				continue
			}
			glog.Infof("load topic: %v-%v", topicName, partition)
			if _, err := os.Stat(GetTopicPartitionLogPath(self.dataRootPath, topicName, partition)); os.IsNotExist(err) {
				glog.Infof("no commit log file under topic: %v-%v", topicName, partition)
				continue
			}
			//scan local File
			topicInfo, err := self.leadership.GetTopicInfo(topicName, partition)
			if err != nil {
				glog.Infof("failed to get topic info:%v-%v", topicName, partition)
				continue
			}
			if FindSlice(topicInfo.ISR, self.myNode.GetID()) != -1 {
				glog.Infof("I am starting as isr node.")
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
				glog.Infof("no need load the local topic since the replica is enough: %v-%v", topicName, partition)
				continue
			}
			err = RetryWithTimeout(func() error {
				err := self.requestJoinCatchup(topicName, partition)
				return err
			})
			if err != nil {
				glog.Infof("failed to request join catchup")
				continue
			}
			go self.catchupFromLeader(*topicInfo)
		}
	}
}

func (self *NsqdCoordinator) checkLocalTopicForISR(topicInfo *TopicPartionMetaInfo) error {
	logmgr, err := self.getLogMgrWithoutCreate(topicInfo.Name, topicInfo.Partition)
	if err != nil {
		glog.Warningf("get local log failed: %v", err)
		return err
	}
	if topicInfo.Leader == self.myNode.GetID() {
		// leader should always has the newest local data
		return nil
	}
	logid := logmgr.GetLastCommitLogID()
	c, err := self.acquireRpcClient(topicInfo.Leader)
	if err != nil {
		return err
	}
	leaderID, err := c.GetLastCommmitLogID(topicInfo)
	if err != nil {
		return err
	}
	if leaderID > logid {
		glog.Infof("this node is out of date, should rejoin.")
		// TODO: request the lookup to remove myself from isr
		return ErrLocalFallBehind
	}

	if logid > leaderID+1 {
		glog.Infof("this node has more data than leader, should rejoin.")
		return ErrLocalForwardThanLeader
	}
	return nil
}

func (self *NsqdCoordinator) checkForUnusedTopics() {
	ticker := time.NewTicker(time.Minute)
	for {
		select {
		case <-ticker.C:
			tmpChecks := make(map[string]map[int]bool, len(self.topicsData))
			for topic, info := range self.topicsData {
				for pid, summary := range info {
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
						glog.Infof("the topic should be clean since not relevance to me: %v", topicMeta)
						delete(self.topicsData[topic], pid)
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

func (self *NsqdCoordinator) IsMineLeaderForTopic(topic string, partition int) bool {
	t, ok := self.topicsData[topic]
	if !ok {
		return false
	}
	tp, ok := t[partition]
	if !ok {
		return false
	}
	return tp.topicLeaderSession.LeaderNode.GetID() == self.myNode.GetID()
}

func (self *NsqdCoordinator) syncToNewLeader(topic string, partition int, leader *TopicLeaderSession) {
	// check last commit log.
}

func (self *NsqdCoordinator) requestJoinCatchup(topic string, partition int) error {
	//
	c, err := self.getLookupConn()
	if err != nil {
		return err
	}
	err = c.RequestJoinCatchup(topic, partition, self.myNode.GetID())
	return err
}

func (self *NsqdCoordinator) requestJoinTopicISR(topicInfo *TopicPartionMetaInfo) (string, error) {
	// request change catchup to isr list and wait for nsqlookupd response to temp disable all new write.
	c, err := self.getLookupConn()
	if err != nil {
		return "", err
	}
	session, err := c.RequestJoinTopicISR(topicInfo.Name, topicInfo.Partition, self.myNode.GetID())
	return session, err
}

func (self *NsqdCoordinator) notifyReadyForTopicISR(topicInfo *TopicPartionMetaInfo, session string) error {
	// notify myself is ready for isr list and can accept new write.
	c, err := self.getLookupConn()
	if err != nil {
		return err
	}

	return c.ReadyForTopicISR(topicInfo.Name, topicInfo.Partition, self.myNode.GetID(), session)
}

func (self *NsqdCoordinator) prepareLeaveFromISR(topic string, partition int) error {
	c, err := self.getLookupConn()
	if err != nil {
		return err
	}
	return c.PrepareLeaveFromISR(topic, partition, self.myNode.GetID())
}

func (self *NsqdCoordinator) requestLeaveFromISR(topic string, partition int) error {
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
func (self *NsqdCoordinator) requestLeaveFromISRByLeader(topic string, partition int, nid string) error {
	topicData, err := self.checkWriteForTopicLeader(topic, partition)
	if err != nil {
		return err
	}
	// send request with leader session, so lookup can check the valid of session.
	c, err := self.getLookupConn()
	if err != nil {
		return err
	}
	return c.RequestLeaveFromISRByLeader(topic, partition, self.myNode.GetID(), &topicData.topicLeaderSession)
}

func (self *NsqdCoordinator) catchupFromLeader(topicInfo TopicPartionMetaInfo) {
	// get local commit log from check point , and pull newer logs from leader
	logmgr := self.getLogMgr(topicInfo.Name, topicInfo.Partition)
	offset, err := logmgr.GetLastLogOffset()
	if err != nil {
		glog.Warningf("catching failed since log offset read error: %v", err)
		return
	}
	// pull logdata from leader at the offset.
	c, err := self.acquireRpcClient(topicInfo.Leader)
	if err != nil {
		glog.Warningf("failed to get rpc client while catchup: %v", err)
		return
	}

	for offset > 0 {
		// TODO: check if leader changed
		localLogData, err := logmgr.GetCommmitLogFromOffset(offset)
		if err != nil {
			offset -= int64(GetLogDataSize())
			continue
		}

		leaderOffset, leaderLogData, err := c.GetCommmitLogFromOffset(&topicInfo, offset)
		if err == ErrCommitLogOutofBound || leaderOffset < offset {
			glog.Infof("local commit log is more than leader while catchup: %v vs %v", offset, leaderOffset)
			// local log is ahead of the leader, must truncate local data.
			// truncate commit log and truncate the data file to last log
			// commit offset.
			lastLog, err := logmgr.TruncateToOffset(leaderOffset + int64(GetLogDataSize()))
			if err != nil {
				glog.Infof("failed to truncate local commit log: %v", err)
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
			glog.Warningf("something wrong while get leader logdata while catchup: %v", err)
		} else {
			if *localLogData == leaderLogData {
				break
			}
		}
		offset -= int64(GetLogDataSize())
	}
	glog.Infof("local commit log match leader at: %v", offset)
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
			glog.Infof("error while get logs :%v", err)
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
			err := logmgr.AppendCommitLog(&l, true)
			if err != nil {
				glog.Infof("Failed to append local log: %v", err)
				return
			}
		}
		offset += int64(len(logs) * GetLogDataSize())

		if synced && !readyJoinISR {
			// notify nsqlookupd coordinator to add myself to isr list.
			s, err := self.requestJoinTopicISR(&topicInfo)
			if err != nil {
				glog.Infof("request join isr failed: %v", err)
				time.Sleep(time.Second)
			} else {
				joinSession = s
				logmgr.FlushCommitLogs()
				synced = false
				readyJoinISR = true
			}
		} else if synced && readyJoinISR {
			logmgr.FlushCommitLogs()
			glog.Infof("local topic is ready for isr: %v", topicInfo.GetTopicDesp())
			err := RetryWithTimeout(func() error {
				return self.notifyReadyForTopicISR(&topicInfo, joinSession)
			})
			if err != nil {
				glog.Infof("notify ready for isr failed: %v", err)
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

// any modify operation on the topic should check for topic leader.
func (self *NsqdCoordinator) checkWriteForTopicLeader(topic string, partition int) (*TopicSummaryData, error) {
	if v, ok := self.topicsData[topic]; ok {
		if topicInfo, ok := v[partition]; ok {
			if topicInfo.topicLeaderSession.LeaderNode.GetID() == self.myNode.GetID() {
				return nil, ErrNotTopicLeader
			}
			if topicInfo.disableWrite {
				return nil, ErrWriteDisabled
			}
			if topicInfo.topicLeaderSession.Session == "" {
				return nil, ErrMissingTopicLeaderSession
			}
			if topicInfo.topicInfo.Leader == self.myNode.GetID() {
				if len(topicInfo.topicInfo.ISR) <= topicInfo.topicInfo.Replica/2 {
					glog.Infof("No enough isr for topic %v while doing modification.", topicInfo.topicInfo.GetTopicDesp())
					return topicInfo, ErrWriteQuorumFailed
				}
				return topicInfo, nil
			}

		}
	}
	return nil, ErrNotTopicLeader
}

// write message to data file and return the file id and offset written.
func (self *NsqdCoordinator) pubMessageLocal(topic string, partition int, logid int64, message string) (int, int, error) {
	return 0, 0, nil
}

func (self *NsqdCoordinator) pubMessagesToCluster(topic string, partition int, messages []string) error {
	topicData, err := self.checkWriteForTopicLeader(topic, partition)
	if err != nil {
		return err
	}
	logMgr := self.getLogMgr(topic, partition)
	commitLogDataList := make([]CommitLogData, 0, len(messages))
	for _, message := range messages {
		logid := logMgr.nextLogID()
		_, msgOffset, err := self.pubMessageLocal(topic, partition, logid, message)
		if err != nil {
			return err
		}
		var l CommitLogData
		l.LogID = logid
		l.Epoch = topicData.topicLeaderSession.LeaderEpoch
		l.MsgOffset = msgOffset
		commitLogDataList = append(commitLogDataList, l)
	}
	success := 0
	// send message to slaves with current topic epoch
	for _, nodeID := range topicData.topicInfo.ISR {
		c, err := self.acquireRpcClient(nodeID)
		if err != nil {
			glog.Infof("get rpc client failed: %v", err)
			continue
		}
		err = c.PubMessage(topicData.topicLeaderSession.LeaderEpoch, &topicData.topicInfo, commitLogDataList, messages)
		if err == nil {
			success++
		}
	}

	if success > topicData.topicInfo.Replica/2 && success == len(topicData.topicInfo.ISR) {
		for _, l := range commitLogDataList {
			err := logMgr.AppendCommitLog(&l, false)
			if err != nil {
				panic(err)
			}
		}
		// TODO: success, move the write offset in the data file
		// move fail can be restored from the commit log while recover.
		return nil
	}
	return ErrWriteQuorumFailed
}

func (self *NsqdCoordinator) pubMessageOnSlave(topic string, partition int, loglist []CommitLogData, msgs []string) error {
	if len(loglist) != len(msgs) {
		glog.Warningf("the pub log size mismatch message size.")
		return ErrPubArgError
	}
	logMgr := self.getLogMgr(topic, partition)
	for i, l := range loglist {
		if logMgr.IsCommitted(l.LogID) {
			glog.Infof("pub the already commited log id : %v", l.LogID)
			return ErrCommitLogIDDup
		}
		_, msgOffset, err := self.pubMessageLocal(topic, partition, l.LogID, msgs[i])
		if err != nil {
			glog.Warningf("pub on slave failed: %v", err)
			return err
		}
		var newlog CommitLogData
		newlog.LogID = l.LogID
		newlog.Epoch = l.Epoch
		newlog.MsgOffset = msgOffset
		err = logMgr.AppendCommitLog(&newlog, true)
		if err != nil {
			glog.Infof("write commit log on slave failed: %v", err)
			return err
		}
	}
	return nil
}

func (self *NsqdCoordinator) updateChannelOffsetLocal(topic string, partition int, channel string, offset ConsumerChanOffset) error {
	self.topicsData[topic][partition].channelConsumeOffset[channel] = offset
	return nil
}

func (self *NsqdCoordinator) syncChannelOffsetToCluster(topic string, partition int, channel string, offset ConsumerChanOffset) error {
	topicData, err := self.checkWriteForTopicLeader(topic, partition)
	if err != nil {
		return err
	}
	err = self.updateChannelOffsetLocal(topic, partition, channel, offset)
	if err != nil {
		return err
	}
	// rpc call to slaves
	successNum := 0
	for _, nodeID := range topicData.topicInfo.ISR {
		c, err := self.acquireRpcClient(nodeID)
		if err != nil {
			glog.Infof("get rpc client failed: %v", err)
			continue
		}

		err = c.UpdateChannelOffset(topicData.topicLeaderSession.LeaderEpoch, &topicData.topicInfo, channel, offset)
		if err != nil {
		} else {
			successNum++
		}
	}
	if successNum > topicData.topicInfo.Replica/2 {
		if successNum != len(topicData.topicInfo.ISR) {
			glog.Infof("some nodes in isr is not synced with consume offset.")
		}
		return nil
	}
	return ErrWriteQuorumFailed
}

// flush cached data to disk. This should be called when topic isr list
// changed or leader changed.
func (self *NsqdCoordinator) NotifyFlushData(topic string, partition int) {
	if len(self.flushNotifyChan) > 1 {
		return
	}
	self.flushNotifyChan <- TopicPartitionID{topic, partition}
}

func (self *NsqdCoordinator) readMessageData(logID int64, fileOffset int) ([]byte, error) {
	return nil, nil
}

func (self *NsqdCoordinator) updateLocalTopic(topicInfo TopicPartionMetaInfo) error {
	// check topic exist and prepare on local.
	return nil
}

func (self *NsqdCoordinator) updateLocalTopicChannels(topicInfo TopicPartionMetaInfo) error {
	return nil
}

// before shutdown, we transfer the leader to others to reduce
// the unavailable time.
func (self *NsqdCoordinator) prepareLeavingCluster() {
	glog.Infof("I am prepare leaving the cluster.")
	for topicName, topicData := range self.topicsData {
		for pid, tpData := range topicData {
			if FindSlice(tpData.topicInfo.ISR, self.myNode.GetID()) == -1 {
				continue
			}
			self.prepareLeaveFromISR(topicName, pid)
			if len(tpData.topicInfo.ISR)-1 <= tpData.topicInfo.Replica/2 {
				glog.Infof("The isr nodes in topic %v is not enough, waiting...", tpData.topicInfo.GetTopicDesp())
				// we need notify lookup to add new isr since I am leaving.
				// wait until isr is enough or timeout.
				time.Sleep(time.Second * 30)
			}

			if tpData.topicLeaderSession.LeaderNode.GetID() != self.myNode.GetID() {
				// not leader
				self.requestLeaveFromISR(topicName, pid)
				continue
			}
			// notify lookup to transfer the leader to other node in the isr
			// wait leader transfer
			glog.Infof("The leader for topic %v is transfered.", tpData.topicInfo.GetTopicDesp())
		}
	}
	glog.Infof("prepare leaving finished.")
}
