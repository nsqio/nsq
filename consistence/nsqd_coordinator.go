package consistence

import (
	"errors"
	"github.com/golang/glog"
	"strconv"
)

var (
	ErrNotTopicLeader            = errors.New("not topic leader")
	ErrEpochMismatch             = errors.New("commit epoch not match")
	ErrWriteQuorumFailed         = errors.New("write to quorum failed.")
	ErrCommitLogIdDup            = errors.New("commit id duplicated")
	ErrMissingTopicLeaderSession = errors.New("missing topic leader session")
	ErrWriteDisabled             = errors.New("write is disabled on the topic")
	ErrPubArgError               = errors.New("pub argument error")
)

func GetTopicPartitionPath(topic string, partition int) string {
	return topic + "_" + strconv.Itoa(partition)
}

type ConsumerChanOffset struct {
	OffsetId   int64
	FileOffset int
}

type TopicSummaryData struct {
	topicInfo            TopicLeadershipInfo
	topicLeaderSession   TopicLeaderSession
	disableWrite         bool
	channelConsumeOffset map[string]ConsumerChanOffset
}

type NsqdCoordinator struct {
	leadership     NSQDLeadership
	lookupLeader   NsqLookupdNodeInfo
	topicsData     map[string]map[int]*TopicSummaryData
	myNode         NsqdNodeInfo
	coordID        string
	isMineLeader   bool
	nsqdRpcClients map[string]*NsqdRpcClient
	topicLogMgr    map[string]map[int]*TopicCommitLogMgr
}

func NewNsqdCoordinator(id string) *NsqdCoordinator {
	return &NsqdCoordinator{
		leadership: nil,
		topicsData: make(map[string]map[int]*TopicSummaryData),
		coordID:    id,
	}
}

func (self *NsqdCoordinator) getLogMgr(topic string, partition int) *TopicCommitLogMgr {
	var mgr *TopicCommitLogMgr
	tmp := make(map[int]*TopicCommitLogMgr)
	if v, ok := self.topicLogMgr[topic]; ok {
		tmp = v
		if mgr, ok = v[partition]; ok {
			return mgr
		}
	}

	mgr = InitTopicCommitLogMgr(topic, partition, GetTopicPartitionPath(topic, partition), DEFAULT_COMMIT_BUF_SIZE)
	tmp[partition] = mgr
	self.topicLogMgr[topic] = tmp
	return mgr
}

func (self *NsqdCoordinator) Start(port string) error {
	if err := StartNsqdCoordinatorRpcServer(self, port); err != nil {
		return err
	}
	return nil
}

func (self *NsqdCoordinator) watchNsqLookupd() {
	// watch the leader of nsqlookupd, always check the leader before response
	// to the nsqlookup admin operation.
	nsqlookupLeaderChan := make(chan NsqLookupdNodeInfo, 1)
	go self.leadership.WatchLookupdLeader("nsqlookup-leader", nsqlookupLeaderChan)
	for {
		select {
		case n, ok := <-nsqlookupLeaderChan:
			if !ok {
				return
			}
			if n.GetId() != self.lookupLeader.GetId() ||
				n.Epoch != self.lookupLeader.Epoch {
				glog.Infof("nsqlookup leader changed: %v", n)
				self.lookupLeader = n
			}
		}
	}
}

func (self *NsqdCoordinator) acquireTopicLeader(topicInfo TopicLeadershipInfo) error {
	err := self.leadership.AcquireTopicLeader(topicInfo.Name, topicInfo.Partition, self.myNode)
	return err
}

func (self *NsqdCoordinator) syncToNewLeader(topic string, partition int, leader *TopicLeaderSession) {
	// check last commit log.
}

func (self *NsqdCoordinator) getLocalCommitLogsReverse(topic string, partition int, startFrom int64, num int) ([]CommitLogData, error) {
	if startFrom < 0 {
		// read from end
		startFrom = 0
	}

	logs := make([]CommitLogData, 0)
	return logs, nil
}

func (self *NsqdCoordinator) getCommitLogFromLeader(topicInfo TopicLeadershipInfo, startFrom int64, num int) ([]CommitLogData, error) {
	logs, err := self.nsqdRpcClients[topicInfo.Leader].GetLeaderCommitLogs(topicInfo.Name, topicInfo.Partition, startFrom, num)
	return logs, err
}

func (self *NsqdCoordinator) compareCommitLogWithLeader(topicInfo TopicLeadershipInfo, startFrom int64, logs []CommitLogData) (int64, error) {
	leaderlogs, err := self.getCommitLogFromLeader(topicInfo, startFrom, len(logs))
	if len(leaderlogs) != len(logs) {
	}
	return -1, nil
}

func (self *NsqdCoordinator) pullTopicDataFromLeader(topicInfo TopicLeadershipInfo) error {
	// get local topic commit log id and send the last 100 logid to leader to
	// check and pull the missing logid.
	var startFrom int64
	startFrom = -1
	var pullStart int64
	for {
		logs, err := self.getLocalCommitLogsReverse(topicInfo.Name, topicInfo.Partition, startFrom, 100)
		if err != nil {
			glog.Infof("get local commit log failed: %v", err)
			return err
		}
		if len(logs) == 0 {
			break
		}
		diffLogFrom, err := self.compareCommitLogWithLeader(topicInfo, startFrom, logs)
		if err != nil {
			glog.Infof("compare log failed: %v", err)
			continue
		}
		if diffLogFrom == -1 {
			// all the logid is the same with leader
			pullStart = logs[len(logs)-1].LogId
			break
		} else {
			startFrom = diffLogFrom + 100
		}
	}
	// pull logs from leader from pullStart, each pull 100 logs, until no
	// newer logs.
	_ = pullStart

	// notify nsqlookupd coordinator to add myself to isr list.
	self.requestJoinTopicISR(topicInfo)
	return nil
}

func (self *NsqdCoordinator) requestJoinTopicISR(topicInfo TopicLeadershipInfo) {
	// wait for nsqlookupd response and check for topic leader for new logs.
}

func (self *NsqdCoordinator) requestJoinCatchup(topic string, partition int) error {
	//
	return nil
}

func (self *NsqdCoordinator) catchupFromLeader(topicInfo TopicLeadershipInfo) {
	// get local commit log from check point , and pull newer logs from leader
}

// any modify operation on the topic should check for topic leader.
func (self *NsqdCoordinator) checkWriteForTopicLeader(topic string, partition int) (*TopicLeadershipInfo, error) {
	if !self.isMineLeader {
		return nil, ErrNotTopicLeader
	}
	if v, ok := self.topicsData[topic]; ok {
		if topicInfo, ok := v[partition]; ok {
			if topicInfo.topicLeaderSession.leaderNode.GetId() == self.coordID {
				return nil, ErrNotTopicLeader
			}
			if topicInfo.disableWrite {
				return nil, ErrWriteDisabled
			}
			if topicInfo.topicLeaderSession.session == "" {
				return nil, ErrMissingTopicLeaderSession
			}
			if topicInfo.topicInfo.Leader == self.coordID {
				if len(topicInfo.topicInfo.ISR) <= topicInfo.topicInfo.Replica/2 {
					glog.Infof("No enough isr for topic %v while doing modification.", topicInfo.topicInfo.GetTopicDesp())
					return &topicInfo.topicInfo, ErrWriteQuorumFailed
				}
				return &topicInfo.topicInfo, nil
			}

		}
	}
	return nil, ErrNotTopicLeader
}

func (self *NsqdCoordinator) checkTopicEpoch(topic string, partition int, epoch int) (*TopicLeaderSession, error) {
	if v, ok := self.topicsData[topic]; ok {
		if topicInfo, ok := v[partition]; ok {
			if topicInfo.topicLeaderSession.topicEpoch != epoch {
				return nil, ErrEpochMismatch
			}
			return &topicInfo.topicLeaderSession, nil
		}
	}
	return nil, ErrEpochMismatch
}

// write message to data file and return the file id and offset written.
func (self *NsqdCoordinator) pubMessageLocal(topic string, partition int, logid int64, message string) (int, int, error) {
	return 0, 0, nil
}

func (self *NsqdCoordinator) pubMessagesToCluster(topic string, partition int, messages []string) error {
	topicInfo, err := self.checkWriteForTopicLeader(topic, partition)
	if err != nil {
		return err
	}
	logMgr := self.getLogMgr(topic, partition)
	commitLogDataList := make([]CommitLogData, 0, len(messages))
	for _, message := range messages {
		logid := logMgr.nextLogId()
		msgFileId, msgOffset, err := self.pubMessageLocal(topic, partition, logid, message)
		if err != nil {
			return err
		}
		var l CommitLogData
		l.LogId = logid
		l.Epoch = topicInfo.Epoch
		l.MsgFileId = msgFileId
		l.MsgOffset = msgOffset
		commitLogDataList = append(commitLogDataList, l)
	}
	success := 0
	// send message to slaves with current topic epoch
	for _, nodeId := range topicInfo.ISR {
		err := self.nsqdRpcClients[nodeId].PubMessage(topicInfo, commitLogDataList, messages)
		if err == nil {
			success++
		}
	}

	if success > topicInfo.Replica/2 && success == len(topicInfo.ISR) {
		for _, l := range commitLogDataList {
			err := logMgr.appendCommitLog(&l, false)
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
		if logMgr.isCommitted(l.LogId) {
			glog.Infof("pub the already commited log id : %v", l.LogId)
			return ErrCommitLogIdDup
		}
		msgFileId, msgOffset, err := self.pubMessageLocal(topic, partition, l.LogId, msgs[i])
		if err != nil {
			glog.Warningf("pub on slave failed: %v", err)
			return err
		}
		var newlog CommitLogData
		newlog.LogId = l.LogId
		newlog.Epoch = l.Epoch
		newlog.MsgFileId = msgFileId
		newlog.MsgOffset = msgOffset
		err = logMgr.appendCommitLog(&newlog, true)
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
	topicInfo, err := self.checkWriteForTopicLeader(topic, partition)
	if err != nil {
		return err
	}
	err = self.updateChannelOffsetLocal(topic, partition, channel, offset)
	if err != nil {
		return err
	}
	// rpc call to slaves
	successNum := 0
	for _, nodeId := range topicInfo.ISR {
		err := self.nsqdRpcClients[nodeId].UpdateChannelOffset(topicInfo, channel, offset)
		if err != nil {
		} else {
			successNum++
		}
	}
	if successNum > topicInfo.Replica/2 {
		return nil
	}
	return ErrWriteQuorumFailed
}

// for each topic data on local, check if necessary keep sync with the leader
// If no leader available, then join catchup list and wait for at least half
// ISR nodes.
func (self *NsqdCoordinator) loadLocalTopicData() error {
	for {
		topicName := ""
		partition := 0
		//scan local File
		topicInfo, err := self.leadership.GetTopicInfo(topicName, partition)
		if err != nil {
			glog.Infof("failed to get topic info.")
			continue
		}
		if len(topicInfo.ISR) >= topicInfo.Replica {
			glog.Infof("no need load the local topic since the replica is enough: %v-%v", topicName, partition)
			continue
		}
		err = self.requestJoinCatchup(topicName, partition)
		if err != nil {
			glog.Infof("join catchup failed.")
		}
		self.catchupFromLeader(*topicInfo)
	}
}

func (self *NsqdCoordinator) updateLocalTopic(topicInfo TopicLeadershipInfo) error {
	// check topic exist and prepare on local.
	return nil
}

func (self *NsqdCoordinator) createLocalTopicChannel(topicInfo TopicLeadershipInfo) error {
	return nil
}
