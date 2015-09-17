package consistence

import (
	"errors"
	"github.com/golang/glog"
	_ "github.com/valyala/gorpc"
	"net"
	"net/rpc"
)

var (
	ErrSessionMismatch = errors.New("session mismatch")
	ErrMissingTopic    = errors.New("missing topic")
)

func FindSlice(in []string, e string) int {
	for i, v := range in {
		if v == e {
			return i
		}
	}
	return -1
}

type NsqdNodeLoadFactor struct {
	nodeLF        float32
	topicLeaderLF map[string]map[int]float32
	topicSlaveLF  map[string]map[int]float32
}

type RpcAdminTopicInfo struct {
	TopicLeadershipInfo
	LookupdEpoch int
	DisableWrite bool
}

type RpcTopicLeaderSession struct {
	RpcTopicData
	TopicLeaderSession
	LookupdEpoch int
}

func StartNsqdCoordinatorRpcServer(coord *NsqdCoordinator, port string) error {
	rpc.Register(coord)
	l, e := net.Listen("tcp", ":"+port)
	if e != nil {
		glog.Warningf("listen rpc error : %v", e.Error())
		return e
	}
	go rpc.Accept(l)
	return nil
}

func (self *NsqdCoordinator) EnableTopicWrite(rpcTopicReq RpcAdminTopicInfo, ret *bool) error {
	// set the topic as not writable.
	if err := self.CheckLookupForWrite(rpcTopicReq.LookupdEpoch); err != nil {
		return err
	}
	if t, ok := self.topicsData[rpcTopicReq.Name]; ok {
		if tp, ok := t[rpcTopicReq.Partition]; ok {
			tp.disableWrite = false
			return nil
		}
	}

	*ret = true
	return ErrMissingTopic
}

func (self *NsqdCoordinator) DisableTopicWrite(rpcTopicReq RpcAdminTopicInfo, ret *bool) error {
	if err := self.CheckLookupForWrite(rpcTopicReq.LookupdEpoch); err != nil {
		return err
	}

	if t, ok := self.topicsData[rpcTopicReq.Name]; ok {
		if tp, ok := t[rpcTopicReq.Partition]; ok {
			tp.disableWrite = true
			return nil
		}
	}
	*ret = true
	return ErrMissingTopic
}

func (self *NsqdCoordinator) GetLastCommitLogId(topicInfo TopicLeadershipInfo, logid *int) error {
	*logid = 0
	return nil
}

func (self *NsqdCoordinator) GetTopicStats(topic string, stat *NodeTopicStats) error {
	if topic == "" {
		// all topic status
	}
	// the status of specific topic
	return nil
}

func (self *NsqdCoordinator) GetNsqdLoadFactors(empty string, lf *NsqdNodeLoadFactor) error {
	// load factor based on the data consumed and the data storage for per CPU
	return nil
}

func (self *NsqdCoordinator) AddCatchupToTopic(rpcTopicReq RpcAdminTopicInfo, ret *bool) error {
	if err := self.CheckLookupForWrite(rpcTopicReq.LookupdEpoch); err != nil {
		return err
	}
	return nil
}

func (self *NsqdCoordinator) AddChannelForTopic(rpcTopicReq RpcAdminTopicInfo, ret *bool) error {
	if err := self.CheckLookupForWrite(rpcTopicReq.LookupdEpoch); err != nil {
		return err
	}
	return nil
}

func (self *NsqdCoordinator) CheckLookupForWrite(lookupEpoch int) error {
	if lookupEpoch < self.lookupLeader.Epoch {
		glog.Warningf("the lookupd epoch is smaller than last: %v", lookupEpoch)
		return ErrEpochMismatch
	}
	return nil
}

func (self *NsqdCoordinator) NotifyTopicLeaderSession(rpcTopicReq RpcTopicLeaderSession, ret *bool) error {
	*ret = true
	if err := self.CheckLookupForWrite(rpcTopicReq.LookupdEpoch); err != nil {
		return err
	}
	if _, ok := self.topicsData[rpcTopicReq.TopicName]; !ok {
		return ErrTopicNotCreated
	}
	topicPartitionInfo, ok := self.topicsData[rpcTopicReq.TopicName][rpcTopicReq.TopicPartition]
	if !ok {
		glog.Infof("topic partition missing.")
		return ErrTopicNotCreated
	}
	n := rpcTopicReq.TopicLeaderSession
	topicPartitionInfo.topicLeaderSession = rpcTopicReq.TopicLeaderSession
	if n.leaderNode == nil {
		self.isMineLeader = false
		glog.Infof("topic leader is missing : %v", rpcTopicReq.RpcTopicData)
	} else if n.leaderNode.GetId() == self.coordID {
		glog.Infof("I become the leader for the topic: %v", rpcTopicReq.RpcTopicData)
		self.isMineLeader = true
	} else {
		self.isMineLeader = false
		glog.Infof("topic %v leader changed to :%v. epoch: %v", rpcTopicReq.RpcTopicData, n.leaderNode.GetId(), n.topicEpoch)
		// if catching up, pull data from the new leader
		// if isr, make sure sync to the new leader
		if FindSlice(topicPartitionInfo.topicInfo.ISR, self.coordID) != -1 {
			self.syncToNewLeader(rpcTopicReq.TopicName, rpcTopicReq.TopicPartition, &n)
		} else if FindSlice(topicPartitionInfo.topicInfo.CatchupList, self.coordID) != -1 {
			self.catchupFromLeader(topicPartitionInfo.topicInfo)
		}
	}

	return nil
}

func (self *NsqdCoordinator) UpdateTopicInfo(rpcTopicReq RpcAdminTopicInfo, ret *bool) error {
	if err := self.CheckLookupForWrite(rpcTopicReq.LookupdEpoch); err != nil {
		return err
	}
	glog.Infof("got update request for topic : %v", rpcTopicReq)
	topicData, ok := self.topicsData[rpcTopicReq.Name]
	if !ok {
		topicData = make(map[int]*TopicSummaryData)
		self.topicsData[rpcTopicReq.Name] = topicData
	}
	if _, ok := topicData[rpcTopicReq.Partition]; !ok {
		topicData[rpcTopicReq.Partition] = &TopicSummaryData{disableWrite: true}
		rpcTopicReq.DisableWrite = true
	}
	topicData[rpcTopicReq.Partition].topicInfo = rpcTopicReq.TopicLeadershipInfo
	self.updateLocalTopic(rpcTopicReq.TopicLeadershipInfo)
	if rpcTopicReq.Leader == self.coordID {
		if !self.isMineLeader {
			glog.Infof("I am notified to be leader for the topic.")
			// leader switch need disable write until the lookup notify leader
			// to accept write.
			rpcTopicReq.DisableWrite = true
		}
		if rpcTopicReq.DisableWrite {
			topicData[rpcTopicReq.Partition].disableWrite = true
		}
		err := self.acquireTopicLeader(rpcTopicReq.TopicLeadershipInfo)
		if err != nil {
			glog.Infof("acquire topic leader failed.")
		}
	} else if FindSlice(rpcTopicReq.ISR, self.coordID) != -1 {
		glog.Infof("I am in isr list.")
	} else if FindSlice(rpcTopicReq.CatchupList, self.coordID) != -1 {
		glog.Infof("I am in catchup list.")
	} else {
		glog.Infof("Not a topic related to me.")
	}
	return nil
}

type RpcTopicData struct {
	TopicName      string
	TopicPartition int
	TopicSession   string
	TopicEpoch     int
}

type RpcChannelOffsetArg struct {
	RpcTopicData
	Channel string
	// position file + file offset
	ChannelOffset ConsumerChanOffset
}

type RpcPubMessage struct {
	RpcTopicData
	LogList       []CommitLogData
	TopicMessages []string
}

type RpcCommitLog struct {
	RpcTopicData
	LogId    int64
	LogEpoch int
	MsgId    string
}

type RpcPullCommitLogsReq struct {
	RpcTopicData
	StartLogId int64
	LogNum     int
}

type RpcPullCommitLogsRsp struct {
	Logs []CommitLogData
}

func (self *NsqdCoordinator) checkForRpcCall(rpcData RpcTopicData) (*TopicLeaderSession, error) {
	if v, ok := self.topicsData[rpcData.TopicName]; ok {
		if topicInfo, ok := v[rpcData.TopicPartition]; ok {
			if topicInfo.topicLeaderSession.topicEpoch != rpcData.TopicEpoch {
				glog.Infof("rpc call with wrong epoch :%v", rpcData)
				return nil, ErrEpochMismatch
			}
			if topicInfo.topicLeaderSession.session != rpcData.TopicSession {
				glog.Infof("rpc call with wrong session:%v", rpcData)
				return nil, ErrSessionMismatch
			}
			return &topicInfo.topicLeaderSession, nil
		}
	}
	glog.Infof("rpc call with missing topic :%v", rpcData)
	return nil, ErrMissingTopic
}

func (self *NsqdCoordinator) UpdateChannelOffset(info RpcChannelOffsetArg, ret *bool) error {
	_, err := self.checkForRpcCall(info.RpcTopicData)
	if err != nil {
		return err
	}
	// update local channel offset
	*ret = true
	self.updateChannelOffsetLocal(info.TopicName, info.TopicPartition, info.Channel, info.ChannelOffset)
	return nil
}

// receive from leader
func (self *NsqdCoordinator) PubMessage(info RpcPubMessage, ret *bool) error {
	_, err := self.checkForRpcCall(info.RpcTopicData)
	if err != nil {
		return err
	}
	// do local pub message
	err = self.pubMessageOnSlave(info.TopicName, info.TopicPartition, info.LogList, info.TopicMessages)
	*ret = true
	return err
}

func (self *NsqdCoordinator) CommitLog(info RpcCommitLog, ret *bool) error {
	_, err := self.checkForRpcCall(info.RpcTopicData)
	if err != nil {
		return err
	}
	// check if the log id is larger than local, commit log on local
	*ret = true
	return nil
}

func (self *NsqdCoordinator) GetLeaderCommitLogs(info RpcPullCommitLogsReq, ret *RpcPullCommitLogsRsp) error {
	if !self.isMineLeader {
		return ErrNotLeader
	}
	var err error
	(*ret).Logs, err = self.getLocalCommitLogsReverse(info.TopicName, info.TopicPartition, info.StartLogId, info.LogNum)
	return err
}
