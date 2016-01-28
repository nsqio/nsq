package consistence

import (
	"github.com/absolute8511/nsq/nsqd"
	"net"
	"net/rpc"
	"time"
)

const (
	RPC_TIMEOUT       = time.Duration(time.Second * 10)
	RPC_TIMEOUT_SHORT = time.Duration(time.Second)
)

type NsqdRpcClient struct {
	remote     string
	timeout    time.Duration
	connection *rpc.Client
}

func NewNsqdRpcClient(addr string, timeout time.Duration) (*NsqdRpcClient, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}

	return &NsqdRpcClient{
		remote:     addr,
		timeout:    timeout,
		connection: rpc.NewClient(conn),
	}, nil
}

func (self *NsqdRpcClient) Reconnect() error {
	conn, err := net.DialTimeout("tcp", self.remote, self.timeout)
	if err != nil {
		return err
	}
	self.connection.Close()
	self.connection = rpc.NewClient(conn)
	return nil
}

func (self *NsqdRpcClient) CallWithRetry(method string, arg interface{}, reply interface{}) error {
	for {
		err := self.connection.Call(method, arg, reply)
		if err == rpc.ErrShutdown {
			err = self.Reconnect()
			if err != nil {
				return err
			}
		} else {
			coordLog.Infof("rpc call %v error: %v", method, err)
			return err
		}
	}
}

func (self *NsqdRpcClient) NotifyTopicLeaderSession(epoch int, topicInfo *TopicPartionMetaInfo, leaderSession *TopicLeaderSession) error {
	var rpcInfo RpcTopicLeaderSession
	rpcInfo.LookupdEpoch = epoch
	rpcInfo.TopicLeaderSession = *leaderSession
	rpcInfo.TopicName = topicInfo.Name
	rpcInfo.TopicPartition = topicInfo.Partition
	rpcInfo.TopicEpoch = leaderSession.LeaderEpoch
	rpcInfo.TopicSession = leaderSession.Session
	ret := false
	return self.CallWithRetry("NsqdCoordinator.NotifyTopicLeaderSession", rpcInfo, &ret)
}

func (self *NsqdRpcClient) UpdateTopicInfo(epoch int, topicInfo *TopicPartionMetaInfo) error {
	var rpcInfo RpcAdminTopicInfo
	rpcInfo.LookupdEpoch = epoch
	rpcInfo.TopicPartionMetaInfo = *topicInfo
	ret := false
	return self.CallWithRetry("NsqdCoordinator.UpdateTopicInfo", rpcInfo, &ret)
}

func (self *NsqdRpcClient) EnableTopicWrite(epoch int, topicInfo *TopicPartionMetaInfo) error {
	var rpcInfo RpcAdminTopicInfo
	rpcInfo.LookupdEpoch = epoch
	rpcInfo.TopicPartionMetaInfo = *topicInfo
	ret := false
	return self.CallWithRetry("NsqdCoordinator.EnableTopicWrite", rpcInfo, &ret)
}

func (self *NsqdRpcClient) DisableTopicWrite(epoch int, topicInfo *TopicPartionMetaInfo) error {
	var rpcInfo RpcAdminTopicInfo
	rpcInfo.LookupdEpoch = epoch
	rpcInfo.TopicPartionMetaInfo = *topicInfo
	ret := false
	return self.CallWithRetry("NsqdCoordinator.DisableTopicWrite", rpcInfo, &ret)
}

func (self *NsqdRpcClient) GetTopicStats(topic string) (*NodeTopicStats, error) {
	var stat NodeTopicStats
	err := self.CallWithRetry("NsqdCoordinator.GetTopicStats", topic, &stat)
	return &stat, err
}

func (self *NsqdRpcClient) UpdateCatchupForTopic(epoch int, info *TopicPartionMetaInfo) error {
	var rpcReq RpcAdminTopicInfo
	rpcReq.TopicPartionMetaInfo = *info
	rpcReq.LookupdEpoch = epoch
	ret := false
	return self.CallWithRetry("NsqdCoordinator.UpdateCatchupForTopic", rpcReq, &ret)
}

func (self *NsqdRpcClient) UpdateChannelsForTopic(epoch int, info *TopicPartionMetaInfo) error {
	var rpcReq RpcAdminTopicInfo
	rpcReq.TopicPartionMetaInfo = *info
	rpcReq.LookupdEpoch = epoch
	ret := false
	return self.CallWithRetry("NsqdCoordinator.UpdateChannelsForTopic", rpcReq, &ret)
}

func (self *NsqdRpcClient) UpdateChannelOffset(leaderEpoch int, info *TopicPartionMetaInfo, channel string, offset ConsumerChanOffset) error {
	var updateInfo RpcChannelOffsetArg
	updateInfo.TopicName = info.Name
	updateInfo.TopicPartition = info.Partition
	updateInfo.TopicEpoch = info.Epoch
	updateInfo.TopicLeaderEpoch = leaderEpoch
	updateInfo.Channel = channel
	updateInfo.ChannelOffset = offset
	ret := false
	err := self.CallWithRetry("NsqdCoordinator.UpdateChannelOffset", updateInfo, &ret)
	return err
}

func (self *NsqdRpcClient) PutMessage(leaderEpoch int, info *TopicPartionMetaInfo, log CommitLogData, message *nsqd.Message) error {
	var pubData RpcPutMessage
	pubData.LogData = log
	pubData.TopicName = info.Name
	pubData.TopicPartition = info.Partition
	pubData.TopicMessage = message
	pubData.TopicEpoch = info.Epoch
	pubData.TopicLeaderEpoch = leaderEpoch
	ret := false
	err := self.CallWithRetry("NsqdCoordinator.PutMessage", pubData, &ret)
	return err
}

func (self *NsqdRpcClient) PutMessages(leaderEpoch int, info *TopicPartionMetaInfo, loglist []CommitLogData, messages []*nsqd.Message) error {
	var pubData RpcPutMessages
	pubData.LogList = loglist
	pubData.TopicName = info.Name
	pubData.TopicPartition = info.Partition
	pubData.TopicMessages = messages
	pubData.TopicEpoch = info.Epoch
	pubData.TopicLeaderEpoch = leaderEpoch
	ret := false
	err := self.CallWithRetry("NsqdCoordinator.PutMessages", pubData, &ret)
	return err
}

func (self *NsqdRpcClient) GetLastCommmitLogID(topicInfo *TopicPartionMetaInfo) (int64, error) {
	var req RpcCommitLogReq
	req.TopicName = topicInfo.Name
	req.TopicPartition = topicInfo.Partition
	var ret int64
	err := self.CallWithRetry("NsqdCoordinator.GetLastCommitLogID", req, &ret)
	return ret, err
}

func (self *NsqdRpcClient) GetCommmitLogFromOffset(topicInfo *TopicPartionMetaInfo, offset int64) (int64, CommitLogData, error) {
	var req RpcCommitLogReq
	req.LogOffset = offset
	req.TopicName = topicInfo.Name
	req.TopicPartition = topicInfo.Partition
	var rsp RpcCommitLogRsp
	err := self.CallWithRetry("NsqdCoordinator.GetCommmitLogFromOffset", req, &rsp)
	return rsp.LogOffset, rsp.LogData, err
}

func (self *NsqdRpcClient) PullCommitLogsAndData(topic string, partition int,
	startOffset int64, num int) ([]CommitLogData, [][]byte, error) {
	var r RpcPullCommitLogsReq
	r.TopicName = topic
	r.TopicPartition = partition
	r.StartLogOffset = startOffset
	r.LogMaxNum = num
	var ret RpcPullCommitLogsRsp
	err := self.CallWithRetry("NsqdCoordinator.PullCommitLogs", r, &ret)
	return ret.Logs, ret.DataList, err
}
