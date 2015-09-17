package consistence

import (
	"github.com/golang/glog"
	"net"
	"net/rpc"
	"time"
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
			glog.Infof("rpc call %v error: %v", method, err)
			return err
		}
	}
}

func (self *NsqdRpcClient) NotifyTopicLeaderSession(epoch int, topicInfo *TopicLeadershipInfo, leaderSession *TopicLeaderSession) error {
	var rpcInfo RpcTopicLeaderSession
	rpcInfo.LookupdEpoch = epoch
	rpcInfo.TopicLeaderSession = leaderSession
	rpcInfo.TopicName = topicInfo.Name
	rpcInfo.TopicPartition = topicInfo.Partition
	rpcInfo.TopicEpoch = leaderSession.topicEpoch
	rpcInfo.TopicSession = leaderSession.session
	ret := false
	return self.CallWithRetry("NsqdCoordinator.NotifyTopicLeaderSession", rpcInfo, &ret)
}

func (self *NsqdRpcClient) UpdateTopicInfo(epoch int, topicInfo *TopicLeadershipInfo) error {
	var rpcInfo RpcAdminTopicInfo
	rpcInfo.LookupdEpoch = epoch
	rpcInfo.TopicLeadershipInfo = *topicInfo
	ret := false
	return self.CallWithRetry("NsqdCoordinator.UpdateTopicInfo", rpcInfo, &ret)
}

func (self *NsqdRpcClient) EnableTopicWrite(epoch int, topicInfo *TopicLeadershipInfo) error {
	var rpcInfo RpcAdminTopicInfo
	rpcInfo.LookupdEpoch = epoch
	rpcInfo.TopicLeadershipInfo = *topicInfo
	ret := false
	return self.CallWithRetry("NsqdCoordinator.EnableTopicWrite", rpcInfo, &ret)
}

func (self *NsqdRpcClient) DisableTopicWrite(epoch int, topicInfo *TopicLeadershipInfo) error {
	var rpcInfo RpcAdminTopicInfo
	rpcInfo.LookupdEpoch = epoch
	rpcInfo.TopicLeadershipInfo = *topicInfo
	ret := false
	return self.CallWithRetry("NsqdCoordinator.DisableTopicWrite", rpcInfo, &ret)
}

func (self *NsqdRpcClient) GetLastCommitLogId(topicInfo *TopicLeadershipInfo) (int, error) {
	logid := 0
	err := self.CallWithRetry("NsqdCoordinator.GetLastCommitLogId", topicInfo, &logid)
	return logid, err
}

func (self *NsqdRpcClient) GetTopicStats(topic string) (*NodeTopicStats, error) {
	var stat NodeTopicStats
	err := self.CallWithRetry("NsqdCoordinator.GetTopicStats", topic, &stat)
	return &stat, err
}

func (self *NsqdRpcClient) UpdateChannelOffset(info *TopicLeadershipInfo, channel string, offset ConsumerChanOffset) error {
	var updateInfo RpcChannelOffsetArg
	updateInfo.TopicName = info.Name
	updateInfo.TopicPartition = info.Partition
	updateInfo.TopicEpoch = info.Epoch
	updateInfo.Channel = channel
	updateInfo.ChannelOffset = offset
	ret := false
	err := self.CallWithRetry("NsqdCoordinator.UpdateChannelOffset", updateInfo, &ret)
	return err
}

func (self *NsqdRpcClient) PubMessage(info *TopicLeadershipInfo, loglist []CommitLogData, messages []string) error {
	var pubData RpcPubMessage
	pubData.LogList = loglist
	pubData.TopicName = info.Name
	pubData.TopicPartition = info.Partition
	pubData.TopicMessages = messages
	pubData.TopicEpoch = info.Epoch
	ret := false
	err := self.CallWithRetry("NsqdCoordinator.PubMessage", pubData, &ret)
	return err
}

func (self *NsqdRpcClient) CommitLog(info *TopicLeadershipInfo, logid int64, msgid string) error {
	var r RpcCommitLog
	r.TopicName = info.Name
	r.TopicPartition = info.Partition
	r.TopicEpoch = info.Epoch
	r.LogId = logid
	r.LogEpoch = info.Epoch
	r.MsgId = msgid
	ret := false
	err := self.CallWithRetry("NsqdCoordinator.CommitLog", r, &ret)
	return err
}

func (self *NsqdRpcClient) GetLeaderCommitLogs(topic string, partition int, startLogId int64, num int) ([]CommitLogData, error) {
	var r RpcPullCommitLogsReq
	r.TopicName = topic
	r.TopicPartition = partition
	r.StartLogId = startLogId
	r.LogNum = num
	var ret RpcPullCommitLogsRsp
	err := self.CallWithRetry("NsqdCoordinator.GetLeaderCommitLogs", r, &ret)
	return ret.Logs, err
}
