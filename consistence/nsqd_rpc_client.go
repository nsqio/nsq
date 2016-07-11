package consistence

import (
	"github.com/absolute8511/gorpc"
	pb "github.com/absolute8511/nsq/consistence/coordgrpc"
	"github.com/absolute8511/nsq/nsqd"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"net"
	"strconv"
	"sync"
	"time"
)

const (
	RPC_TIMEOUT            = time.Duration(time.Second * 5)
	RPC_TIMEOUT_SHORT      = time.Duration(time.Second * 3)
	RPC_TIMEOUT_FOR_LOOKUP = time.Duration(time.Second * 1)
)

type NsqdRpcClient struct {
	sync.Mutex
	remote     string
	timeout    time.Duration
	d          *gorpc.Dispatcher
	c          *gorpc.Client
	dc         *gorpc.DispatcherClient
	grpcClient pb.NsqdCoordRpcV2Client
	grpcConn   *grpc.ClientConn
}

func convertRpcError(err error, errInterface interface{}) *CoordErr {
	if err != nil {
		return NewCoordErr(err.Error(), CoordNetErr)
	}
	if errInterface == nil {
		return nil
	}
	coordErr, ok := errInterface.(*CoordErr)
	if ok {
		if coordErr != nil && coordErr.HasError() {
			return coordErr
		}
	} else if pbErr, ok := errInterface.(*pb.CoordErr); ok {
		if pbErr != nil && (pbErr.ErrType != 0 || pbErr.ErrCode != 0) {
			return &CoordErr{
				ErrMsg:  pbErr.ErrMsg,
				ErrCode: ErrRPCRetCode(pbErr.ErrCode),
				ErrType: CoordErrType(pbErr.ErrType),
			}
		}
	} else {
		return NewCoordErr("Not an Invalid CoordErr", CoordCommonErr)
	}
	return nil
}

func NewNsqdRpcClient(addr string, timeout time.Duration) (*NsqdRpcClient, error) {
	c := gorpc.NewTCPClient(addr)
	c.RequestTimeout = timeout
	c.DisableCompression = true
	c.Start()
	d := gorpc.NewDispatcher()
	d.AddService("NsqdCoordRpcServer", &NsqdCoordRpcServer{})
	ip, port, _ := net.SplitHostPort(addr)
	portNum, _ := strconv.Atoi(port)
	grpcAddr := ip + ":" + strconv.Itoa(portNum+1)
	grpcConn, err := grpc.Dial(grpcAddr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(timeout))
	var grpcClient pb.NsqdCoordRpcV2Client
	if err != nil {
		coordLog.Warningf("failed to connect to grpc server %v: %v", grpcAddr, err)
		grpcClient = nil
		grpcConn = nil
	} else {
		grpcClient = pb.NewNsqdCoordRpcV2Client(grpcConn)
	}
	coordLog.Infof("connected to rpc server %v: %v", grpcAddr, addr)

	return &NsqdRpcClient{
		remote:     addr,
		timeout:    timeout,
		d:          d,
		c:          c,
		dc:         d.NewServiceClient("NsqdCoordRpcServer", c),
		grpcClient: grpcClient,
		grpcConn:   grpcConn,
	}, nil
}

func (self *NsqdRpcClient) Close() {
	self.Lock()
	if self.c != nil {
		self.c.Stop()
		self.c = nil
	}
	if self.grpcConn != nil {
		self.grpcConn.Close()
		self.grpcConn = nil
	}
	self.Unlock()
}

func (self *NsqdRpcClient) Reconnect() error {
	self.Lock()
	if self.c != nil {
		self.c.Stop()
	}
	if self.grpcConn != nil {
		self.grpcConn.Close()
	}
	self.c = gorpc.NewTCPClient(self.remote)
	self.c.RequestTimeout = self.timeout
	self.c.DisableCompression = true
	self.dc = self.d.NewServiceClient("NsqdCoordRpcServer", self.c)
	self.c.Start()

	ip, port, _ := net.SplitHostPort(self.remote)
	portNum, _ := strconv.Atoi(port)
	grpcAddr := ip + ":" + strconv.Itoa(portNum+1)
	grpcConn, err := grpc.Dial(grpcAddr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(self.timeout))
	if err != nil {
		coordLog.Warningf("failed to connect to grpc server %v: %v", grpcAddr, err)
		self.grpcConn = nil
		self.grpcClient = nil
	} else {
		self.grpcConn = grpcConn
		self.grpcClient = pb.NewNsqdCoordRpcV2Client(grpcConn)
	}
	coordLog.Infof("connected to rpc server %v: %v", grpcAddr, self.remote)

	self.Unlock()
	return nil
}

func (self *NsqdRpcClient) CallFast(method string, arg interface{}) (interface{}, error) {
	reply, err := self.dc.CallTimeout(method, arg, time.Second)
	return reply, err
}

func (self *NsqdRpcClient) CallWithRetry(method string, arg interface{}) (interface{}, error) {
	for {
		reply, err := self.dc.Call(method, arg)
		if err != nil && err.(*gorpc.ClientError).Connection {
			coordLog.Infof("rpc connection closed, error: %v", err)
			err = self.Reconnect()
			if err != nil {
				return reply, err
			}
		} else {
			if err != nil {
				coordLog.Debugf("rpc call %v error: %v", method, err)
			}
			return reply, err
		}
	}
}

func (self *NsqdRpcClient) NotifyTopicLeaderSession(epoch EpochType, topicInfo *TopicPartitionMetaInfo, leaderSession *TopicLeaderSession, joinSession string) *CoordErr {
	var rpcInfo RpcTopicLeaderSession
	rpcInfo.LookupdEpoch = epoch
	rpcInfo.TopicLeaderSession = leaderSession.Session
	rpcInfo.TopicLeaderSessionEpoch = leaderSession.LeaderEpoch
	if leaderSession.LeaderNode != nil {
		rpcInfo.LeaderNode = *leaderSession.LeaderNode
	}
	rpcInfo.JoinSession = joinSession
	rpcInfo.TopicName = topicInfo.Name
	rpcInfo.TopicPartition = topicInfo.Partition
	retErr, err := self.CallWithRetry("NotifyTopicLeaderSession", &rpcInfo)
	return convertRpcError(err, retErr)
}

func (self *NsqdRpcClient) NotifyAcquireTopicLeader(epoch EpochType, topicInfo *TopicPartitionMetaInfo) *CoordErr {
	var rpcInfo RpcAcquireTopicLeaderReq
	rpcInfo.LookupdEpoch = epoch
	rpcInfo.TopicName = topicInfo.Name
	rpcInfo.TopicPartition = topicInfo.Partition
	rpcInfo.TopicWriteEpoch = topicInfo.EpochForWrite
	rpcInfo.Epoch = topicInfo.Epoch
	rpcInfo.LeaderNodeID = topicInfo.Leader
	retErr, err := self.CallWithRetry("NotifyAcquireTopicLeader", &rpcInfo)
	return convertRpcError(err, retErr)
}

func (self *NsqdRpcClient) UpdateTopicInfo(epoch EpochType, topicInfo *TopicPartitionMetaInfo) *CoordErr {
	var rpcInfo RpcAdminTopicInfo
	rpcInfo.LookupdEpoch = epoch
	rpcInfo.TopicPartitionMetaInfo = *topicInfo
	retErr, err := self.CallWithRetry("UpdateTopicInfo", &rpcInfo)
	return convertRpcError(err, retErr)
}

func (self *NsqdRpcClient) EnableTopicWrite(epoch EpochType, topicInfo *TopicPartitionMetaInfo) *CoordErr {
	var rpcInfo RpcAdminTopicInfo
	rpcInfo.LookupdEpoch = epoch
	rpcInfo.TopicPartitionMetaInfo = *topicInfo
	retErr, err := self.CallWithRetry("EnableTopicWrite", &rpcInfo)
	return convertRpcError(err, retErr)
}

func (self *NsqdRpcClient) DisableTopicWriteFast(epoch EpochType, topicInfo *TopicPartitionMetaInfo) *CoordErr {
	var rpcInfo RpcAdminTopicInfo
	rpcInfo.LookupdEpoch = epoch
	rpcInfo.TopicPartitionMetaInfo = *topicInfo
	retErr, err := self.CallFast("DisableTopicWrite", &rpcInfo)
	return convertRpcError(err, retErr)
}

func (self *NsqdRpcClient) DisableTopicWrite(epoch EpochType, topicInfo *TopicPartitionMetaInfo) *CoordErr {
	var rpcInfo RpcAdminTopicInfo
	rpcInfo.LookupdEpoch = epoch
	rpcInfo.TopicPartitionMetaInfo = *topicInfo
	retErr, err := self.CallWithRetry("DisableTopicWrite", &rpcInfo)
	return convertRpcError(err, retErr)
}

func (self *NsqdRpcClient) DeleteNsqdTopic(epoch EpochType, topicInfo *TopicPartitionMetaInfo) *CoordErr {
	var rpcInfo RpcAdminTopicInfo
	rpcInfo.LookupdEpoch = epoch
	rpcInfo.TopicPartitionMetaInfo = *topicInfo
	retErr, err := self.CallWithRetry("DeleteNsqdTopic", &rpcInfo)
	return convertRpcError(err, retErr)
}

func (self *NsqdRpcClient) IsTopicWriteDisabled(topicInfo *TopicPartitionMetaInfo) bool {
	var rpcInfo RpcAdminTopicInfo
	rpcInfo.TopicPartitionMetaInfo = *topicInfo
	ret, err := self.CallWithRetry("IsTopicWriteDisabled", &rpcInfo)
	if err != nil {
		return false
	}
	return ret.(bool)
}

func (self *NsqdRpcClient) GetTopicStats(topic string) (*NodeTopicStats, error) {
	stat, err := self.CallWithRetry("GetTopicStats", topic)
	return stat.(*NodeTopicStats), err
}

func (self *NsqdRpcClient) NotifyUpdateChannelOffset(leaderSession *TopicLeaderSession, info *TopicPartitionMetaInfo, channel string, offset ChannelConsumerOffset) *CoordErr {
	var updateInfo RpcChannelOffsetArg
	updateInfo.TopicName = info.Name
	updateInfo.TopicPartition = info.Partition
	updateInfo.TopicWriteEpoch = info.EpochForWrite
	updateInfo.Epoch = info.Epoch
	updateInfo.TopicLeaderSessionEpoch = leaderSession.LeaderEpoch
	updateInfo.TopicLeaderSession = leaderSession.Session
	updateInfo.Channel = channel
	updateInfo.ChannelOffset = offset
	err := self.dc.Send("UpdateChannelOffset", &updateInfo)
	return convertRpcError(err, nil)
}

func (self *NsqdRpcClient) UpdateChannelOffset(leaderSession *TopicLeaderSession, info *TopicPartitionMetaInfo, channel string, offset ChannelConsumerOffset) *CoordErr {
	if self.grpcClient != nil {
		var req pb.RpcChannelOffsetArg
		var rpcData pb.RpcTopicData
		rpcData.TopicName = info.Name
		rpcData.TopicPartition = int32(info.Partition)
		rpcData.TopicWriteEpoch = int64(info.EpochForWrite)
		rpcData.Epoch = int64(info.Epoch)
		rpcData.TopicLeaderSessionEpoch = int64(leaderSession.LeaderEpoch)
		rpcData.TopicLeaderSession = leaderSession.Session
		req.TopicData = &rpcData
		req.Channel = channel
		req.ChannelOffset.Voffset = offset.VOffset
		req.ChannelOffset.Flush = offset.Flush
		req.ChannelOffset.AllowBackward = offset.AllowBackward

		ctx, cancel := context.WithTimeout(context.Background(), RPC_TIMEOUT_SHORT)
		retErr, err := self.grpcClient.UpdateChannelOffset(ctx, &req)
		cancel()
		if err == nil {
			return convertRpcError(err, retErr)
		}
		// maybe old server not implemented the grpc method.
	}

	var updateInfo RpcChannelOffsetArg
	updateInfo.TopicName = info.Name
	updateInfo.TopicPartition = info.Partition
	updateInfo.TopicWriteEpoch = info.EpochForWrite
	updateInfo.Epoch = info.Epoch
	updateInfo.TopicLeaderSessionEpoch = leaderSession.LeaderEpoch
	updateInfo.TopicLeaderSession = leaderSession.Session
	updateInfo.Channel = channel
	updateInfo.ChannelOffset = offset
	retErr, err := self.CallFast("UpdateChannelOffset", &updateInfo)
	return convertRpcError(err, retErr)
}

func (self *NsqdRpcClient) PutMessage(leaderSession *TopicLeaderSession, info *TopicPartitionMetaInfo, log CommitLogData, message *nsqd.Message) *CoordErr {
	if self.grpcClient != nil && false {
		ctx, cancel := context.WithTimeout(context.Background(), RPC_TIMEOUT_SHORT)
		var req pb.RpcPutMessage
		var rpcData pb.RpcTopicData
		rpcData.TopicName = info.Name
		rpcData.TopicPartition = int32(info.Partition)
		rpcData.TopicWriteEpoch = int64(info.EpochForWrite)
		rpcData.Epoch = int64(info.Epoch)
		rpcData.TopicLeaderSessionEpoch = int64(leaderSession.LeaderEpoch)
		rpcData.TopicLeaderSession = leaderSession.Session
		req.TopicData = &rpcData
		var pbLogData pb.CommitLogData
		pbLogData.LogID = log.LogID
		pbLogData.Epoch = int64(log.Epoch)
		pbLogData.MsgNum = log.MsgNum
		pbLogData.MsgCnt = log.MsgCnt
		pbLogData.MsgSize = log.MsgSize
		pbLogData.MsgOffset = log.MsgOffset
		pbLogData.LastMsgLogID = log.LastMsgLogID
		req.LogData = &pbLogData

		var msg pb.NsqdMessage
		msg.ID = uint64(message.ID)
		msg.Body = message.Body
		msg.Trace_ID = message.TraceID
		msg.Attemps = uint32(message.Attempts)
		msg.Timestamp = message.Timestamp
		req.TopicMessage = &msg

		retErr, err := self.grpcClient.PutMessage(ctx, &req)
		cancel()
		if err == nil {
			return convertRpcError(err, retErr)
		}
		// maybe old server not implemented the grpc method.
	}
	var putData RpcPutMessage
	putData.LogData = log
	putData.TopicName = info.Name
	putData.TopicPartition = info.Partition
	putData.TopicMessage = message
	putData.TopicWriteEpoch = info.EpochForWrite
	putData.Epoch = info.Epoch
	putData.TopicLeaderSessionEpoch = leaderSession.LeaderEpoch
	putData.TopicLeaderSession = leaderSession.Session
	retErr, err := self.CallWithRetry("PutMessage", &putData)
	return convertRpcError(err, retErr)
}

func (self *NsqdRpcClient) PutMessages(leaderSession *TopicLeaderSession, info *TopicPartitionMetaInfo, log CommitLogData, messages []*nsqd.Message) *CoordErr {
	if self.grpcClient != nil && false {
		ctx, cancel := context.WithTimeout(context.Background(), RPC_TIMEOUT_SHORT)
		var req pb.RpcPutMessages
		var rpcData pb.RpcTopicData
		rpcData.TopicName = info.Name
		rpcData.TopicPartition = int32(info.Partition)
		rpcData.TopicWriteEpoch = int64(info.EpochForWrite)
		rpcData.Epoch = int64(info.Epoch)
		rpcData.TopicLeaderSessionEpoch = int64(leaderSession.LeaderEpoch)
		rpcData.TopicLeaderSession = leaderSession.Session

		req.TopicData = &rpcData
		var pbLogData pb.CommitLogData
		pbLogData.LogID = log.LogID
		pbLogData.Epoch = int64(log.Epoch)
		pbLogData.MsgNum = log.MsgNum
		pbLogData.MsgCnt = log.MsgCnt
		pbLogData.MsgSize = log.MsgSize
		pbLogData.MsgOffset = log.MsgOffset
		pbLogData.LastMsgLogID = log.LastMsgLogID
		req.LogData = &pbLogData

		for _, message := range messages {
			var msg pb.NsqdMessage
			msg.ID = uint64(message.ID)
			msg.Body = message.Body
			msg.Trace_ID = message.TraceID
			msg.Attemps = uint32(message.Attempts)
			msg.Timestamp = message.Timestamp
			req.TopicMessage = append(req.TopicMessage, &msg)
		}

		retErr, err := self.grpcClient.PutMessages(ctx, &req)
		cancel()
		if err == nil {
			return convertRpcError(err, retErr)
		}
		// maybe old server not implemented the grpc method.
	}

	var putData RpcPutMessages
	putData.LogData = log
	putData.TopicName = info.Name
	putData.TopicPartition = info.Partition
	putData.TopicMessages = messages
	putData.TopicWriteEpoch = info.EpochForWrite
	putData.Epoch = info.Epoch
	putData.TopicLeaderSessionEpoch = leaderSession.LeaderEpoch
	putData.TopicLeaderSession = leaderSession.Session
	retErr, err := self.CallWithRetry("PutMessages", &putData)
	return convertRpcError(err, retErr)
}

func (self *NsqdRpcClient) GetLastCommitLogID(topicInfo *TopicPartitionMetaInfo) (int64, *CoordErr) {
	var req RpcCommitLogReq
	req.TopicName = topicInfo.Name
	req.TopicPartition = topicInfo.Partition
	var retErr CoordErr
	ret, err := self.CallWithRetry("GetLastCommitLogID", &req)
	if err != nil || ret == nil {
		return 0, convertRpcError(err, &retErr)
	}
	return ret.(int64), convertRpcError(err, &retErr)
}

func (self *NsqdRpcClient) GetCommitLogFromOffset(topicInfo *TopicPartitionMetaInfo, logIndex int64, offset int64) (int64, int64, CommitLogData, *CoordErr) {
	var req RpcCommitLogReq
	req.LogStartIndex = logIndex
	req.LogOffset = offset
	req.TopicName = topicInfo.Name
	req.TopicPartition = topicInfo.Partition
	var rsp *RpcCommitLogRsp
	rspVar, err := self.CallWithRetry("GetCommitLogFromOffset", &req)
	if err != nil {
		return 0, 0, CommitLogData{}, convertRpcError(err, nil)
	}
	rsp = rspVar.(*RpcCommitLogRsp)
	return rsp.LogStartIndex, rsp.LogOffset, rsp.LogData, convertRpcError(err, &rsp.ErrInfo)
}

func (self *NsqdRpcClient) PullCommitLogsAndData(topic string, partition int,
	logIndex int64, startOffset int64, num int) ([]CommitLogData, [][]byte, error) {
	var r RpcPullCommitLogsReq
	r.TopicName = topic
	r.TopicPartition = partition
	r.StartLogOffset = startOffset
	r.StartIndexCnt = logIndex
	r.LogMaxNum = num
	var ret *RpcPullCommitLogsRsp
	retVar, err := self.CallWithRetry("PullCommitLogsAndData", &r)
	if err != nil {
		return nil, nil, err
	}
	ret = retVar.(*RpcPullCommitLogsRsp)
	return ret.Logs, ret.DataList, nil
}

func (self *NsqdRpcClient) CallRpcTest(data string) (string, *CoordErr) {
	var req RpcTestReq
	req.Data = data
	var ret *RpcTestRsp
	retVar, err := self.CallWithRetry("TestRpcError", &req)
	if err != nil {
		return "", convertRpcError(err, nil)
	}
	ret = retVar.(*RpcTestRsp)
	return ret.RspData, convertRpcError(err, ret.RetErr)
}

func (self *NsqdRpcClient) CallRpcTesttimeout(data string) error {
	_, err := self.CallWithRetry("TestRpcTimeout", "req")
	return err
}
