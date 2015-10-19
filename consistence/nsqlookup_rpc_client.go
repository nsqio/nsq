package consistence

import (
	"github.com/golang/glog"
	"net"
	"net/rpc"
	"time"
)

type NsqLookupRpcClient struct {
	remote     string
	timeout    time.Duration
	connection *rpc.Client
}

func NewNsqLookupRpcClient(addr string, timeout time.Duration) (*NsqLookupRpcClient, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}

	return &NsqLookupRpcClient{
		remote:     addr,
		timeout:    timeout,
		connection: rpc.NewClient(conn),
	}, nil
}

func (self *NsqLookupRpcClient) Reconnect() error {
	conn, err := net.DialTimeout("tcp", self.remote, self.timeout)
	if err != nil {
		return err
	}
	self.connection.Close()
	self.connection = rpc.NewClient(conn)
	return nil
}

func (self *NsqLookupRpcClient) CallWithRetry(method string, arg interface{}, reply interface{}) error {
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

func (self *NsqLookupRpcClient) RequestJoinCatchup(topic string, partition int, nid string) error {
	var req RpcReqJoinCatchup
	req.NodeID = nid
	req.TopicName = topic
	req.TopicPartition = partition
	var ret RpcRspBase
	return self.CallWithRetry("NSQLookupdCoordinator.RpcReqJoinCatchup", req, &ret)
}

func (self *NsqLookupRpcClient) RequestJoinTopicISR(topic string, partition int, nid string) (string, error) {
	var req RpcReqJoinISR
	req.NodeID = nid
	req.TopicName = topic
	req.TopicPartition = partition
	var ret RpcRspJoinISR
	err := self.CallWithRetry("NSQLookupdCoordinator.RpcReqJoinCatchup", req, &ret)
	return ret.ReqSession, err
}

func (self *NsqLookupRpcClient) ReadyForTopicISR(topic string, partition int, nid string, session string) error {
	var req RpcReadyForJoinISR
	req.NodeID = nid
	req.ReqSession = session
	req.TopicName = topic
	req.TopicPartition = partition
	var ret RpcRspBase
	return self.CallWithRetry("NSQLookupdCoordinator.RpcReadyForJoinISR", req, &ret)
}

func (self *NsqLookupRpcClient) PrepareLeaveFromISR(topic string, partition int, nid string) error {
	var req RpcPrepareLeaveFromISR
	req.NodeID = nid
	req.TopicName = topic
	req.TopicPartition = partition
	var ret RpcRspBase
	return self.CallWithRetry("NSQLookupdCoordinator.RpcPrepareLeaveFromISR", req, &ret)
}

func (self *NsqLookupRpcClient) RequestLeaveFromISR(topic string, partition int, nid string) error {
	var req RpcReqLeaveFromISR
	req.NodeID = nid
	req.TopicName = topic
	req.TopicPartition = partition
	var ret RpcRspBase
	return self.CallWithRetry("NSQLookupdCoordinator.RpcReqLeaveFromISR", req, &ret)
}

func (self *NsqLookupRpcClient) RequestLeaveFromISRByLeader(topic string, partition int, nid string, leaderSession *TopicLeaderSession) error {
	var req RpcReqLeaveFromISRByLeader
	req.NodeID = nid
	req.TopicName = topic
	req.TopicPartition = partition
	req.LeaderSession = *leaderSession
	var ret RpcRspBase
	return self.CallWithRetry("NSQLookupdCoordinator.RpcReqLeaveFromISRByLeader", req, &ret)
}
