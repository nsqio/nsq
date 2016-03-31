package consistence

import (
	"net"
	"net/rpc"
	"time"
)

type INsqlookupRemoteProxy interface {
	Reconnect() error
	RequestJoinCatchup(topic string, partition int, nid string) *CoordErr
	RequestJoinTopicISR(topic string, partition int, nid string) *CoordErr
	ReadyForTopicISR(topic string, partition int, nid string, leaderSession *TopicLeaderSession, joinISRSession string) *CoordErr
	RequestLeaveFromISR(topic string, partition int, nid string) *CoordErr
	RequestLeaveFromISRByLeader(topic string, partition int, nid string, leaderSession *TopicLeaderSession) *CoordErr
}

type nsqlookupRemoteProxyCreateFunc func(string, time.Duration) (INsqlookupRemoteProxy, error)

type NsqLookupRpcClient struct {
	remote     string
	timeout    time.Duration
	connection *rpc.Client
}

func NewNsqLookupRpcClient(addr string, timeout time.Duration) (INsqlookupRemoteProxy, error) {
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
			if err != nil {
				coordLog.Infof("rpc call %v error: %v", method, err)
			}
			return err
		}
	}
}

func (self *NsqLookupRpcClient) RequestJoinCatchup(topic string, partition int, nid string) *CoordErr {
	var req RpcReqJoinCatchup
	req.NodeID = nid
	req.TopicName = topic
	req.TopicPartition = partition
	var ret CoordErr
	err := self.CallWithRetry("NsqLookupCoordRpcServer.RequestJoinCatchup", req, &ret)
	return convertRpcError(err, &ret)
}

func (self *NsqLookupRpcClient) RequestJoinTopicISR(topic string, partition int, nid string) *CoordErr {
	var req RpcReqJoinISR
	req.NodeID = nid
	req.TopicName = topic
	req.TopicPartition = partition
	var ret CoordErr
	err := self.CallWithRetry("NsqLookupCoordRpcServer.RequestJoinTopicISR", req, &ret)
	return convertRpcError(err, &ret)
}

func (self *NsqLookupRpcClient) ReadyForTopicISR(topic string, partition int, nid string, leaderSession *TopicLeaderSession, joinISRSession string) *CoordErr {
	var req RpcReadyForISR
	req.NodeID = nid
	if leaderSession != nil {
		req.LeaderSession = *leaderSession
	}
	req.JoinISRSession = joinISRSession
	req.TopicName = topic
	req.TopicPartition = partition
	var ret CoordErr
	err := self.CallWithRetry("NsqLookupCoordRpcServer.ReadyForTopicISR", req, &ret)
	return convertRpcError(err, &ret)
}

func (self *NsqLookupRpcClient) RequestLeaveFromISR(topic string, partition int, nid string) *CoordErr {
	var req RpcReqLeaveFromISR
	req.NodeID = nid
	req.TopicName = topic
	req.TopicPartition = partition
	var ret CoordErr
	err := self.CallWithRetry("NsqLookupCoordRpcServer.RequestLeaveFromISR", req, &ret)
	return convertRpcError(err, &ret)
}

func (self *NsqLookupRpcClient) RequestLeaveFromISRByLeader(topic string, partition int, nid string, leaderSession *TopicLeaderSession) *CoordErr {
	var req RpcReqLeaveFromISRByLeader
	req.NodeID = nid
	req.TopicName = topic
	req.TopicPartition = partition
	req.LeaderSession = *leaderSession
	var ret CoordErr
	err := self.CallWithRetry("NsqLookupCoordRpcServer.RequestLeaveFromISRByLeader", req, &ret)
	return convertRpcError(err, &ret)
}
