package consistence

import (
	"net"
	"net/rpc"
	"os"
)

type RpcLookupReqBase struct {
	TopicName      string
	TopicPartition int
	NodeID         string
}

type RpcReqJoinCatchup struct {
	RpcLookupReqBase
}

type RpcReqJoinISR struct {
	RpcLookupReqBase
}

type RpcReadyForISR struct {
	RpcLookupReqBase
	LeaderSession  TopicLeaderSession
	JoinISRSession string
}

type RpcRspJoinISR struct {
	CoordErr
	JoinISRSession string
}

type RpcReqLeaveFromISR struct {
	RpcLookupReqBase
}

type RpcReqLeaveFromISRByLeader struct {
	RpcLookupReqBase
	LeaderSession TopicLeaderSession
}

type NsqLookupCoordRpcServer struct {
	nsqLookupCoord *NsqLookupCoordinator
	rpcListener    net.Listener
	rpcServer      *rpc.Server
}

func NewNsqLookupCoordRpcServer(coord *NsqLookupCoordinator) *NsqLookupCoordRpcServer {
	return &NsqLookupCoordRpcServer{
		nsqLookupCoord: coord,
		rpcServer:      rpc.NewServer(),
	}
}

func (self *NsqLookupCoordRpcServer) start(ip, port string) error {
	e := self.rpcServer.Register(self)
	if e != nil {
		panic(e)
	}
	self.rpcListener, e = net.Listen("tcp4", ip+":"+port)
	if e != nil {
		coordLog.Errorf("listen rpc error : %v", e.Error())
		os.Exit(1)
	}

	coordLog.Infof("nsqlookup coordinator rpc listen at : %v", self.rpcListener.Addr())
	self.rpcServer.Accept(self.rpcListener)
	return nil
}

func (self *NsqLookupCoordRpcServer) stop() {
	if self.rpcListener != nil {
		self.rpcListener.Close()
	}
}

func (self *NsqLookupCoordRpcServer) RequestJoinCatchup(req RpcReqJoinCatchup, ret *CoordErr) error {
	err := self.nsqLookupCoord.handleRequestJoinCatchup(req.TopicName, req.TopicPartition, req.NodeID)
	if err != nil {
		*ret = *err
	}
	return nil
}

func (self *NsqLookupCoordRpcServer) RequestJoinTopicISR(req RpcReqJoinISR, ret *CoordErr) error {
	err := self.nsqLookupCoord.handleRequestJoinISR(req.TopicName, req.TopicPartition, req.NodeID)
	if err != nil {
		*ret = *err
	}
	return nil
}

func (self *NsqLookupCoordRpcServer) ReadyForTopicISR(req RpcReadyForISR, ret *CoordErr) error {
	err := self.nsqLookupCoord.handleReadyForISR(req.TopicName, req.TopicPartition, req.NodeID, req.LeaderSession, req.JoinISRSession)
	if err != nil {
		*ret = *err
	}
	return nil
}

func (self *NsqLookupCoordRpcServer) RequestLeaveFromISR(req RpcReqLeaveFromISR, ret *CoordErr) error {
	err := self.nsqLookupCoord.handleLeaveFromISR(req.TopicName, req.TopicPartition, nil, req.NodeID)
	if err != nil {
		*ret = *err
	}
	return nil
}

func (self *NsqLookupCoordRpcServer) RequestLeaveFromISRByLeader(req RpcReqLeaveFromISRByLeader, ret *CoordErr) error {
	err := self.nsqLookupCoord.handleLeaveFromISR(req.TopicName, req.TopicPartition, &req.LeaderSession, req.NodeID)
	if err != nil {
		*ret = *err
	}
	return nil
}
