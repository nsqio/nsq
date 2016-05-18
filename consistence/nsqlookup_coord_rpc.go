package consistence

import (
	"github.com/valyala/gorpc"
	"net"
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
	rpcDispatcher  *gorpc.Dispatcher
	rpcServer      *gorpc.Server
}

func NewNsqLookupCoordRpcServer(coord *NsqLookupCoordinator) *NsqLookupCoordRpcServer {
	return &NsqLookupCoordRpcServer{
		nsqLookupCoord: coord,
		rpcDispatcher:  gorpc.NewDispatcher(),
	}
}

func (self *NsqLookupCoordRpcServer) start(ip, port string) error {
	self.rpcDispatcher.AddService("NsqLookupCoordRpcServer", self)
	self.rpcServer = gorpc.NewTCPServer(net.JoinHostPort(ip, port), self.rpcDispatcher.NewHandlerFunc())
	e := self.rpcServer.Start()
	if e != nil {
		coordLog.Errorf("listen rpc error : %v", e)
		os.Exit(1)
	}

	coordLog.Infof("nsqlookup coordinator rpc listen at : %v", self.rpcServer.Listener.ListenAddr())
	return nil
}

func (self *NsqLookupCoordRpcServer) stop() {
	if self.rpcServer != nil {
		self.rpcServer.Stop()
	}
}

func (self *NsqLookupCoordRpcServer) RequestJoinCatchup(req *RpcReqJoinCatchup) *CoordErr {
	return self.nsqLookupCoord.handleRequestJoinCatchup(req.TopicName, req.TopicPartition, req.NodeID)
}

func (self *NsqLookupCoordRpcServer) RequestJoinTopicISR(req *RpcReqJoinISR) *CoordErr {
	return self.nsqLookupCoord.handleRequestJoinISR(req.TopicName, req.TopicPartition, req.NodeID)
}

func (self *NsqLookupCoordRpcServer) ReadyForTopicISR(req *RpcReadyForISR) *CoordErr {
	return self.nsqLookupCoord.handleReadyForISR(req.TopicName, req.TopicPartition, req.NodeID, req.LeaderSession, req.JoinISRSession)
}

func (self *NsqLookupCoordRpcServer) RequestLeaveFromISR(req *RpcReqLeaveFromISR) *CoordErr {
	return self.nsqLookupCoord.handleLeaveFromISR(req.TopicName, req.TopicPartition, nil, req.NodeID)
}

func (self *NsqLookupCoordRpcServer) RequestLeaveFromISRByLeader(req *RpcReqLeaveFromISRByLeader) *CoordErr {
	return self.nsqLookupCoord.handleLeaveFromISR(req.TopicName, req.TopicPartition, &req.LeaderSession, req.NodeID)
}
