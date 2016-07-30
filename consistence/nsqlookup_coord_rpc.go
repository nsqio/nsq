package consistence

import (
	"github.com/absolute8511/gorpc"
	"net"
	"os"
	"time"
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

type RpcReqNewTopicInfo struct {
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
	lastNotify     time.Time
}

func NewNsqLookupCoordRpcServer(coord *NsqLookupCoordinator) *NsqLookupCoordRpcServer {
	return &NsqLookupCoordRpcServer{
		nsqLookupCoord: coord,
		rpcDispatcher:  gorpc.NewDispatcher(),
		lastNotify:     time.Now(),
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
	s := time.Now().Unix()
	defer func() {
		e := time.Now().Unix()
		if e-s > int64(RPC_TIMEOUT/2) {
			coordLog.Infof("rpc call used: %v", e-s)
		}
	}()
	var ret CoordErr
	err := self.nsqLookupCoord.handleRequestJoinCatchup(req.TopicName, req.TopicPartition, req.NodeID)
	if err != nil {
		ret = *err
		return &ret
	}
	return &ret
}

func (self *NsqLookupCoordRpcServer) RequestJoinTopicISR(req *RpcReqJoinISR) *CoordErr {
	s := time.Now().Unix()
	defer func() {
		e := time.Now().Unix()
		if e-s > int64(RPC_TIMEOUT/2) {
			coordLog.Infof("rpc call used: %v", e-s)
		}
	}()

	var ret CoordErr
	err := self.nsqLookupCoord.handleRequestJoinISR(req.TopicName, req.TopicPartition, req.NodeID)
	if err != nil {
		ret = *err
		return &ret
	}
	return &ret
}

func (self *NsqLookupCoordRpcServer) ReadyForTopicISR(req *RpcReadyForISR) *CoordErr {
	s := time.Now().Unix()
	defer func() {
		e := time.Now().Unix()
		if e-s > int64(RPC_TIMEOUT/2) {
			coordLog.Infof("rpc call used: %v", e-s)
		}
	}()

	var ret CoordErr
	err := self.nsqLookupCoord.handleReadyForISR(req.TopicName, req.TopicPartition, req.NodeID, req.LeaderSession, req.JoinISRSession)
	if err != nil {
		ret = *err
		return &ret
	}
	return &ret
}

func (self *NsqLookupCoordRpcServer) RequestLeaveFromISR(req *RpcReqLeaveFromISR) *CoordErr {
	s := time.Now().Unix()
	defer func() {
		e := time.Now().Unix()
		if e-s > int64(RPC_TIMEOUT/2) {
			coordLog.Infof("rpc call used: %v", e-s)
		}
	}()

	var ret CoordErr
	err := self.nsqLookupCoord.handleLeaveFromISR(req.TopicName, req.TopicPartition, nil, req.NodeID)
	if err != nil {
		ret = *err
		return &ret
	}
	return &ret
}

func (self *NsqLookupCoordRpcServer) RequestLeaveFromISRByLeader(req *RpcReqLeaveFromISRByLeader) *CoordErr {
	s := time.Now().Unix()
	defer func() {
		e := time.Now().Unix()
		if e-s > int64(RPC_TIMEOUT/2) {
			coordLog.Infof("rpc call used: %v", e-s)
		}
	}()

	var ret CoordErr
	err := self.nsqLookupCoord.handleLeaveFromISR(req.TopicName, req.TopicPartition, &req.LeaderSession, req.NodeID)
	if err != nil {
		ret = *err
		return &ret
	}
	return &ret
}

func (self *NsqLookupCoordRpcServer) RequestNotifyNewTopicInfo(req *RpcReqJoinCatchup) *CoordErr {
	var coordErr CoordErr
	if time.Since(self.lastNotify) < time.Millisecond*10 {
		return &coordErr
	}
	self.nsqLookupCoord.handleRequestNewTopicInfo(req.TopicName, req.TopicPartition, req.NodeID)
	return &coordErr
}
