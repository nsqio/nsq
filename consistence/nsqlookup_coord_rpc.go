package consistence

import (
	"errors"
)

var (
	ErrLookupRpc = errors.New("lookup rpc call failed")
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

type RpcReadyForJoinISR struct {
	RpcLookupReqBase
	ReqSession string
}

type RpcRspBase struct {
	RetCode ErrRPCRetCode
	ErrInfo error
}

type RpcRspJoinISR struct {
	RpcRspBase
	ReqSession string
}

type RpcPrepareLeaveFromISR struct {
	RpcLookupReqBase
}

type RpcReqLeaveFromISR struct {
	RpcLookupReqBase
}

type RpcReqLeaveFromISRByLeader struct {
	RpcLookupReqBase
	LeaderSession TopicLeaderSession
}

func (self *NSQLookupdCoordinator) RequestJoinCatchup(req RpcReqJoinCatchup, ret *RpcRspBase) error {
	err := self.handleRequestJoinCatchup(req.TopicName, req.TopicPartition, req.NodeID)
	ret.ErrInfo = err
	ret.RetCode = GetRpcErrCode(err)
	return err
}

func (self *NSQLookupdCoordinator) RequestJoinTopicISR(req RpcReqJoinISR, ret *RpcRspJoinISR) error {
	session, err := self.handleRequestJoinISR(req.TopicName, req.TopicPartition, req.NodeID)
	ret.ErrInfo = err
	ret.ReqSession = session
	ret.RetCode = GetRpcErrCode(err)
	return err
}

func (self *NSQLookupdCoordinator) ReadyForTopicISR(req RpcReadyForJoinISR, ret *RpcRspBase) error {
	err := self.handleReadyForJoinISR(req.TopicName, req.TopicPartition, req.NodeID, req.ReqSession)
	ret.ErrInfo = err
	ret.RetCode = GetRpcErrCode(err)
	return err
}

func (self *NSQLookupdCoordinator) PrepareLeaveFromISR(req RpcPrepareLeaveFromISR, ret *RpcRspBase) error {
	err := self.handlePrepareLeaveFromISR(req.TopicName, req.TopicPartition, req.NodeID)
	ret.ErrInfo = err
	ret.RetCode = GetRpcErrCode(err)
	return err
}

func (self *NSQLookupdCoordinator) RequestLeaveFromISR(req RpcReqLeaveFromISR, ret *RpcRspBase) error {
	err := self.handleLeaveFromISR(req.TopicName, req.TopicPartition, nil, req.NodeID)
	ret.ErrInfo = err
	ret.RetCode = GetRpcErrCode(err)
	return err
}

func (self *NSQLookupdCoordinator) RequestLeaveFromISRByLeader(req RpcReqLeaveFromISRByLeader, ret *RpcRspBase) error {
	err := self.handleLeaveFromISR(req.TopicName, req.TopicPartition, &req.LeaderSession, req.NodeID)
	ret.ErrInfo = err
	ret.RetCode = GetRpcErrCode(err)
	return err
}
