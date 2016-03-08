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

type RpcReadyForISR struct {
	RpcLookupReqBase
	ReadyISR      []string
	LeaderSession TopicLeaderSession
}

type RpcRspJoinISR struct {
	CoordErr
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

func (self *NSQLookupdCoordinator) RequestJoinCatchup(req RpcReqJoinCatchup, ret *CoordErr) error {
	err := self.handleRequestJoinCatchup(req.TopicName, req.TopicPartition, req.NodeID)
	ret.ErrMsg = err.Error()
	return err
}

func (self *NSQLookupdCoordinator) RequestJoinTopicISR(req RpcReqJoinISR, ret *RpcRspJoinISR) error {
	session, err := self.handleRequestJoinISR(req.TopicName, req.TopicPartition, req.NodeID)
	ret.ErrMsg = err.Error()
	ret.ReqSession = session
	return err
}

func (self *NSQLookupdCoordinator) ReadyForTopicISR(req RpcReadyForISR, ret *CoordErr) error {
	err := self.handleReadyForISR(req.TopicName, req.TopicPartition, req.NodeID, req.LeaderSession, req.ReadyISR)
	ret.ErrMsg = err.Error()
	return err
}

func (self *NSQLookupdCoordinator) PrepareLeaveFromISR(req RpcPrepareLeaveFromISR, ret *CoordErr) error {
	err := self.handlePrepareLeaveFromISR(req.TopicName, req.TopicPartition, req.NodeID)
	ret.ErrMsg = err.Error()
	return err
}

func (self *NSQLookupdCoordinator) RequestLeaveFromISR(req RpcReqLeaveFromISR, ret *CoordErr) error {
	err := self.handleLeaveFromISR(req.TopicName, req.TopicPartition, nil, req.NodeID)
	ret.ErrMsg = err.Error()
	return err
}

func (self *NSQLookupdCoordinator) RequestLeaveFromISRByLeader(req RpcReqLeaveFromISRByLeader, ret *CoordErr) error {
	err := self.handleLeaveFromISR(req.TopicName, req.TopicPartition, &req.LeaderSession, req.NodeID)
	ret.ErrMsg = err.Error()
	return err
}
