package consistence

import (
	pb "github.com/absolute8511/nsq/consistence/coordgrpc"
	"github.com/absolute8511/nsq/internal/levellogger"
	"github.com/absolute8511/nsq/nsqd"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"net"
	"os"
	"time"
)

func (self *NsqdCoordinator) checkWriteForGRpcCall(rpcData *pb.RpcTopicData) (*TopicCoordinator, *CoordErr) {
	if rpcData == nil {
		return nil, ErrPubArgError
	}
	topicCoord, err := self.getTopicCoord(rpcData.TopicName, int(rpcData.TopicPartition))
	if err != nil || topicCoord == nil {
		coordLog.Infof("rpc call with missing topic :%v", rpcData)
		return nil, ErrMissingTopicCoord
	}
	tcData := topicCoord.GetData()
	if tcData.GetTopicEpochForWrite() != EpochType(rpcData.TopicWriteEpoch) {
		coordLog.Infof("rpc call with wrong epoch :%v, current: %v", rpcData, tcData.GetTopicEpochForWrite())
		return nil, ErrEpochMismatch
	}
	if tcData.GetLeaderSession() != rpcData.TopicLeaderSession {
		coordLog.Infof("rpc call with wrong session:%v, local: %v", rpcData, tcData.GetLeaderSession())
		return nil, ErrLeaderSessionMismatch
	}
	return topicCoord, nil
}

type nsqdCoordGRpcServer struct {
	nsqdCoord      *NsqdCoordinator
	rpcServer      *grpc.Server
	dataRootPath   string
	disableRpcTest bool
}

func NewNsqdCoordGRpcServer(coord *NsqdCoordinator, rootPath string) *nsqdCoordGRpcServer {
	return &nsqdCoordGRpcServer{
		nsqdCoord:    coord,
		rpcServer:    grpc.NewServer(),
		dataRootPath: rootPath,
	}
}

func (s *nsqdCoordGRpcServer) start(ip, port string) error {
	lis, err := net.Listen("tcp", ip+":"+port)
	if err != nil {
		coordLog.Errorf("starting grpc server error: %v", err)
		os.Exit(1)
	}
	pb.RegisterNsqdCoordRpcV2Server(s.rpcServer, s)
	go s.rpcServer.Serve(lis)
	coordLog.Infof("nsqd grpc coordinator server listen at: %v", lis.Addr())
	return nil
}

func (s *nsqdCoordGRpcServer) stop() {
	if s.rpcServer != nil {
		s.rpcServer.Stop()
	}
}

func (s *nsqdCoordGRpcServer) UpdateChannelOffset(ctx context.Context, req *pb.RpcChannelOffsetArg) (*pb.CoordErr, error) {
	var coordErr pb.CoordErr
	tc, err := s.nsqdCoord.checkWriteForGRpcCall(req.TopicData)
	if err != nil {
		coordErr.ErrMsg = err.ErrMsg
		coordErr.ErrCode = int32(err.ErrCode)
		coordErr.ErrType = int32(err.ErrType)
		return &coordErr, nil
	}
	// update local channel offset
	var chOffset ChannelConsumerOffset
	chOffset.Flush = req.ChannelOffset.Flush
	chOffset.VOffset = req.ChannelOffset.Voffset
	chOffset.AllowBackward = req.ChannelOffset.AllowBackward
	err = s.nsqdCoord.updateChannelOffsetOnSlave(tc.GetData(), req.Channel, chOffset)
	if err != nil {
		coordErr.ErrMsg = err.ErrMsg
		coordErr.ErrCode = int32(err.ErrCode)
		coordErr.ErrType = int32(err.ErrType)
	}
	return &coordErr, nil
}

func (s *nsqdCoordGRpcServer) PutMessage(ctx context.Context, req *pb.RpcPutMessage) (*pb.CoordErr, error) {
	var coordErr pb.CoordErr
	if coordLog.Level() >= levellogger.LOG_DEBUG {
		s := time.Now().Unix()
		defer func() {
			e := time.Now().Unix()
			if e-s > int64(RPC_TIMEOUT/2) {
				coordLog.Infof("PutMessage rpc call used: %v", e-s)
			}
		}()
	}

	tc, err := s.nsqdCoord.checkWriteForGRpcCall(req.TopicData)
	if err != nil {
		coordErr.ErrMsg = err.ErrMsg
		coordErr.ErrCode = int32(err.ErrCode)
		coordErr.ErrType = int32(err.ErrType)
		return &coordErr, nil
	}
	// do local pub message
	var commitData CommitLogData
	commitData.Epoch = EpochType(req.LogData.Epoch)
	commitData.LogID = req.LogData.LogID
	commitData.MsgNum = req.LogData.MsgNum
	commitData.MsgCnt = req.LogData.MsgCnt
	commitData.MsgSize = req.LogData.MsgSize
	commitData.MsgOffset = req.LogData.MsgOffset
	commitData.LastMsgLogID = req.LogData.LastMsgLogID
	var msg nsqd.Message
	msg.ID = nsqd.MessageID(req.TopicMessage.ID)
	msg.TraceID = req.TopicMessage.Trace_ID
	msg.Attempts = uint16(req.TopicMessage.Attemps)
	msg.Timestamp = req.TopicMessage.Timestamp

	msg.Body = req.TopicMessage.Body
	err = s.nsqdCoord.putMessageOnSlave(tc, commitData, &msg, false)
	if err != nil {
		coordErr.ErrMsg = err.ErrMsg
		coordErr.ErrCode = int32(err.ErrCode)
		coordErr.ErrType = int32(err.ErrType)
	}
	return &coordErr, nil
}

func (s *nsqdCoordGRpcServer) PutMessages(ctx context.Context, req *pb.RpcPutMessages) (*pb.CoordErr, error) {
	var coordErr pb.CoordErr
	if coordLog.Level() >= levellogger.LOG_DEBUG {
		s := time.Now().Unix()
		defer func() {
			e := time.Now().Unix()
			if e-s > int64(RPC_TIMEOUT/2) {
				coordLog.Infof("PutMessage rpc call used: %v", e-s)
			}
		}()
	}

	tc, err := s.nsqdCoord.checkWriteForGRpcCall(req.TopicData)
	if err != nil {
		coordErr.ErrMsg = err.ErrMsg
		coordErr.ErrCode = int32(err.ErrCode)
		coordErr.ErrType = int32(err.ErrType)
		return &coordErr, nil
	}
	// do local pub message
	var commitData CommitLogData
	commitData.Epoch = EpochType(req.LogData.Epoch)
	commitData.LogID = req.LogData.LogID
	commitData.MsgNum = req.LogData.MsgNum
	commitData.MsgCnt = req.LogData.MsgCnt
	commitData.MsgSize = req.LogData.MsgSize
	commitData.MsgOffset = req.LogData.MsgOffset
	commitData.LastMsgLogID = req.LogData.LastMsgLogID
	var msgs []*nsqd.Message
	for _, pbm := range req.TopicMessage {
		var msg nsqd.Message
		msg.ID = nsqd.MessageID(pbm.ID)
		msg.TraceID = pbm.Trace_ID
		msg.Attempts = uint16(pbm.Attemps)
		msg.Timestamp = pbm.Timestamp
		msg.Body = pbm.Body
		msgs = append(msgs, &msg)
	}

	err = s.nsqdCoord.putMessagesOnSlave(tc, commitData, msgs)
	if err != nil {
		coordErr.ErrMsg = err.ErrMsg
		coordErr.ErrCode = int32(err.ErrCode)
		coordErr.ErrType = int32(err.ErrType)
	}
	return &coordErr, nil
}
