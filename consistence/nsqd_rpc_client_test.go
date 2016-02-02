package consistence

import (
	"github.com/absolute8511/nsq/internal/test"
	"testing"
	"time"
)

type fakeNsqdLeadership struct {
	clusterID string
	regData   map[string]*NsqdNodeInfo
}

func NewFakeNSQDLeadership() NSQDLeadership {
	return &fakeNsqdLeadership{
		regData: make(map[string]*NsqdNodeInfo),
	}
}

func (self *fakeNsqdLeadership) InitClusterID(id string) {
	self.clusterID = id
}

func (self *fakeNsqdLeadership) Register(nodeData NsqdNodeInfo) error {
	self.regData[nodeData.GetID()] = &nodeData
	return nil
}

func (self *fakeNsqdLeadership) Unregister(nodeData NsqdNodeInfo) error {
	delete(self.regData, nodeData.GetID())
	return nil
}

func (self *fakeNsqdLeadership) AcquireTopicLeader(topic string, partition int, nodeData NsqdNodeInfo) error {
	return nil
}

func (self *fakeNsqdLeadership) ReleaseTopicLeader(topic string, partition int) error {
	return nil
}

func (self *fakeNsqdLeadership) WatchLookupdLeader(key string, leader chan *NsqLookupdNodeInfo, stop chan struct{}) error {
	return nil
}

func (self *fakeNsqdLeadership) GetTopicInfo(topic string, partition int) (*TopicPartionMetaInfo, error) {
	return nil, nil
}

func startNsqdRpcWithoutNsqd() *NsqdCoordinator {
	nsqdCoord := NewNsqdCoordinator("127.0.0.1", "0", "0", "", "./", nil)
	nsqdCoord.leadership = NewFakeNSQDLeadership()
	err := nsqdCoord.Start()
	if err != nil {
		panic(err)
	}
	time.Sleep(time.Second)
	return nsqdCoord
}

func TestNsqdRPCClient(t *testing.T) {
	coordLog.level = 2
	nsqdCoord := startNsqdRpcWithoutNsqd()
	client, err := NewNsqdRpcClient(nsqdCoord.rpcServer.rpcListener.Addr().String(), time.Second)
	test.Nil(t, err)
	var rspInt int32
	err = client.CallWithRetry("NsqdCoordinator.TestRpcCallNotExist", "req", &rspInt)
	test.NotNil(t, err)

	rsp, rpcErr := client.CallRpcTest("reqdata")
	test.Nil(t, rpcErr.CallErr)
	test.Equal(t, rsp, "reqdata")
	test.Equal(t, rpcErr.ReplyErr.ErrCode, RpcNoErr)
	test.Equal(t, rpcErr.ReplyErr.ErrMsg, "reqdata")
	test.Equal(t, rpcErr.ReplyErr.ErrType, CommonErr)
}
