package consistence

import (
	"errors"
	"fmt"
	"github.com/absolute8511/nsq/internal/test"
	"github.com/absolute8511/nsq/nsqd"
	"io/ioutil"
	"os"
	"strconv"
	"testing"
	"time"
)

type fakeNsqdLeadership struct {
	clusterID            string
	regData              map[string]*NsqdNodeInfo
	fakeTopicsLeaderData map[string]map[int]*TopicCoordinator
	fakeTopicsInfo       map[string]map[int]*TopicPartionMetaInfo
}

func NewFakeNSQDLeadership() NSQDLeadership {
	return &fakeNsqdLeadership{
		regData:              make(map[string]*NsqdNodeInfo),
		fakeTopicsLeaderData: make(map[string]map[int]*TopicCoordinator),
		fakeTopicsInfo:       make(map[string]map[int]*TopicPartionMetaInfo),
	}
}

func (self *fakeNsqdLeadership) InitClusterID(id string) {
	self.clusterID = id
}

func (self *fakeNsqdLeadership) RegisterNsqd(nodeData *NsqdNodeInfo) error {
	self.regData[nodeData.GetID()] = nodeData
	return nil
}

func (self *fakeNsqdLeadership) UnregisterNsqd(nodeData *NsqdNodeInfo) error {
	delete(self.regData, nodeData.GetID())
	return nil
}

func (self *fakeNsqdLeadership) IsNodeTopicLeader(topic string, partition int, nodeData *NsqdNodeInfo) bool {
	t, ok := self.fakeTopicsLeaderData[topic]
	var tc *TopicCoordinator
	if ok {
		if tc, ok = t[partition]; ok {
			if tc.topicLeaderSession.LeaderNode != nil {
				if tc.topicLeaderSession.LeaderNode.GetID() == nodeData.GetID() {
					return true
				}
			}
		}
	}
	return false
}

func (self *fakeNsqdLeadership) GetAllLookupdNodes() ([]NsqLookupdNodeInfo, error) {
	v := make([]NsqLookupdNodeInfo, 0)
	return v, nil
}

func (self *fakeNsqdLeadership) AcquireTopicLeader(topic string, partition int, nodeData *NsqdNodeInfo, epoch EpochType) error {
	t, ok := self.fakeTopicsLeaderData[topic]
	var tc *TopicCoordinator
	if ok {
		if tc, ok = t[partition]; ok {
			if tc.topicLeaderSession.LeaderNode != nil {
				if tc.topicLeaderSession.LeaderNode.GetID() == nodeData.GetID() {
					return nil
				}
				return errors.New("topic leader already exist.")
			}
			tc.topicLeaderSession.LeaderNode = nodeData
			tc.topicLeaderSession.LeaderEpoch++
			tc.topicLeaderSession.Session = nodeData.GetID() + strconv.Itoa(int(tc.topicLeaderSession.LeaderEpoch))
			tc.topicInfo.ISR = append(tc.topicInfo.ISR, nodeData.GetID())
			tc.topicInfo.Leader = nodeData.GetID()
			tc.topicInfo.Epoch++
		} else {
			tc = &TopicCoordinator{}
			tc.topicInfo.Name = topic
			tc.topicInfo.Partition = partition
			tc.localDataLoaded = true
			tc.topicInfo.Leader = nodeData.GetID()
			tc.topicInfo.ISR = append(tc.topicInfo.ISR, nodeData.GetID())
			tc.topicInfo.Epoch++
			tc.topicLeaderSession.LeaderNode = nodeData
			tc.topicLeaderSession.LeaderEpoch++
			tc.topicLeaderSession.Session = nodeData.GetID() + strconv.Itoa(int(tc.topicLeaderSession.LeaderEpoch))
			t[partition] = tc
		}
	} else {
		tmp := make(map[int]*TopicCoordinator)
		tc = &TopicCoordinator{}
		tc.topicInfo.Name = topic
		tc.topicInfo.Partition = partition
		tc.localDataLoaded = true
		tc.topicInfo.Leader = nodeData.GetID()
		tc.topicInfo.ISR = append(tc.topicInfo.ISR, nodeData.GetID())
		tc.topicInfo.Epoch++
		tc.topicLeaderSession.LeaderNode = nodeData
		tc.topicLeaderSession.LeaderEpoch++
		tc.topicLeaderSession.Session = nodeData.GetID() + strconv.Itoa(int(tc.topicLeaderSession.LeaderEpoch))
		tmp[partition] = tc
		self.fakeTopicsLeaderData[topic] = tmp
	}
	return nil
}

func (self *fakeNsqdLeadership) ReleaseTopicLeader(topic string, partition int, session *TopicLeaderSession) error {
	t, ok := self.fakeTopicsLeaderData[topic]
	if ok {
		delete(t, partition)
	}
	return nil
}

func (self *fakeNsqdLeadership) WatchLookupdLeader(leader chan *NsqLookupdNodeInfo, stop chan struct{}) error {
	return nil
}

func (self *fakeNsqdLeadership) GetTopicInfo(topic string, partition int) (*TopicPartionMetaInfo, error) {
	t, ok := self.fakeTopicsInfo[topic]
	if ok {
		tp, ok2 := t[partition]
		if ok2 {
			return tp, nil
		}
	}
	return nil, errors.New("topic not exist")
}

func (self *fakeNsqdLeadership) GetTopicLeaderSession(topic string, partition int) (*TopicLeaderSession, error) {
	s, ok := self.fakeTopicsLeaderData[topic]
	if !ok {
		return nil, ErrMissingTopicLeaderSession
	}
	ss, ok := s[partition]
	if !ok {
		return nil, ErrMissingTopicLeaderSession
	}
	return &ss.topicLeaderSession, nil
}

func startNsqdCoord(t *testing.T, rpcport string, dataPath string, extraID string, nsqd *nsqd.NSQD, useFake bool) *NsqdCoordinator {
	nsqdCoord := NewNsqdCoordinator(TEST_NSQ_CLUSTER_NAME, "127.0.0.1", "0", rpcport, extraID, dataPath, nsqd)
	if useFake {
		nsqdCoord.leadership = NewFakeNSQDLeadership()
		nsqdCoord.lookupRemoteCreateFunc = func(addr string, to time.Duration) (INsqlookupRemoteProxy, error) {
			p, err := NewFakeLookupRemoteProxy(addr, to)
			if err == nil {
				p.(*fakeLookupRemoteProxy).t = t
			}
			return p, err
		}
	} else {
		nsqdCoord.SetLeadershipMgr(NewNsqdEtcdMgr(testEtcdServers))
		nsqdCoord.leadership.UnregisterNsqd(&nsqdCoord.myNode)
	}
	nsqdCoord.lookupLeader = NsqLookupdNodeInfo{}
	return nsqdCoord
}

func startNsqdCoordWithFakeData(t *testing.T, rpcport string, dataPath string,
	extraID string, nsqd *nsqd.NSQD, fakeLeadership *fakeNsqdLeadership, fakeLookupProxy *fakeLookupRemoteProxy) *NsqdCoordinator {
	nsqdCoord := NewNsqdCoordinator(TEST_NSQ_CLUSTER_NAME, "127.0.0.1", "0", rpcport, extraID, dataPath, nsqd)
	nsqdCoord.leadership = fakeLeadership
	nsqdCoord.lookupRemoteCreateFunc = func(addr string, to time.Duration) (INsqlookupRemoteProxy, error) {
		fakeLookupProxy.t = t
		fakeLookupProxy.fakeNsqdCoords[nsqdCoord.myNode.GetID()] = nsqdCoord
		return fakeLookupProxy, nil
	}
	nsqdCoord.lookupLeader = NsqLookupdNodeInfo{}
	err := nsqdCoord.Start()
	if err != nil {
		panic(err)
	}
	time.Sleep(time.Second)
	return nsqdCoord
}

func TestNsqdRPCClient(t *testing.T) {
	coordLog.level = 2
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)

	nsqdCoord := startNsqdCoord(t, "0", tmpDir, "", nil, true)
	nsqdCoord.Start()
	defer nsqdCoord.Stop()
	time.Sleep(time.Second * 2)
	client, err := NewNsqdRpcClient(nsqdCoord.rpcServer.rpcListener.Addr().String(), time.Second)
	test.Nil(t, err)
	var rspInt int32
	err = client.CallWithRetry("NsqdCoordinator.TestRpcCallNotExist", "req", &rspInt)
	test.NotNil(t, err)

	rsp, rpcErr := client.CallRpcTest("reqdata")
	test.NotNil(t, rpcErr)
	test.Equal(t, rsp, "reqdata")
	test.Equal(t, rpcErr.ErrCode, RpcNoErr)
	test.Equal(t, rpcErr.ErrMsg, "reqdata")
	test.Equal(t, rpcErr.ErrType, CoordCommonErr)
}
