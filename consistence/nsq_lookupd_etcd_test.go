package consistence

import (
	"fmt"
	"testing"
	"time"
)

func processLookupdLeader(leader chan *NsqLookupdNodeInfo, stop chan struct{}) {
	for {
		select {
		case l := <-leader:
			fmt.Println("lookupd new leader:", l.ID, l.NodeIp)
		case <-stop:
			return
		}
	}
}

func processTopicLeader(leader chan *TopicLeaderSession, stop chan struct{}) {
	for {
		select {
		case l := <-leader:
			fmt.Println("topic:", l.Topic, "partition:", l.Partition, "Leader Node:", l.LeaderNode)
		case <-stop:
			return
		}
	}
}

func TestLookupd(t *testing.T) {
	ID := "n-1"
	ID2 := "l-1"
	topic := "topic-1"

	nodeMgr := NewNsqdEtcdMgr(EtcdHost)
	nodeMgr.InitClusterID("cluster-1")
	nodeInfo := &NsqdNodeInfo{
		ID:      ID,
		NodeIp:  "127.0.0.1",
		TcpPort: "2222",
		RpcPort: "2223",
	}
	err := nodeMgr.RegisterNsqd(nodeInfo)
	if err != nil {
		fmt.Println("Node register error:", err.Error())
		return
	} else {
		fmt.Printf("Nsqd Node[%s] register success.\n", nodeInfo.ID)
	}

	lookupdMgr := NewNsqLookupdEtcdMgr(EtcdHost)
	lookupdMgr.InitClusterID("cluster-1")
	lookupdInfo := &NsqLookupdNodeInfo{
		ID:       ID2,
		NodeIp:   "127.0.0.1",
		HttpPort: "8090",
	}
	err = lookupdMgr.Register(lookupdInfo)
	if err != nil {
		fmt.Println("register error:", err.Error())
		return
	} else {
		fmt.Printf("Nsqd Lookupd Node[%s] register success.\n", lookupdInfo.ID)
	}

	leader := make(chan *NsqLookupdNodeInfo, 1)
	leaderStop := make(chan struct{}, 1)
	go processLookupdLeader(leader, leaderStop)
	fmt.Println("--- sleep 5 second ---")
	time.Sleep(5 * time.Second)
	lookupdMgr.AcquireAndWatchLeader(leader, leaderStop)

	fmt.Println("start to watch topic leader...")
	// watch topic
	topicLeaderCh := make(chan *TopicLeaderSession, 1)
	topicLeaderStop := make(chan struct{}, 1)
	go lookupdMgr.WatchTopicLeader(topicLeaderCh, topicLeaderStop)
	go processTopicLeader(topicLeaderCh, topicLeaderStop)
	fmt.Println("--- sleep 5 second ---")
	time.Sleep(5 * time.Second)

	err = nodeMgr.AcquireTopicLeader(topic, 0, nodeInfo, 100)
	if err != nil {
		fmt.Println("AcquireTopicLeader error:", err.Error())
		return
	} else {
		fmt.Printf("Nsqd Node[%s] acquire leader of topic[%s] success.\n", nodeInfo.ID, topic)
	}

	// create topic partition
	err = lookupdMgr.CreateTopicPartition(topic, 0)
	if err != nil {
		fmt.Println("CreateTopicPartition error:", err.Error())
		return
	} else {
		fmt.Printf("create topic[%s] partition-0 success.\n", topic)
	}

	// create topic
	metaInfo := &TopicMetaInfo{PartitionNum: 1, Replica: 1}
	err = lookupdMgr.CreateTopic(topic, metaInfo)
	if err != nil {
		fmt.Println("CreateTopic error:", err.Error())
		return
	} else {
		fmt.Printf("create topic[%s] success\n", topic)
	}

	// topic if exist
	ok, err := lookupdMgr.IsExistTopic(topic)
	if err != nil {
		fmt.Println("IsExistTopic error:", err.Error())
		return
	}
	fmt.Println("topic -", topic, "IfExist:", ok)

	// topic partition if exist
	ok, err = lookupdMgr.IsExistTopicPartition(topic, 0)
	if err != nil {
		fmt.Println("IsExistTopicPartition error:", err.Error())
		return
	}
	fmt.Println("topic -", topic, "partition-0 IfExist:", ok)

	// create topic node info
	topicNodeInfo := &TopicPartitionReplicaInfo{}
	topicNodeInfo.Leader = "127.0.0.1"
	topicNodeInfo.ISR = []string{"127.0.0.1"}

	// update
	err = lookupdMgr.UpdateTopicNodeInfo(topic, 0, topicNodeInfo, 0)
	if err != nil {
		fmt.Println("UpdateTopicNodeInfo error:", err.Error())
		return
	}

	// get
	// topic leader session
	topicLeader, err := lookupdMgr.GetTopicLeaderSession(topic, 0)
	if err != nil {
		fmt.Println("GetTopicLeaderSession error:", err.Error())
		return
	}
	fmt.Println("Topic Leader:", topicLeader)
	// topic info
	topicInfo, err := lookupdMgr.GetTopicInfo(topic, 0)
	if err != nil {
		fmt.Println("GetTopicInfo error:", err.Error())
		return
	}
	fmt.Println("Topic Info:", topicInfo)

	// time sleep
	fmt.Println("--- sleep 10 second ---")
	time.Sleep(10 * time.Second)

	// delete topic
	err = lookupdMgr.DeleteTopic(topic, 0)
	if err != nil {
		fmt.Println("DeleteTopic error:", err.Error())
		return
	} else {
		fmt.Println("delete topic success.")
	}

	lookupdMgr.Stop()
	err = lookupdMgr.Unregister(lookupdInfo)
	if err != nil {
		fmt.Println("unregister error:", err.Error())
		return
	} else {
		fmt.Println("unregister lookup mgr success.")
	}

	err = nodeMgr.ReleaseTopicLeader(topic, 0, nil)
	if err != nil {
		fmt.Println("ReleaseTopicLeader error:", err.Error())
		return
	} else {
		fmt.Println("release topic leader success.")
	}

	err = nodeMgr.UnregisterNsqd(nodeInfo)
	if err != nil {
		fmt.Println("Node unregister error:", err.Error())
		return
	} else {
		fmt.Println("nsqd node unregister success.")
	}
}
