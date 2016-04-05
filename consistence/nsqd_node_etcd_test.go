package consistence

import (
	"fmt"
	"testing"
	"time"
)

const (
	EtcdHost = "http://192.168.66.205:2379,http://192.168.66.237:2379"
)

func TestNodeRe(t *testing.T) {
	nodeMgr := NewNsqdEtcdMgr(NewEtcdClient(EtcdHost))
	nodeMgr.InitClusterID("cluster-1")
	ID := "1"
	nodeInfo := &NsqdNodeInfo{
		ID:      ID,
		NodeIp:  "127.0.0.1",
		TcpPort: "2222",
		RpcPort: "2223",
	}
	err := nodeMgr.Register(nodeInfo)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	time.Sleep(30 * time.Second)
	err = nodeMgr.Unregister(nodeInfo)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
}
