package consistence

import (
	"fmt"
	"testing"
	"time"

	"golang.org/x/net/context"
)

const (
	EtcdHost = "http://192.168.66.205:2379,http://192.168.66.237:2379"
)

func TestNodeRe(t *testing.T) {
	nodeMgr := NewNsqdEtcdMgr(EtcdHost)
	nodeMgr.InitClusterID("cluster-1")
	ID := "1"
	nodeInfo := &NsqdNodeInfo{
		ID:      ID,
		NodeIp:  "127.0.0.1",
		TcpPort: "2222",
		RpcPort: "2223",
	}
	err := nodeMgr.RegisterNsqd(nodeInfo)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	time.Sleep(30 * time.Second)
	err = nodeMgr.UnregisterNsqd(nodeInfo)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
}

func TestETCDWatch(t *testing.T) {
	client := NewEClient(EtcdHost)
	watcher := client.Watch("q11", 0, true)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Second)
		cancel()
	}()

	for {
		rsp, err := watcher.Next(ctx)
		if err != nil {
			if err == context.Canceled {
				fmt.Println("watch canceled")
				return
			} else {
				time.Sleep(5 * time.Second)
			}
			continue
		}
		fmt.Println(rsp.Action, rsp.Node.Key, rsp.Node.Value)
	}
}
