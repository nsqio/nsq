package consistence

import (
	"fmt"
	"testing"
	"time"

	"github.com/absolute8511/nsq/internal/test"
	"golang.org/x/net/context"
	etcdlock "github.com/reechou/xlock2"
)

const (
	EtcdHost = "http://etcd-dev.s.qima-inc.com:2379"
)

func TestNodeRe(t *testing.T) {
	nodeMgr := NewNsqdEtcdMgr(EtcdHost)
	nodeMgr.InitClusterID("cluster-1")
	ID := "ree-test-1"
	nodeInfo := &NsqdNodeInfo{
		ID:      ID,
		NodeIP:  "127.0.0.1",
		TcpPort: "2222",
		RpcPort: "2223",
	}
	err := nodeMgr.RegisterNsqd(nodeInfo)
	test.Nil(t, err)
	time.Sleep(10 * time.Second)
	err = nodeMgr.UnregisterNsqd(nodeInfo)
	test.Nil(t, err)
}

func TestETCDWatch(t *testing.T) {
	client := etcdlock.NewEClient(EtcdHost)
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
