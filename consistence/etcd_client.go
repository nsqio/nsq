package consistence

import (
	"strings"
	"time"

	"github.com/coreos/etcd/client"
	"golang.org/x/net/context"
)

type EtcdClient struct {
	client client.Client
	kapi   client.KeysAPI
}

func NewEClient(host string) *EtcdClient {
	machines := strings.Split(host, ",")
	initEtcdPeers(machines)
	cfg := client.Config{
		Endpoints:               machines,
		Transport:               client.DefaultTransport,
		HeaderTimeoutPerRequest: time.Second,
	}

	c, err := client.New(cfg)
	if err != nil {
		return nil
	}

	return &EtcdClient{
		client: c,
		kapi:   client.NewKeysAPI(c),
	}
}

func (self *EtcdClient) Get(key string, sort, recursive bool) (*client.Response, error) {
	getOptions := &client.GetOptions{
		Recursive: recursive,
		Sort:      sort,
	}
	return self.kapi.Get(context.Background(), key, getOptions)
}

func (self *EtcdClient) Create(key string, value string, ttl uint64) (*client.Response, error) {
	return self.kapi.Create(context.Background(), key, value)
}

func (self *EtcdClient) Delete(key string, recursive bool) (*client.Response, error) {
	delOptions := &client.DeleteOptions{
		Recursive: recursive,
	}
	return self.kapi.Delete(context.Background(), key, delOptions)
}

func (self *EtcdClient) CreateDir(key string, ttl uint64) (*client.Response, error) {
	setOptions := &client.SetOptions{
		TTL: time.Duration(ttl) * time.Second,
		Dir: true,
	}
	return self.kapi.Set(context.Background(), key, "", setOptions)
}

func (self *EtcdClient) Set(key string, value string, ttl uint64) (*client.Response, error) {
	setOptions := &client.SetOptions{
		TTL: time.Duration(ttl) * time.Second,
	}
	return self.kapi.Set(context.Background(), key, value, setOptions)
}

func (self *EtcdClient) SetWithTTL(key string, ttl uint64) (*client.Response, error) {
	setOptions := &client.SetOptions{
		TTL:       time.Duration(ttl) * time.Second,
		Refresh:   true,
		PrevExist: client.PrevExist,
	}
	return self.kapi.Set(context.Background(), key, "", setOptions)
}

func (self *EtcdClient) CompareAndSwap(key string, value string, ttl uint64, prevValue string, prevIndex uint64) (*client.Response, error) {
	refresh := false
	if ttl > 0 {
		refresh = true
	}
	setOptions := &client.SetOptions{
		PrevValue: prevValue,
		PrevIndex: prevIndex,
		TTL:       time.Duration(ttl) * time.Second,
		Refresh:   refresh,
	}
	return self.kapi.Set(context.Background(), key, value, setOptions)
}

func (self *EtcdClient) Watch(key string, waitIndex uint64, recursive bool) client.Watcher {
	watchOptions := &client.WatcherOptions{
		AfterIndex: waitIndex,
		Recursive:  recursive,
	}
	return self.kapi.Watcher(key, watchOptions)
}
