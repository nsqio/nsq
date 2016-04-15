//        file: consistence/utils.go
// description: utils function of nsq etcd mgr

//      author: reezhou
//       email: reechou@gmail.com
//   copyright: youzan

package consistence

import (
	"strings"
	"net/url"

	"github.com/coreos/go-etcd/etcd"
)

func NewEtcdClient(etcdHost string) *etcd.Client {
	machines := strings.Split(etcdHost, ",")
	initEtcdPeers(machines)
	if len(machines) == 1 && machines[0] == "" {
		machines[0] = "http://127.0.0.1:4001"
	}
	return etcd.NewClient(machines)
}

func initEtcdPeers(machines []string) error {
	for i, ep := range machines {
		u, err := url.Parse(ep)
		if err != nil {
			return err
		}
		if u.Scheme == "" {
			u.Scheme = "http"
		}
		machines[i] = u.String()
	}
	return nil
}

func CheckKeyIfExist(err error) bool {
	etcdErr, ok := err.(*etcd.EtcdError)
	return ok && etcdErr != nil && etcdErr.ErrorCode == 100
}
