//        file: consistence/utils.go
// description: utils function of nsq etcd mgr

//      author: reezhou
//       email: reechou@gmail.com
//   copyright: youzan

package consistence

import (
	"strings"

	"github.com/coreos/go-etcd/etcd"
)

func NewEtcdClient(etcdHost string) *etcd.Client {
	machines := strings.Split(etcdHost, ",")
	if len(machines) == 1 && machines[0] == "" {
		machines[0] = "http://127.0.0.1:4001"
	}
	return etcd.NewClient(machines)
}

func CheckKeyIfExist(err error) bool {
	etcdErr, ok := err.(*etcd.EtcdError)
	return ok && etcdErr != nil && etcdErr.ErrorCode == 100
}
