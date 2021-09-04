//go:build go1.16 && debug
// +build go1.16,debug

package nsqadmin

import (
	"io/ioutil"
	"os"
	"path"
)

func staticAsset(name string) ([]byte, error) {
	path := path.Join("../../nsqadmin/static/build", name)
	_, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadFile(path)
}
