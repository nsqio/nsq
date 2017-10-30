package mod

import (
	"io/ioutil"
	"testing"

	"github.com/nsqio/nsq/internal/test"
	"github.com/nsqio/nsq/nsqd"
)

func makeNSQD(t *testing.T, opts *nsqd.Options) *nsqd.NSQD {
	opts.Logger = test.NewTestLogger(t)
	opts.TCPAddress = "127.0.0.1:0"
	opts.HTTPAddress = "127.0.0.1:0"
	opts.HTTPSAddress = "127.0.0.1:0"
	if opts.DataPath == "" {
		tmpDir, err := ioutil.TempDir("", "nsq-test-")
		test.Nil(t, err)
		opts.DataPath = tmpDir
	}
	nsqd := nsqd.New(opts)
	nsqd.Main()
	return nsqd
}

func TestOneMod(t *testing.T) {
	nsqdOpts := nsqd.NewOptions()
	nsqdOpts.ModOpt = []string{"dogstatsd=-address=127.0.0.1:0", "dogstatsd=-prefix=stage."}

	nsqd := makeNSQD(t, nsqdOpts)
	defer nsqd.Exit()
	err := Init(nsqdOpts.ModOpt, nsqd)
	test.Nil(t, err)
}

func TestBadModName(t *testing.T) {
	nsqdOpts := nsqd.NewOptions()
	nsqdOpts.ModOpt = []string{"nonexisting=whatever"}

	nsqd := makeNSQD(t, nsqdOpts)
	defer nsqd.Exit()
	err := Init(nsqdOpts.ModOpt, nsqd)
	test.NotNil(t, err)
}
