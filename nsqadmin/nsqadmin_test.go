package nsqadmin

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"testing"

	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/test"
	"github.com/nsqio/nsq/nsqd"
)

func TestNeitherNSQDAndNSQLookup(t *testing.T) {
	opts := NewOptions()
	opts.Logger = lg.NilLogger{}
	opts.HTTPAddress = "127.0.0.1:0"
	_, err := New(opts)
	test.NotNil(t, err)
	test.Equal(t, "--nsqd-http-address or --lookupd-http-address required", fmt.Sprintf("%s", err))
}

func TestBothNSQDAndNSQLookup(t *testing.T) {
	opts := NewOptions()
	opts.Logger = lg.NilLogger{}
	opts.HTTPAddress = "127.0.0.1:0"
	opts.NSQLookupdHTTPAddresses = []string{"127.0.0.1:4161"}
	opts.NSQDHTTPAddresses = []string{"127.0.0.1:4151"}
	_, err := New(opts)
	test.NotNil(t, err)
	test.Equal(t, "use --nsqd-http-address or --lookupd-http-address not both", fmt.Sprintf("%s", err))
}

func TestTLSHTTPClient(t *testing.T) {
	lgr := test.NewTestLogger(t)

	nsqdOpts := nsqd.NewOptions()
	nsqdOpts.TLSCert = "./test/server.pem"
	nsqdOpts.TLSKey = "./test/server.key"
	nsqdOpts.TLSRootCAFile = "./test/ca.pem"
	nsqdOpts.TLSClientAuthPolicy = "require-verify"
	nsqdOpts.Logger = lgr
	_, nsqdHTTPAddr, nsqd := mustStartNSQD(nsqdOpts)
	defer os.RemoveAll(nsqdOpts.DataPath)
	defer nsqd.Exit()

	opts := NewOptions()
	opts.HTTPAddress = "127.0.0.1:0"
	opts.NSQDHTTPAddresses = []string{nsqdHTTPAddr.String()}
	opts.HTTPClientTLSRootCAFile = "./test/ca.pem"
	opts.HTTPClientTLSCert = "./test/client.pem"
	opts.HTTPClientTLSKey = "./test/client.key"
	opts.Logger = lgr
	nsqadmin, err := New(opts)
	test.Nil(t, err)
	go func() {
		err := nsqadmin.Main()
		if err != nil {
			panic(err)
		}
	}()
	defer nsqadmin.Exit()

	httpAddr := nsqadmin.RealHTTPAddr()
	u := url.URL{
		Scheme: "http",
		Host:   httpAddr.String(),
		Path:   "/api/nodes/" + nsqdHTTPAddr.String(),
	}

	resp, err := http.Get(u.String())
	test.Nil(t, err)
	defer resp.Body.Close()

	test.Equal(t, resp.StatusCode < 500, true)
}

func mustStartNSQD(opts *nsqd.Options) (*net.TCPAddr, *net.TCPAddr, *nsqd.NSQD) {
	opts.TCPAddress = "127.0.0.1:0"
	opts.HTTPAddress = "127.0.0.1:0"
	opts.HTTPSAddress = "127.0.0.1:0"
	if opts.DataPath == "" {
		tmpDir, err := ioutil.TempDir("", "nsq-test-")
		if err != nil {
			panic(err)
		}
		opts.DataPath = tmpDir
	}
	nsqd, err := nsqd.New(opts)
	if err != nil {
		panic(err)
	}
	go func() {
		err := nsqd.Main()
		if err != nil {
			panic(err)
		}
	}()
	return nsqd.RealTCPAddr(), nsqd.RealHTTPAddr(), nsqd
}
