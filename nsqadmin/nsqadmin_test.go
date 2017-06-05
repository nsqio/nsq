package nsqadmin

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"testing"

	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/test"
	"github.com/nsqio/nsq/nsqd"
)

func TestNeitherNSQDAndNSQLookup(t *testing.T) {
	if os.Getenv("BE_CRASHER") == "1" {
		opts := NewOptions()
		opts.Logger = lg.NilLogger{}
		opts.HTTPAddress = "127.0.0.1:0"
		New(opts)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestNeitherNSQDAndNSQLookup")
	cmd.Env = append(os.Environ(), "BE_CRASHER=1")
	err := cmd.Run()
	test.Equal(t, "exit status 1", fmt.Sprintf("%v", err))
	if e, ok := err.(*exec.ExitError); ok && !e.Success() {
		return
	}
	t.Fatalf("process ran with err %v, want exit status 1", err)
}

func TestBothNSQDAndNSQLookup(t *testing.T) {
	if os.Getenv("BE_CRASHER") == "1" {
		opts := NewOptions()
		opts.Logger = lg.NilLogger{}
		opts.HTTPAddress = "127.0.0.1:0"
		opts.NSQLookupdHTTPAddresses = []string{"127.0.0.1:4161"}
		opts.NSQDHTTPAddresses = []string{"127.0.0.1:4151"}
		New(opts)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestBothNSQDAndNSQLookup")
	cmd.Env = append(os.Environ(), "BE_CRASHER=1")
	err := cmd.Run()
	test.Equal(t, "exit status 1", fmt.Sprintf("%v", err))
	if e, ok := err.(*exec.ExitError); ok && !e.Success() {
		return
	}
	t.Fatalf("process ran with err %v, want exit status 1", err)
}

func TestTLSHTTPClient(t *testing.T) {
	nsqdOpts := nsqd.NewOptions()
	nsqdOpts.TLSCert = "./test/server.pem"
	nsqdOpts.TLSKey = "./test/server-key.pem"
	nsqdOpts.TLSRootCAFile = "./test/ca.pem"
	nsqdOpts.TLSClientAuthPolicy = "require-verify"
	_, nsqdHTTPAddr, nsqd := mustStartNSQD(nsqdOpts)
	defer os.RemoveAll(nsqdOpts.DataPath)
	defer nsqd.Exit()

	opts := NewOptions()
	opts.HTTPAddress = "127.0.0.1:0"
	opts.NSQDHTTPAddresses = []string{nsqdHTTPAddr.String()}
	opts.HTTPClientTLSRootCAFile = "./test/ca.pem"
	opts.HTTPClientTLSCert = "./test/client.pem"
	opts.HTTPClientTLSKey = "./test/client-key.pem"
	nsqadmin := New(opts)
	nsqadmin.Main()
	defer nsqadmin.Exit()

	httpAddr := nsqadmin.RealHTTPAddr()
	u := url.URL{
		Scheme: "http",
		Host:   httpAddr.String(),
		Path:   "/api/nodes/" + nsqdHTTPAddr.String(),
	}

	resp, err := http.Get(u.String())
	test.Equal(t, nil, err)
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
	nsqd := nsqd.New(opts)
	nsqd.Main()
	return nsqd.RealTCPAddr(), nsqd.RealHTTPAddr(), nsqd
}

func TestCrashingLogger(t *testing.T) {
	if os.Getenv("BE_CRASHER") == "1" {
		// Test invalid log level causes error
		opts := NewOptions()
		opts.LogLevel = "bad"
		opts.NSQLookupdHTTPAddresses = []string{"127.0.0.1:4161"}
		_ = New(opts)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestCrashingLogger")
	cmd.Env = append(os.Environ(), "BE_CRASHER=1")
	err := cmd.Run()
	if e, ok := err.(*exec.ExitError); ok && !e.Success() {
		return
	}
	t.Fatalf("process ran with err %v, want exit status 1", err)
}
