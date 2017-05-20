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

	"github.com/nsqio/nsq/internal/test"
	"github.com/nsqio/nsq/nsqd"
)

func TestNoLogger(t *testing.T) {
	opts := NewOptions()
	opts.Logger = nil
	opts.HTTPAddress = "127.0.0.1:0"
	opts.NSQLookupdHTTPAddresses = []string{"127.0.0.1:4161"}
	nsqadmin := New(opts)

	nsqadmin.logf(LOG_ERROR, "should never be logged")
}

func TestNeitherNSQDAndNSQLookup(t *testing.T) {
	if os.Getenv("BE_CRASHER") == "1" {
		opts := NewOptions()
		opts.Logger = nil
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
		opts.Logger = nil
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
		nsqdOpts := nsqd.NewOptions()
		_, _, nsqd := mustStartNSQD(nsqdOpts)
		defer os.RemoveAll(nsqdOpts.DataPath)
		defer nsqd.Exit()

		opts := NewOptions()
		opts.LogLevel = "bad"
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

type mockLogger struct {
	Count int
}

func (l *mockLogger) Output(maxdepth int, s string) error {
	l.Count++
	return nil
}

func TestLogging(t *testing.T) {
	nsqdOpts := nsqd.NewOptions()
	_, nsqdHTTPAddr, nsqd := mustStartNSQD(nsqdOpts)
	defer os.RemoveAll(nsqdOpts.DataPath)
	defer nsqd.Exit()

	logger := &mockLogger{}

	opts := NewOptions()
	opts.HTTPAddress = "127.0.0.1:0"
	opts.NSQDHTTPAddresses = []string{nsqdHTTPAddr.String()}
	opts.Logger = logger

	// Test only fatal get through
	opts.LogLevel = "FaTaL"
	nsqadmin1 := New(opts)
	logger.Count = 0
	for i := 1; i <= 5; i++ {
		nsqadmin1.logf(i, "Test")
	}
	test.Equal(t, 1, logger.Count)

	// Test only warnings or higher get through
	opts.LogLevel = "WARN"
	nsqadmin2 := New(opts)
	logger.Count = 0
	for i := 1; i <= 5; i++ {
		nsqadmin2.logf(i, "Test")
	}
	test.Equal(t, 3, logger.Count)

	// Test everything gets through
	opts.LogLevel = "debuG"
	nsqadmin3 := New(opts)
	logger.Count = 0
	for i := 1; i <= 5; i++ {
		nsqadmin3.logf(i, "Test")
	}
	test.Equal(t, 5, logger.Count)

	// Test everything gets through with verbose = true
	opts.LogLevel = "fatal"
	opts.Verbose = true
	nsqadmin4 := New(opts)
	logger.Count = 0
	for i := 1; i <= 5; i++ {
		nsqadmin4.logf(i, "Test")
	}
	test.Equal(t, 5, logger.Count)
}
