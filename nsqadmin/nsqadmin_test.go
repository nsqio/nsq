package nsqadmin

import (
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/nsqio/nsq/internal/test"
)

func TestNoLogger(t *testing.T) {
	opts := NewOptions()
	opts.Logger = nil
	opts.HTTPAddress = "127.0.0.1:0"
	opts.NSQLookupdHTTPAddresses = []string{"127.0.0.1:4161"}
	nsqlookupd := New(opts)

	nsqlookupd.logf("should never be logged")
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
