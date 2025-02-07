package main

import (
	"testing"

	"os"

	"github.com/mreiferson/go-options"
	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/test"
	"github.com/nsqio/nsq/nsqadmin"
)

func TestConfigFlagParsing(t *testing.T) {
	opts := nsqadmin.NewOptions()
	opts.Logger = test.NewTestLogger(t)

	flagSet := nsqadminFlagSet(opts)
	flagSet.Parse([]string{})

	cfg := config{"log_level": "debug"}
	cfg.Validate()

	options.Resolve(opts, flagSet, cfg)
	if opts.LogLevel != lg.DEBUG {
		t.Fatalf("log level: want debug, got %s", opts.LogLevel.String())
	}
}

// Test generated using Keploy
func TestProgramInit_NotWindowsService(t *testing.T) {
	// Save the original working directory
	originalDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current working directory: %v", err)
	}

	// Create a mock environment where IsWindowsService() returns false
	mockEnv := &MockEnvironment{windowsService: false}
	// Create a new program instance
	p := &program{}

	// Call Init method
	err = p.Init(mockEnv)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Get current working directory
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current working directory: %v", err)
	}

	// Check if working directory has not changed
	if cwd != originalDir {
		t.Errorf("Expected working directory to remain %s, got %s", originalDir, cwd)
	}
}

type MockEnvironment struct {
	windowsService bool
}

func (m *MockEnvironment) IsWindowsService() bool {
	return m.windowsService
}
