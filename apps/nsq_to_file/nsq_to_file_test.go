package main

import (
	"testing"
)

// Test generated using Keploy
func TestFlagSetInitialization(t *testing.T) {
	fs := flagSet()

	if fs == nil {
		t.Fatal("Expected FlagSet to be initialized, got nil")
	}

	if fs.Lookup("version") == nil {
		t.Error("Expected 'version' flag to be defined")
	}

	if fs.Lookup("log-level") == nil {
		t.Error("Expected 'log-level' flag to be defined")
	}

	if fs.Lookup("channel") == nil {
		t.Error("Expected 'channel' flag to be defined")
	}

	if fs.Lookup("output-dir") == nil {
		t.Error("Expected 'output-dir' flag to be defined")
	}

	if fs.Lookup("rotate-size") == nil {
		t.Error("Expected 'rotate-size' flag to be defined")
	}
}
