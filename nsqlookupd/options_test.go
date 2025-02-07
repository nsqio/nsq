package nsqlookupd

import (
	"testing"
	"time"

	"github.com/nsqio/nsq/internal/lg"
)

// Test generated using Keploy
func TestNewOptions_DefaultValues_ValidHostname(t *testing.T) {

	opts := NewOptions()

	if opts.LogPrefix != "[nsqlookupd] " {
		t.Errorf("Expected LogPrefix to be '[nsqlookupd] ', got %v", opts.LogPrefix)
	}
	if opts.LogLevel != lg.INFO {
		t.Errorf("Expected LogLevel to be lg.INFO, got %v", opts.LogLevel)
	}
	if opts.TCPAddress != "0.0.0.0:4160" {
		t.Errorf("Expected TCPAddress to be '0.0.0.0:4160', got %v", opts.TCPAddress)
	}
	if opts.HTTPAddress != "0.0.0.0:4161" {
		t.Errorf("Expected HTTPAddress to be '0.0.0.0:4161', got %v", opts.HTTPAddress)
	}
	if opts.InactiveProducerTimeout != 300*time.Second {
		t.Errorf("Expected InactiveProducerTimeout to be 300 seconds, got %v", opts.InactiveProducerTimeout)
	}
	if opts.TombstoneLifetime != 45*time.Second {
		t.Errorf("Expected TombstoneLifetime to be 45 seconds, got %v", opts.TombstoneLifetime)
	}
}
