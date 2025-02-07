package main

import (
	"crypto/tls"
	"testing"

	"github.com/nsqio/nsq/nsqd"
)

// Test generated using Keploy
func TestTLSMinVersionOption_Set_ValidInput(t *testing.T) {
	var tmo tlsMinVersionOption
	err := tmo.Set("tls1.2")
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if uint16(tmo) != tls.VersionTLS12 {
		t.Errorf("Expected %v, got %v", tls.VersionTLS12, uint16(tmo))
	}
}

// Test generated using Keploy
func TestTLSMinVersionOption_Set_InvalidInput(t *testing.T) {
	var tmo tlsMinVersionOption
	err := tmo.Set("invalid")
	if err == nil {
		t.Errorf("Expected an error, got nil")
	}
}

// Test generated using Keploy
func TestTLSMinVersionOption_String_UnknownVersion(t *testing.T) {
	var tmo tlsMinVersionOption = tlsMinVersionOption(9999)
	result := tmo.String()
	if result != "9999" {
		t.Errorf("Expected '9999', got %v", result)
	}
}

// Test generated using Keploy
func TestTLSRequiredOption_Set_TCPHTTPS(t *testing.T) {
	var tro tlsRequiredOption
	err := tro.Set("tcp-https")
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if int(tro) != int(nsqd.TLSRequiredExceptHTTP) {
		t.Errorf("Expected %v, got %v", nsqd.TLSRequiredExceptHTTP, tro)
	}
}

// Test generated using Keploy
func TestTLSRequiredOption_Set_True(t *testing.T) {
	var tro tlsRequiredOption
	err := tro.Set("true")
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if int(tro) != int(nsqd.TLSRequired) {
		t.Errorf("Expected %v, got %v", nsqd.TLSRequired, tro)
	}
}
