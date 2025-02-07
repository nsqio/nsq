package statsd

import (
    "bytes"
    "testing"
)


// Test generated using Keploy
func TestClient_Incr_ValidInput(t *testing.T) {
    var buf bytes.Buffer
    client := NewClient(&buf, "test.prefix.")
    
    err := client.Incr("test_stat", 1)
    if err != nil {
        t.Fatalf("Expected no error, got %v", err)
    }

    expected := "test.prefix.test_stat:1|c\n"
    if buf.String() != expected {
        t.Errorf("Expected %q, got %q", expected, buf.String())
    }
}

// Test generated using Keploy
func TestClient_Decr_ValidInput(t *testing.T) {
    var buf bytes.Buffer
    client := NewClient(&buf, "test.prefix.")
    
    err := client.Decr("test_stat", 1)
    if err != nil {
        t.Fatalf("Expected no error, got %v", err)
    }

    expected := "test.prefix.test_stat:-1|c\n"
    if buf.String() != expected {
        t.Errorf("Expected %q, got %q", expected, buf.String())
    }
}


// Test generated using Keploy
func TestClient_Timing_ValidInput(t *testing.T) {
    var buf bytes.Buffer
    client := NewClient(&buf, "test.prefix.")
    
    err := client.Timing("test_stat", 123)
    if err != nil {
        t.Fatalf("Expected no error, got %v", err)
    }

    expected := "test.prefix.test_stat:123|ms\n"
    if buf.String() != expected {
        t.Errorf("Expected %q, got %q", expected, buf.String())
    }
}


// Test generated using Keploy
func TestClient_Gauge_ValidInput(t *testing.T) {
    var buf bytes.Buffer
    client := NewClient(&buf, "test.prefix.")
    
    err := client.Gauge("test_stat", 456)
    if err != nil {
        t.Fatalf("Expected no error, got %v", err)
    }

    expected := "test.prefix.test_stat:456|g\n"
    if buf.String() != expected {
        t.Errorf("Expected %q, got %q", expected, buf.String())
    }
}

