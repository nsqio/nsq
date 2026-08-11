package statsd

import (
    "testing"
)


// Test generated using Keploy
func TestHostKey_ReplacesDotsAndColons(t *testing.T) {
    input := "example.com:8080"
    expected := "example_com_8080"
    result := HostKey(input)
    if result != expected {
        t.Errorf("Expected %v, got %v", expected, result)
    }
}
