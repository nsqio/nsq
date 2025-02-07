package main

import (
    "testing"
)


// Test generated using Keploy
func TestConfigValidate_NoLogLevel(t *testing.T) {
    cfg := config{}
    cfg.Validate()
    if _, exists := cfg["log_level"]; exists {
        t.Errorf("Expected log_level to not exist in the config, but it does")
    }
}
