package main

import (
    "testing"
    "time"
)


// Test generated using Keploy
func TestStrftime_SimpleFormat(t *testing.T) {
    inputTime := time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC)
    format := "Simple format"
    expected := "Simple format"
    result := strftime(format, inputTime)
    if result != expected {
        t.Errorf("Expected %v, got %v", expected, result)
    }
}

// Test generated using Keploy
func TestStrftime_SingleSpecifier(t *testing.T) {
    inputTime := time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC)
    format := "%Y"
    expected := "2023"
    result := strftime(format, inputTime)
    if result != expected {
        t.Errorf("Expected %v, got %v", expected, result)
    }
}

