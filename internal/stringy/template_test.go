package stringy

import (
    "testing"
)


// Test generated using Keploy
func TestNanoSecondToHuman_SecondsConversion(t *testing.T) {
    input := 1500000000.0
    expected := "1.5s"
    result := NanoSecondToHuman(input)
    if result != expected {
        t.Errorf("Expected %v, got %v", expected, result)
    }
}

// Test generated using Keploy
func TestNanoSecondToHuman_MillisecondsConversion(t *testing.T) {
    input := 1500000.0
    expected := "1.5ms"
    result := NanoSecondToHuman(input)
    if result != expected {
        t.Errorf("Expected %v, got %v", expected, result)
    }
}


// Test generated using Keploy
func TestNanoSecondToHuman_MicrosecondsConversion(t *testing.T) {
    input := 1500.0
    expected := "1.5us"
    result := NanoSecondToHuman(input)
    if result != expected {
        t.Errorf("Expected %v, got %v", expected, result)
    }
}


// Test generated using Keploy
func TestNanoSecondToHuman_NanosecondsConversion(t *testing.T) {
    input := 500.0
    expected := "500.0ns"
    result := NanoSecondToHuman(input)
    if result != expected {
        t.Errorf("Expected %v, got %v", expected, result)
    }
}


