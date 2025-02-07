package app

import (
    "testing"
    "fmt"
)


// Test generated using Keploy
func TestFloatArray_Set_ValidInput(t *testing.T) {
    var fa FloatArray
    err := fa.Set("1.1,2.2,3.3")
    if err != nil {
        t.Errorf("Expected no error, got %v", err)
    }
    expected := FloatArray{3.3, 2.2, 1.1}
    if fmt.Sprintf("%v", fa) != fmt.Sprintf("%v", expected) {
        t.Errorf("Expected %v, got %v", expected, fa)
    }
}

// Test generated using Keploy
func TestFloatArray_String(t *testing.T) {
    fa := FloatArray{3.3, 2.2, 1.1}
    expected := "3.300000,2.200000,1.100000"
    result := fa.String()
    if result != expected {
        t.Errorf("Expected %v, got %v", expected, result)
    }
}

