package nsqd

import (
    "testing"
)



// Test generated using Keploy
func TestPercentile_EmptyArray(t *testing.T) {
    data := []uint64{}
    result := percentile(50, data, len(data))
    expected := uint64(0)
    if result != expected {
        t.Errorf("Expected %v, got %v", expected, result)
    }
}
