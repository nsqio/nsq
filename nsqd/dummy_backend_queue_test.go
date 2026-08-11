package nsqd

import (
    "testing"
)


// Test generated using Keploy
func TestDummyBackendQueue_Put_ReturnsNil(t *testing.T) {
    queue := newDummyBackendQueue()
    err := queue.Put([]byte("test"))
    if err != nil {
        t.Errorf("Expected nil, got %v", err)
    }
}

// Test generated using Keploy
func TestDummyBackendQueue_Close_ReturnsNil(t *testing.T) {
    queue := newDummyBackendQueue()
    err := queue.Close()
    if err != nil {
        t.Errorf("Expected nil, got %v", err)
    }
}


// Test generated using Keploy
func TestDummyBackendQueue_Depth_ReturnsZero(t *testing.T) {
    queue := newDummyBackendQueue()
    depth := queue.Depth()
    if depth != 0 {
        t.Errorf("Expected 0, got %v", depth)
    }
}

