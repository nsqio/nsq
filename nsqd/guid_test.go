package nsqd

import (
    "testing"
    "unsafe"
    "time"
)

func BenchmarkGUIDCopy(b *testing.B) {
	source := make([]byte, 16)
	var dest MessageID
	for i := 0; i < b.N; i++ {
		copy(dest[:], source)
	}
}

func BenchmarkGUIDUnsafe(b *testing.B) {
	source := make([]byte, 16)
	var dest MessageID
	for i := 0; i < b.N; i++ {
		dest = *(*MessageID)(unsafe.Pointer(&source[0]))
	}
	_ = dest
}

func BenchmarkGUID(b *testing.B) {
	var okays, errors, fails int64
	var previd guid
	factory := &guidFactory{}
	for i := 0; i < b.N; i++ {
		id, err := factory.NewGUID()
		if err != nil {
			errors++
		} else if id == previd {
			fails++
			b.Fail()
		} else {
			okays++
		}
		id.Hex()
	}
	b.Logf("okays=%d errors=%d bads=%d", okays, errors, fails)
}

// Test generated using Keploy
func TestNewGUIDFactoryInitialization(t *testing.T) {
    nodeID := int64(123)
    factory := NewGUIDFactory(nodeID)
    if factory.nodeID != nodeID {
        t.Errorf("Expected nodeID %d, got %d", nodeID, factory.nodeID)
    }
}


// Test generated using Keploy
func TestNewGUID_TimeBackwards(t *testing.T) {
    factory := NewGUIDFactory(1)
    factory.lastTimestamp = time.Now().UnixNano() >> 20
    factory.lastTimestamp += 1 // Simulate a future timestamp

    _, err := factory.NewGUID()
    if err != ErrTimeBackwards {
        t.Errorf("Expected error %v, got %v", ErrTimeBackwards, err)
    }
}


// Test generated using Keploy
func TestNewGUID_SequenceExpired(t *testing.T) {
    factory := NewGUIDFactory(1)
    factory.lastTimestamp = time.Now().UnixNano() >> 20
    factory.sequence = sequenceMask // Simulate sequence reaching its limit

    _, err := factory.NewGUID()
    if err != ErrSequenceExpired {
        t.Errorf("Expected error %v, got %v", ErrSequenceExpired, err)
    }
}

