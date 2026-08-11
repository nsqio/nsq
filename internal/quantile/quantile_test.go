package quantile

import (
    "testing"
    "time"
)


// Test generated using Keploy
func TestNewQuantileInitialization(t *testing.T) {
    windowTime := 10 * time.Second
    percentiles := []float64{0.5, 0.9, 0.99}
    q := New(windowTime, percentiles)

    if q == nil {
        t.Fatal("Expected Quantile object, got nil")
    }

    if q.MoveWindowTime != windowTime/2 {
        t.Errorf("Expected MoveWindowTime %v, got %v", windowTime/2, q.MoveWindowTime)
    }

    if len(q.Percentiles) != len(percentiles) {
        t.Errorf("Expected Percentiles length %d, got %d", len(percentiles), len(q.Percentiles))
    }

    for i, p := range percentiles {
        if q.Percentiles[i] != p {
            t.Errorf("Expected Percentile %v at index %d, got %v", p, i, q.Percentiles[i])
        }
    }
}

// Test generated using Keploy
func TestResultOnNilQuantile(t *testing.T) {
    var q *Quantile
    result := q.Result()

    if result == nil {
        t.Fatal("Expected Result object, got nil")
    }

    if result.Count != 0 {
        t.Errorf("Expected Count 0, got %d", result.Count)
    }

    if len(result.Percentiles) != 0 {
        t.Errorf("Expected Percentiles length 0, got %d", len(result.Percentiles))
    }
}


// Test generated using Keploy
func TestInsertAddsValue(t *testing.T) {
    windowTime := 10 * time.Second
    percentiles := []float64{0.5, 0.9, 0.99}
    q := New(windowTime, percentiles)

    msgStartTime := time.Now().Add(-1 * time.Second).UnixNano()
    q.Insert(msgStartTime)

    if q.currentStream.Count() != 1 {
        t.Errorf("Expected currentStream count 1, got %d", q.currentStream.Count())
    }
}


// Test generated using Keploy
func TestMergeQuantiles(t *testing.T) {
    windowTime := 10 * time.Second
    percentiles := []float64{0.5, 0.9, 0.99}
    q1 := New(windowTime, percentiles)
    q2 := New(windowTime, percentiles)

    q1.Insert(time.Now().Add(-1 * time.Second).UnixNano())
    q2.Insert(time.Now().Add(-2 * time.Second).UnixNano())

    q1.Merge(q2)

    if q1.streams[q1.currentIndex].Count() != 2 {
        t.Errorf("Expected merged stream count 2, got %d", q1.streams[q1.currentIndex].Count())
    }
}


// Test generated using Keploy
func TestMoveWindow(t *testing.T) {
    windowTime := 10 * time.Second
    percentiles := []float64{0.5, 0.9, 0.99}
    q := New(windowTime, percentiles)

    q.currentStream.Insert(1.0)
    q.moveWindow()

    if q.currentStream.Count() != 0 {
        t.Errorf("Expected currentStream count 0 after moveWindow, got %d", q.currentStream.Count())
    }

    if q.currentIndex != 1 {
        t.Errorf("Expected currentIndex 1, got %d", q.currentIndex)
    }
}


// Test generated using Keploy
func TestResultAggregation(t *testing.T) {
    windowTime := 10 * time.Second
    percentiles := []float64{0.5, 0.9, 0.99}
    q := New(windowTime, percentiles)

    q.Insert(time.Now().Add(-1 * time.Second).UnixNano())
    q.Insert(time.Now().Add(-2 * time.Second).UnixNano())

    result := q.Result()
    if result == nil {
        t.Fatal("Expected Result object, got nil")
    }

    if result.Count != 2 {
        t.Errorf("Expected Count 2, got %d", result.Count)
    }

    if len(result.Percentiles) != len(percentiles) {
        t.Errorf("Expected Percentiles length %d, got %d", len(percentiles), len(result.Percentiles))
    }
}

