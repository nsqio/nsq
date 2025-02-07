package quantile

import (
    "testing"
)


// Test generated using Keploy
func TestUnmarshalJSON_ValidInput(t *testing.T) {
    jsonData := `{
        "count": 10,
        "percentiles": [
            {"value": 1.0, "quantile": 0.5},
            {"value": 2.0, "quantile": 0.9}
        ],
        "topic": "test_topic",
        "channel": "test_channel",
        "host": "test_host"
    }`

    var aggregate E2eProcessingLatencyAggregate
    err := aggregate.UnmarshalJSON([]byte(jsonData))
    if err != nil {
        t.Fatalf("Expected no error, got %v", err)
    }

    if aggregate.Count != 10 {
        t.Errorf("Expected Count to be 10, got %d", aggregate.Count)
    }
    if len(aggregate.Percentiles) != 2 {
        t.Errorf("Expected 2 percentiles, got %d", len(aggregate.Percentiles))
    }
    if aggregate.Topic != "test_topic" {
        t.Errorf("Expected Topic to be 'test_topic', got %s", aggregate.Topic)
    }
    if aggregate.Channel != "test_channel" {
        t.Errorf("Expected Channel to be 'test_channel', got %s", aggregate.Channel)
    }
    if aggregate.Addr != "test_host" {
        t.Errorf("Expected Addr to be 'test_host', got %s", aggregate.Addr)
    }
}

// Test generated using Keploy
func TestUnmarshalJSON_InvalidInput(t *testing.T) {
    invalidJSON := `{"count": "invalid_count"}`

    var aggregate E2eProcessingLatencyAggregate
    err := aggregate.UnmarshalJSON([]byte(invalidJSON))
    if err == nil {
        t.Fatalf("Expected an error, got nil")
    }
}


// Test generated using Keploy
func TestLen_CorrectCount(t *testing.T) {
    aggregate := &E2eProcessingLatencyAggregate{
        Percentiles: []map[string]float64{
            {"quantile": 0.5},
            {"quantile": 0.9},
        },
    }

    if aggregate.Len() != 2 {
        t.Errorf("Expected Len to be 2, got %d", aggregate.Len())
    }
}


// Test generated using Keploy
func TestLess_CorrectComparison(t *testing.T) {
    aggregate := &E2eProcessingLatencyAggregate{
        Percentiles: []map[string]float64{
            {"percentile": 0.9},
            {"percentile": 0.5},
        },
    }

    if !aggregate.Less(0, 1) {
        t.Errorf("Expected Less(0, 1) to return true, got false")
    }
    if aggregate.Less(1, 0) {
        t.Errorf("Expected Less(1, 0) to return false, got true")
    }
}


// Test generated using Keploy
func TestSwap_CorrectSwapping(t *testing.T) {
    aggregate := &E2eProcessingLatencyAggregate{
        Percentiles: []map[string]float64{
            {"percentile": 0.9},
            {"percentile": 0.5},
        },
    }

    aggregate.Swap(0, 1)

    if aggregate.Percentiles[0]["percentile"] != 0.5 {
        t.Errorf("Expected first element to be 0.5, got %f", aggregate.Percentiles[0]["percentile"])
    }
    if aggregate.Percentiles[1]["percentile"] != 0.9 {
        t.Errorf("Expected second element to be 0.9, got %f", aggregate.Percentiles[1]["percentile"])
    }
}


// Test generated using Keploy
func TestAdd_WithEmptyPercentiles(t *testing.T) {
    aggregate1 := &E2eProcessingLatencyAggregate{
        Count: 5,
        Percentiles: []map[string]float64{
            {"quantile": 0.5, "max": 2.0, "min": 1.0, "average": 1.5, "count": 5},
        },
        Topic:   "test_topic",
        Channel: "test_channel",
        Addr:    "test_host",
    }
    aggregate2 := &E2eProcessingLatencyAggregate{
        Count:       10,
        Percentiles: []map[string]float64{},
    }

    aggregate1.Add(aggregate2)

    if aggregate1.Count != 15 {
        t.Errorf("Expected Count to be 15, got %d", aggregate1.Count)
    }
    if len(aggregate1.Percentiles) != 1 {
        t.Errorf("Expected 1 percentile, got %d", len(aggregate1.Percentiles))
    }

    // Verify that Percentiles remain unchanged
    p0 := aggregate1.Percentiles[0]
    if p0["count"] != 5 {
        t.Errorf("Expected count 5 for the percentile, got %f", p0["count"])
    }
}


// Test generated using Keploy
func TestAdd_NonOverlappingQuantiles(t *testing.T) {
    aggregate1 := &E2eProcessingLatencyAggregate{
        Count: 5,
        Percentiles: []map[string]float64{
            {"quantile": 0.5, "max": 2.0, "min": 1.0, "average": 1.5, "count": 5},
        },
        Topic:   "test_topic",
        Channel: "test_channel",
        Addr:    "test_host",
    }
    aggregate2 := &E2eProcessingLatencyAggregate{
        Count: 5,
        Percentiles: []map[string]float64{
            {"quantile": 0.9, "max": 4.0, "min": 3.0, "average": 3.5, "count": 5},
        },
    }

    aggregate1.Add(aggregate2)

    if aggregate1.Count != 10 {
        t.Errorf("Expected Count to be 10, got %d", aggregate1.Count)
    }
    if len(aggregate1.Percentiles) != 2 {
        t.Errorf("Expected 2 percentiles, got %d", len(aggregate1.Percentiles))
    }

    // Check first percentile
    p0 := aggregate1.Percentiles[0]
    if p0["quantile"] != 0.5 {
        t.Errorf("Expected first quantile to be 0.5, got %f", p0["quantile"])
    }
    if p0["count"] != 5 {
        t.Errorf("Expected count 5 for quantile 0.5, got %f", p0["count"])
    }

    // Check second percentile
    p1 := aggregate1.Percentiles[1]
    if p1["quantile"] != 0.9 {
        t.Errorf("Expected second quantile to be 0.9, got %f", p1["quantile"])
    }
    if p1["count"] != 5 {
        t.Errorf("Expected count 5 for quantile 0.9, got %f", p1["count"])
    }
}

