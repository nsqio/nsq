package nsqd

import (
    "testing"
)


// Test generated using Keploy
func TestHasExperiment_ExperimentExists_ReturnsTrue(t *testing.T) {
    options := Options{
        Experiments: []string{"topology-aware-consumption"},
    }
    result := options.HasExperiment(TopologyAwareConsumption)
    if !result {
        t.Errorf("Expected true, got false")
    }
}
