package clusterinfo

import (
    "testing"
)


// Test generated using Keploy
func TestClientStats_HasUserAgent_ReturnsTrue(t *testing.T) {
    cs := &ClientStats{
        UserAgent: "test-agent",
    }

    if !cs.HasUserAgent() {
        t.Errorf("Expected HasUserAgent to return true when UserAgent is not empty")
    }
}

// Test generated using Keploy
func TestClientStats_HasSampleRate_ReturnsTrue(t *testing.T) {
    cs := &ClientStats{
        SampleRate: 50,
    }

    if !cs.HasSampleRate() {
        t.Errorf("Expected HasSampleRate to return true when SampleRate > 0")
    }
}


// Test generated using Keploy
func TestProducers_HTTPAddrs_ReturnsCorrectAddresses(t *testing.T) {
    producers := Producers{
        &Producer{
            BroadcastAddress: "127.0.0.1",
            HTTPPort:         4151,
        },
        &Producer{
            BroadcastAddress: "192.168.1.1",
            HTTPPort:         4151,
        },
    }

    addrs := producers.HTTPAddrs()
    expectedAddrs := []string{"127.0.0.1:4151", "192.168.1.1:4151"}

    for i, addr := range addrs {
        if addr != expectedAddrs[i] {
            t.Errorf("Expected address %s, got %s", expectedAddrs[i], addr)
        }
    }
}


// Test generated using Keploy
func TestProducers_Search_FindsProducer(t *testing.T) {
    producer := &Producer{
        BroadcastAddress: "127.0.0.1",
        HTTPPort:         4151,
    }
    producers := Producers{producer}

    found := producers.Search("127.0.0.1:4151")
    if found == nil {
        t.Fatalf("Expected to find producer, got nil")
    }
    if found != producer {
        t.Errorf("Expected to find the correct producer, got a different one")
    }
}

