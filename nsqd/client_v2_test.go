package nsqd

import (
    "strings"
    "testing"
    "time"
)


// Test generated using Keploy
func TestClientV2Stats_String_Consumer(t *testing.T) {
    currentTime := time.Now().Add(-5 * time.Minute)
    stats := ClientV2Stats{
        ClientID:      "test-client",
        Hostname:      "test-host",
        Version:       "1.0",
        RemoteAddress: "127.0.0.1:4150",
        State:         stateConnected,
        ReadyCount:    10,
        InFlightCount: 2,
        FinishCount:   100,
        RequeueCount:  5,
        MessageCount:  150,
        ConnectTime:   currentTime.Unix(),
    }

    result := stats.String()
    if !strings.Contains(result, "state: 2") {
        t.Errorf("Expected result to contain 'state: 2', got %s", result)
    }
}
