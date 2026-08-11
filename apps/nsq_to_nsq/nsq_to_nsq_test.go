package main

import (
    "testing"
    "github.com/nsqio/go-nsq"
    "github.com/nsqio/nsq/internal/app"
)


// Test generated using Keploy
func TestPublishHandler_ShouldPassMessage(t *testing.T) {
    requireJSONField = new(string)
    *requireJSONField = "field1"
    requireJSONValue = new(string)
    *requireJSONValue = "value1"

    ph := &PublishHandler{}
    js := map[string]interface{}{
        "field1": "value1",
    }

    pass, backoff := ph.shouldPassMessage(js)
    if !pass || backoff {
        t.Errorf("Expected pass=true and backoff=false, got pass=%v, backoff=%v", pass, backoff)
    }

    js = map[string]interface{}{
        "field1": "wrong_value",
    }
    pass, backoff = ph.shouldPassMessage(js)
    if pass || backoff {
        t.Errorf("Expected pass=false and backoff=false, got pass=%v, backoff=%v", pass, backoff)
    }
}

// Test generated using Keploy
func TestPublishHandler_HandleMessage_JSONUnmarshalError(t *testing.T) {
    requireJSONField = new(string)
    *requireJSONField = "field1"

    ph := &PublishHandler{}
    msg := &nsq.Message{
        Body: []byte(`invalid json`),
    }

    err := ph.HandleMessage(msg, "test_topic")
    if err != nil {
        t.Fatalf("Expected no error, got %v", err)
    }
}


// Test generated using Keploy
func TestFilterMessage_WhitelistFields(t *testing.T) {
    whitelistJSONFields = app.StringArray{"field1", "field2"}
    js := map[string]interface{}{
        "field1": "value1",
        "field2": 42,
        "field3": "should be removed",
    }
    rawMsg := []byte(`{"field1":"value1","field2":42,"field3":"should be removed"}`)

    filteredMsg, err := filterMessage(js, rawMsg)
    if err != nil {
        t.Fatalf("Expected no error, got %v", err)
    }

    expectedMsg := `{"field1":"value1","field2":42}`
    if string(filteredMsg) != expectedMsg {
        t.Errorf("Expected %s, got %s", expectedMsg, string(filteredMsg))
    }
}


// Test generated using Keploy
func TestFilterMessage_JSONMarshalError(t *testing.T) {
    whitelistJSONFields = app.StringArray{"field1"}
    js := map[string]interface{}{
        "field1": func() {}, // Invalid type for JSON marshalling
    }
    rawMsg := []byte(`{"field1":"invalid"}`)

    _, err := filterMessage(js, rawMsg)
    if err == nil {
        t.Fatalf("Expected an error, got nil")
    }
}


// Test generated using Keploy
func TestFilterMessage_NoWhitelistFields(t *testing.T) {
    whitelistJSONFields = app.StringArray{}
    js := map[string]interface{}{
        "field1": "value1",
        "field2": "value2",
    }
    rawMsg := []byte(`{"field1":"value1","field2":"value2"}`)

    filteredMsg, err := filterMessage(js, rawMsg)
    if err != nil {
        t.Fatalf("Expected no error, got %v", err)
    }

    if string(filteredMsg) != string(rawMsg) {
        t.Errorf("Expected message to be unchanged when no whitelist fields, got %s", string(filteredMsg))
    }
}


// Test generated using Keploy
func TestPublishHandler_HandleMessage_MissingRequiredField(t *testing.T) {
    requireJSONField = new(string)
    *requireJSONField = "field1"

    ph := &PublishHandler{}
    msg := &nsq.Message{
        Body: []byte(`{"field2": "value2"}`),
    }

    err := ph.HandleMessage(msg, "test_topic")
    if err == nil {
        t.Fatalf("Expected an error due to missing required field, got nil")
    }
}

