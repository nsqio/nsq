package http_api

import (
    "io"
    "net/http"
    "net/url"
    "strings"
    "testing"
    "errors"
)


// Test generated using Keploy
func TestNewReqParams_ValidRequest(t *testing.T) {
    reqBody := "key1=value1&key2=value2"
    req := &http.Request{
        URL: &url.URL{
            RawQuery: "param1=value1&param2=value2",
        },
        Body: io.NopCloser(strings.NewReader(reqBody)),
    }

    reqParams, err := NewReqParams(req)
    if err != nil {
        t.Fatalf("Expected no error, got %v", err)
    }

    if reqParams.Values.Get("param1") != "value1" {
        t.Errorf("Expected param1 to be 'value1', got %v", reqParams.Values.Get("param1"))
    }

    if string(reqParams.Body) != reqBody {
        t.Errorf("Expected body to be '%v', got %v", reqBody, string(reqParams.Body))
    }
}

// Test generated using Keploy
func TestNewReqParams_InvalidQuery(t *testing.T) {
    req := &http.Request{
        URL: &url.URL{
            RawQuery: "%zz", // Invalid query
        },
        Body: io.NopCloser(strings.NewReader("")),
    }

    _, err := NewReqParams(req)
    if err == nil {
        t.Fatalf("Expected an error, got nil")
    }
}


// Test generated using Keploy
type errorReader struct{}

func (e *errorReader) Read(p []byte) (n int, err error) {
    return 0, errors.New("read error")
}

func TestNewReqParams_BodyReadError(t *testing.T) {
    req := &http.Request{
        URL: &url.URL{
            RawQuery: "param1=value1",
        },
        Body: io.NopCloser(&errorReader{}),
    }

    _, err := NewReqParams(req)
    if err == nil {
        t.Fatalf("Expected an error, got nil")
    }
}


// Test generated using Keploy
func TestReqParams_Get_ExistingKey(t *testing.T) {
    reqParams := &ReqParams{
        Values: url.Values{
            "key1": {"value1"},
        },
    }

    value, err := reqParams.Get("key1")
    if err != nil {
        t.Fatalf("Expected no error, got %v", err)
    }

    if value != "value1" {
        t.Errorf("Expected value to be 'value1', got %v", value)
    }
}


// Test generated using Keploy
func TestReqParams_Get_NonExistingKey(t *testing.T) {
    reqParams := &ReqParams{
        Values: url.Values{},
    }

    _, err := reqParams.Get("key1")
    if err == nil {
        t.Fatalf("Expected an error, got nil")
    }
}


// Test generated using Keploy
func TestReqParams_GetAll_ExistingKey(t *testing.T) {
    reqParams := &ReqParams{
        Values: url.Values{
            "key1": {"value1", "value2"},
        },
    }

    values, err := reqParams.GetAll("key1")
    if err != nil {
        t.Fatalf("Expected no error, got %v", err)
    }

    if len(values) != 2 || values[0] != "value1" || values[1] != "value2" {
        t.Errorf("Expected values to be ['value1', 'value2'], got %v", values)
    }
}


// Test generated using Keploy
func TestReqParams_GetAll_InvalidKey(t *testing.T) {
    reqParams := &ReqParams{
        Values: url.Values{
            "key1": {"value1", "value2"},
        },
    }

    _, err := reqParams.GetAll("key@1")
    if err == nil {
        t.Fatalf("Expected an error, got nil")
    }
}

