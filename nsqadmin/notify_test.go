package nsqadmin

import (
    "net/http"
    "testing"
    "encoding/base64"
)


// Test generated using Keploy
func TestBasicAuthUser_EmptyAuthorizationHeader(t *testing.T) {
    req := &http.Request{
        Header: http.Header{},
    }
    user := basicAuthUser(req)
    if user != "" {
        t.Errorf("Expected empty string, got %v", user)
    }
}

// Test generated using Keploy
func TestBasicAuthUser_InvalidBase64(t *testing.T) {
    req := &http.Request{
        Header: http.Header{
            "Authorization": []string{"Basic invalid_base64"},
        },
    }
    user := basicAuthUser(req)
    if user != "" {
        t.Errorf("Expected empty string, got %v", user)
    }
}


// Test generated using Keploy
func TestBasicAuthUser_NoColonInDecodedString(t *testing.T) {
    req := &http.Request{
        Header: http.Header{
            "Authorization": []string{"Basic " + base64.StdEncoding.EncodeToString([]byte("invalidstring"))},
        },
    }
    user := basicAuthUser(req)
    if user != "" {
        t.Errorf("Expected empty string, got %v", user)
    }
}


// Test generated using Keploy
func TestBasicAuthUser_ValidAuthorizationHeader(t *testing.T) {
    username := "testuser"
    password := "testpass"
    auth := "Basic " + base64.StdEncoding.EncodeToString([]byte(username+":"+password))
    req := &http.Request{
        Header: http.Header{
            "Authorization": []string{auth},
        },
    }
    user := basicAuthUser(req)
    if user != username {
        t.Errorf("Expected %v, got %v", username, user)
    }
}

