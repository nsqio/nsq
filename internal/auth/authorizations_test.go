package auth

import (
    "testing"
    "time"
)


// Test generated using Keploy
func TestAuthorization_HasPermission_ValidPermission(t *testing.T) {
    auth := Authorization{
        Permissions: []string{"publish", "subscribe"},
    }
    if !auth.HasPermission("publish") {
        t.Errorf("Expected HasPermission to return true for 'publish', got false")
    }
}

// Test generated using Keploy
func TestState_IsExpired_Expired(t *testing.T) {
    state := State{
        Expires: time.Now().Add(-1 * time.Hour),
    }
    if !state.IsExpired() {
        t.Errorf("Expected IsExpired to return true for expired state, got false")
    }
}


// Test generated using Keploy
func TestAuthorization_HasPermission_InvalidPermission(t *testing.T) {
    auth := Authorization{
        Permissions: []string{"publish", "subscribe"},
    }
    if auth.HasPermission("delete") {
        t.Errorf("Expected HasPermission to return false for 'delete', got true")
    }
}


// Test generated using Keploy
func TestAuthorization_IsAllowed_InvalidTopic(t *testing.T) {
    auth := Authorization{
        Topic:    "test-topic",
        Channels: []string{"test-channel"},
    }
    if auth.IsAllowed("invalid-topic", "test-channel") {
        t.Errorf("Expected IsAllowed to return false for invalid topic, got true")
    }
}


// Test generated using Keploy
func TestState_IsAllowed_InvalidTopic(t *testing.T) {
    state := State{
        Authorizations: []Authorization{
            {
                Topic:    "test-topic",
                Channels: []string{"test-channel"},
            },
        },
    }
    if state.IsAllowed("invalid-topic", "test-channel") {
        t.Errorf("Expected IsAllowed to return false for unauthorized topic, got true")
    }
}

