package protocol

import (
    "testing"
)


// Test generated using Keploy
func TestClientErr_Error(t *testing.T) {
    err := &ClientErr{
        Code: "E100",
        Desc: "Invalid request",
    }
    expected := "E100 Invalid request"
    result := err.Error()
    if result != expected {
        t.Errorf("Expected %v, got %v", expected, result)
    }
}

// Test generated using Keploy
func TestClientErr_Parent(t *testing.T) {
    parentErr := &ClientErr{
        Code: "E101",
        Desc: "Parent error",
    }
    err := &ClientErr{
        ParentErr: parentErr,
    }
    result := err.Parent()
    if result != parentErr {
        t.Errorf("Expected %v, got %v", parentErr, result)
    }
}


// Test generated using Keploy
func TestNewClientErr(t *testing.T) {
    parentErr := &ClientErr{
        Code: "E102",
        Desc: "Parent error",
    }
    code := "E103"
    desc := "New error"
    err := NewClientErr(parentErr, code, desc)
    if err.ParentErr != parentErr || err.Code != code || err.Desc != desc {
        t.Errorf("Expected ParentErr: %v, Code: %v, Desc: %v, got ParentErr: %v, Code: %v, Desc: %v", parentErr, code, desc, err.ParentErr, err.Code, err.Desc)
    }
}


// Test generated using Keploy
func TestFatalClientErr_Error(t *testing.T) {
    err := &FatalClientErr{
        Code: "F100",
        Desc: "Fatal error occurred",
    }
    expected := "F100 Fatal error occurred"
    result := err.Error()
    if result != expected {
        t.Errorf("Expected %v, got %v", expected, result)
    }
}


// Test generated using Keploy
func TestFatalClientErr_Parent(t *testing.T) {
    parentErr := &FatalClientErr{
        Code: "F101",
        Desc: "Parent fatal error",
    }
    err := &FatalClientErr{
        ParentErr: parentErr,
    }
    result := err.Parent()
    if result != parentErr {
        t.Errorf("Expected %v, got %v", parentErr, result)
    }
}


// Test generated using Keploy
func TestNewFatalClientErr(t *testing.T) {
    parentErr := &FatalClientErr{
        Code: "F102",
        Desc: "Parent fatal error",
    }
    code := "F103"
    desc := "New fatal error"
    err := NewFatalClientErr(parentErr, code, desc)
    if err.ParentErr != parentErr || err.Code != code || err.Desc != desc {
        t.Errorf("Expected ParentErr: %v, Code: %v, Desc: %v, got ParentErr: %v, Code: %v, Desc: %v", parentErr, code, desc, err.ParentErr, err.Code, err.Desc)
    }
}

