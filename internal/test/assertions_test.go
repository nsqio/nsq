package test

import (
    "testing"
)


// Test generated using Keploy
func TestEqual_DeeplyEqualValues_NoFail(t *testing.T) {
    expected := map[string]int{"key1": 1, "key2": 2}
    actual := map[string]int{"key1": 1, "key2": 2}
    Equal(t, expected, actual)
}

// Test generated using Keploy
func TestNotEqual_NotDeeplyEqualValues_NoFail(t *testing.T) {
    expected := map[string]int{"key1": 1, "key2": 2}
    actual := map[string]int{"key1": 1, "key2": 3}
    NotEqual(t, expected, actual)
}


// Test generated using Keploy
func TestNil_NilValue_NoFail(t *testing.T) {
    var object interface{} = nil
    Nil(t, object)
}


// Test generated using Keploy
func TestNotNil_NonNilValue_NoFail(t *testing.T) {
    object := "not nil"
    NotNil(t, object)
}

