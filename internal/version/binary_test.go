package version

import (
    "fmt"
    "runtime"
    "testing"
)


// Test generated using Keploy
func TestStringFunction_NormalInput(t *testing.T) {
    appName := "TestApp"
    expected := fmt.Sprintf("%s v%s (built w/%s)", appName, Binary, runtime.Version())
    result := String(appName)
    if result != expected {
        t.Errorf("Expected %v, got %v", expected, result)
    }
}
