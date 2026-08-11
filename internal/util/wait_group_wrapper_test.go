package util

import (
	"testing"
)

// Test generated using Keploy
func TestWaitGroupWrapper_Wrap_CallbackExecution(t *testing.T) {
	var wgWrapper WaitGroupWrapper
	executed := false

	wgWrapper.Wrap(func() {
		executed = true
	})

	wgWrapper.Wait()

	if !executed {
		t.Errorf("Expected callback to be executed, but it was not")
	}
}
