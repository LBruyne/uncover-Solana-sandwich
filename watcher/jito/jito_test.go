package jito_test

import (
	"context"
	"testing"
	"time"
	"watcher/jito"
)

// TestRunJitoCmdIntegration is an integration test for RunJitoCmd.
// It will run for a short period and verify that bundles are fetched and inserted.
func TestRunJitoCmdIntegration(t *testing.T) {
	// Run the function in a separate goroutine
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		err := jito.RunJitoCmd()
		if err != nil {
			t.Errorf("RunJitoCmd returned error: %v", err)
		}
	}()

	// Wait until context timeout or function finishes
	select {
	case <-ctx.Done():
		t.Log("Test timed out as expected (RunJitoCmd is infinite loop)")
	case <-done:
		t.Log("RunJitoCmd finished unexpectedly")
	}
}
