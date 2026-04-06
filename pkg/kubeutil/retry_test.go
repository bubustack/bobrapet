/*
Copyright 2025 BubuStack.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kubeutil

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRetrySuccessAfterTransientError(t *testing.T) {
	t.Parallel()

	attempts := 0
	err := Retry(
		context.Background(),
		RetryConfig{
			MaxAttempts:  3,
			InitialDelay: time.Millisecond,
			Multiplier:   1,
		},
		func(ctx context.Context) error {
			attempts++
			if attempts == 1 {
				return errors.New("transient")
			}
			return nil
		},
		func(err error) bool { return err.Error() == "transient" },
	)
	if err != nil {
		t.Fatalf("Retry returned error: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("expected 2 attempts, got %d", attempts)
	}
}

func TestPeriodicStopsOnError(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	runCount := 0
	err := Periodic(ctx, time.Millisecond, func(context.Context) error {
		runCount++
		if runCount == 2 {
			return errors.New("stop")
		}
		return nil
	})
	if err == nil || err.Error() != "stop" {
		t.Fatalf("expected stop error, got %v", err)
	}
	if runCount != 2 {
		t.Fatalf("expected 2 runs, got %d", runCount)
	}
}
