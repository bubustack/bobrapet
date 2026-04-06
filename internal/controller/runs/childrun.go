package runs

import (
	"reflect"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
)

// UpdateTimeoutAndRetry mutates the StepRun spec with the provided timeout and retry settings.
// It returns booleans indicating whether each field changed.
func UpdateTimeoutAndRetry(
	stepRun *runsv1alpha1.StepRun,
	timeout string,
	retry *bubuv1alpha1.RetryPolicy,
) (timeoutChanged, retryChanged bool) {
	if stepRun.Spec.Timeout != timeout {
		stepRun.Spec.Timeout = timeout
		timeoutChanged = true
	}

	if !reflect.DeepEqual(stepRun.Spec.Retry, retry) {
		if retry == nil {
			stepRun.Spec.Retry = nil
		} else {
			stepRun.Spec.Retry = retry.DeepCopy()
		}
		retryChanged = true
	}
	return
}
