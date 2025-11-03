package childrun

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

// UpdateRequestedManifest ensures the StepRun requested manifest field matches the desired slice.
// When clone is true, the desired slice is deep-copied before assignment to avoid aliasing.
// Returns true when the manifest field changed.
func UpdateRequestedManifest(
	current *[]runsv1alpha1.ManifestRequest,
	desired []runsv1alpha1.ManifestRequest,
	clone bool,
) bool {
	if current == nil {
		return false
	}

	existing := *current
	switch {
	case len(desired) > 0:
		if !manifestRequestsEqual(existing, desired) {
			if clone {
				*current = copyManifestRequests(desired)
			} else {
				*current = desired
			}
			return true
		}
	case len(existing) > 0:
		*current = nil
		return true
	}
	return false
}

func manifestRequestsEqual(a, b []runsv1alpha1.ManifestRequest) bool {
	if len(a) != len(b) {
		return false
	}

	toMap := func(reqs []runsv1alpha1.ManifestRequest) map[string]map[runsv1alpha1.ManifestOperation]struct{} {
		out := make(map[string]map[runsv1alpha1.ManifestOperation]struct{}, len(reqs))
		for _, req := range reqs {
			set := make(map[runsv1alpha1.ManifestOperation]struct{}, len(req.Operations))
			for _, op := range req.Operations {
				set[op] = struct{}{}
			}
			out[req.Path] = set
		}
		return out
	}

	mapA := toMap(a)
	mapB := toMap(b)

	if len(mapA) != len(mapB) {
		return false
	}

	for path, opsA := range mapA {
		opsB, ok := mapB[path]
		if !ok || len(opsA) != len(opsB) {
			return false
		}
		for op := range opsA {
			if _, exists := opsB[op]; !exists {
				return false
			}
		}
	}

	return true
}

func copyManifestRequests(src []runsv1alpha1.ManifestRequest) []runsv1alpha1.ManifestRequest {
	if len(src) == 0 {
		return nil
	}

	out := make([]runsv1alpha1.ManifestRequest, len(src))
	for i := range src {
		out[i].Path = src[i].Path
		if len(src[i].Operations) > 0 {
			ops := make([]runsv1alpha1.ManifestOperation, len(src[i].Operations))
			copy(ops, src[i].Operations)
			out[i].Operations = ops
		}
	}
	return out
}
