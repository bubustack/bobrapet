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

package controller

import (
	"context"
	"slices"
	"strings"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/core/contracts"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// EnsureStepRunTriggerTokens adds trigger tokens to a StepRun's annotations.
//
// Behavior:
//   - Fetches the StepRun by key with conflict retry.
//   - Delegates to EnsureTriggerTokens to add tokens to annotations.
//   - Returns newly added tokens for caller tracking.
//
// Arguments:
//   - ctx context.Context: propagated to Get and Patch operations.
//   - cl client.Client: the Kubernetes client.
//   - key types.NamespacedName: the StepRun's namespace and name.
//   - tokens ...string: trigger tokens to add.
//
// Returns:
//   - []string: newly added tokens.
//   - error: on Get or Patch failures.
func EnsureStepRunTriggerTokens(
	ctx context.Context,
	cl client.Client,
	key types.NamespacedName,
	tokens ...string,
) ([]string, error) {
	var added []string
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		current := &runsv1alpha1.StepRun{}
		if err := cl.Get(ctx, key, current); err != nil {
			return err
		}
		var inner error
		added, inner = EnsureTriggerTokens(ctx, cl, current, contracts.StepRunTriggerAnnotation, tokens...)
		return inner
	})
	return added, err
}

// EnsureTriggerTokens trims, deduplicates, and patches annotationKey on the
// provided object with the supplied trigger tokens.
//
// Behavior:
//   - Parses existing tokens from the annotation.
//   - Trims whitespace from each new token.
//   - Skips tokens that already exist.
//   - Patches the annotation with the merged, sorted token set.
//
// Arguments:
//   - ctx context.Context: context for the patch operation.
//   - cl client.Client: Kubernetes client for performing the merge patch.
//   - obj client.Object: the object whose annotations will be modified.
//   - annotationKey string: the annotation key to modify.
//   - tokens ...string: trigger tokens to add to the annotation.
//
// Returns:
//   - []string: tokens that were newly added (nil if no changes made).
//   - error: patch failure or nil on success.
//
// Side Effects:
//   - Modifies obj's annotations and patches it to the API server.
//   - Performs merge patch using client.MergeFrom for atomic updates.
func EnsureTriggerTokens(
	ctx context.Context,
	cl client.Client,
	obj client.Object,
	annotationKey string,
	tokens ...string,
) ([]string, error) {
	if len(tokens) == 0 || obj == nil {
		return nil, nil
	}

	existing := ParseTriggerTokens(obj.GetAnnotations()[annotationKey])
	added := []string{}

	for _, token := range tokens {
		token = strings.TrimSpace(token)
		if token == "" || existing.Has(token) {
			continue
		}
		existing.Insert(token)
		added = append(added, token)
	}

	if len(added) == 0 {
		return nil, nil
	}

	before := obj.DeepCopyObject().(client.Object)
	if obj.GetAnnotations() == nil {
		obj.SetAnnotations(make(map[string]string))
	}
	obj.GetAnnotations()[annotationKey] = strings.Join(SortedTokens(existing), ",")

	if err := cl.Patch(ctx, obj, client.MergeFrom(before)); err != nil {
		return nil, err
	}
	return added, nil
}

// ParseTriggerTokens parses a comma-separated trigger token string into a
// deduplicated set.
//
// Behavior:
//   - Splits the string by commas.
//   - Trims whitespace from each token.
//   - Filters out empty tokens.
//   - Returns a deduplicated set.
//
// Arguments:
//   - raw string: comma-separated string of trigger tokens.
//
// Returns:
//   - sets.String: deduplicated set of non-empty tokens.
func ParseTriggerTokens(raw string) sets.Set[string] {
	items := strings.Split(raw, ",")
	set := sets.New[string]()
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item != "" {
			set.Insert(item)
		}
	}
	return set
}

// SortedTokens converts a token set to a deterministically sorted string slice.
//
// This ensures consistent annotation values across different reconciles,
// preventing unnecessary patch operations due to ordering differences.
//
// Arguments:
//   - set sets.String: the token set to convert.
//
// Returns:
//   - []string: sorted slice of tokens.
func SortedTokens(set sets.Set[string]) []string {
	list := set.UnsortedList()
	slices.Sort(list)
	return list
}

// ContainsToken checks if a target token exists in the provided token slice.
//
// Arguments:
//   - tokens []string: slice of tokens to search.
//   - target string: token to find.
//
// Returns:
//   - bool: true if target exists in tokens.
func ContainsToken(tokens []string, target string) bool {
	return slices.Contains(tokens, target)
}
