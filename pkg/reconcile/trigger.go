package reconcile

import (
	"context"
	"fmt"
	"sort"
	"strings"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

/*
IMPORTANT NOTE (wire-up):
- This package needs to know *where* tokens are stored (annotation key) and how they are encoded.
- If your project already has a canonical annotation key (e.g., in contracts), use it here.

Adjust this constant to match your existing system.
*/
const StoryRunTriggerTokensAnnotationKey = "bubustack.io/storyrun-trigger-tokens"

// ContainsToken checks if token is in slice.
func ContainsToken(tokens []string, token string) bool {
	for _, t := range tokens {
		if t == token {
			return true
		}
	}
	return false
}

func splitTokens(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	seen := map[string]struct{}{}
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

func joinTokens(tokens []string) string {
	if len(tokens) == 0 {
		return ""
	}
	cp := append([]string(nil), tokens...)
	sort.Strings(cp)
	return strings.Join(cp, ",")
}

func addToken(existing []string, token string) (newTokens []string, added bool) {
	if token == "" {
		return existing, false
	}
	for _, t := range existing {
		if t == token {
			return existing, false
		}
	}
	newTokens = append(append([]string(nil), existing...), token)
	sort.Strings(newTokens)
	return newTokens, true
}

// EnsureStoryRunTriggerTokens ensures token exists on the StoryRun in a conflict-safe way.
// Returns the list of tokens that were newly added (empty if already present).
func EnsureStoryRunTriggerTokens(
	ctx context.Context,
	c client.Client,
	run *runsv1alpha1.StoryRun,
	token string,
) ([]string, error) {
	if run == nil {
		return nil, fmt.Errorf("storyrun is nil")
	}
	key := types.NamespacedName{Name: run.Name, Namespace: run.Namespace}

	var added []string

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var latest runsv1alpha1.StoryRun
		if err := c.Get(ctx, key, &latest); err != nil {
			return err
		}

		ann := latest.GetAnnotations()
		if ann == nil {
			ann = map[string]string{}
		}

		existing := splitTokens(ann[StoryRunTriggerTokensAnnotationKey])
		newTokens, didAdd := addToken(existing, token)
		if !didAdd {
			added = nil
			return nil
		}

		ann[StoryRunTriggerTokensAnnotationKey] = joinTokens(newTokens)
		latest.SetAnnotations(ann)

		merge := client.MergeFrom(latest.DeepCopy())
		// We must re-apply because MergeFrom snapshot must reflect "before" state.
		// So: take a fresh before snapshot, then mutate, then patch.
		before := latest.DeepCopy()
		latest.SetAnnotations(ann)
		if err := c.Patch(ctx, &latest, client.MergeFrom(before)); err != nil {
			return err
		}

		added = []string{token}
		_ = merge // left for clarity; we used client.MergeFrom(before) above
		return nil
	})
	return added, err
}
