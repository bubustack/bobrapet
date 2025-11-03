package list

import (
	"context"
	"fmt"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/kubeutil"
	runsidentity "github.com/bubustack/bobrapet/pkg/runs/identity"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// StepRunsByStoryRun lists StepRuns in the given namespace that belong to the
// provided StoryRun name plus any additional label selectors, storing the
// result in out. Callers can pass nil extras to filter solely by StoryRun.
func StepRunsByStoryRun(
	ctx context.Context,
	c client.Client,
	namespace string,
	storyRunName string,
	extraLabels map[string]string,
	out *runsv1alpha1.StepRunList,
) error {
	if out == nil {
		return fmt.Errorf("StepRunsByStoryRun requires a non-nil output list")
	}

	labels := runsidentity.SelectorLabels(storyRunName)
	for k, v := range extraLabels {
		labels[k] = v
	}

	return kubeutil.ListByLabels(ctx, c, namespace, labels, out)
}
