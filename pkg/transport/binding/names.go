package binding

import "github.com/bubustack/bobrapet/pkg/kubeutil"

const (
	bindingSuffix    = "transport"
	storyScopeSuffix = "story"
)

// Name returns the deterministic name for the TransportBinding associated with
// a given StoryRun step. Names are kept DNS-safe and stable using the shared
// kubeutil.ComposeName helper.
func Name(storyRunName, stepName string) string {
	return kubeutil.ComposeName(storyRunName, stepName, bindingSuffix)
}

// StoryScopedName returns the deterministic name for a TransportBinding that is
// shared across all StoryRuns for a given Story/step pair.
func StoryScopedName(storyName, stepName string) string {
	return kubeutil.ComposeName(storyName, stepName, storyScopeSuffix, bindingSuffix)
}
